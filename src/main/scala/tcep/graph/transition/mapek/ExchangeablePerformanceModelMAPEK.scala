package tcep.graph.transition.mapek

import akka.actor.{ActorContext, ActorLogging, ActorRef, Address, Props, Timers}
import akka.cluster.Cluster
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.cardygan.config.Config
import org.slf4j.LoggerFactory
import tcep.data.Queries
import tcep.data.Queries.{FrequencyRequirement, LatencyRequirement, Query, Requirement}
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.qos.OperatorQosMonitor._
import tcep.graph.transition.MAPEK._
import tcep.graph.transition._
import tcep.graph.transition.mapek.ExchangeablePerformanceModelMAPEK.GetContextSample
import tcep.graph.transition.mapek.contrast.ContrastMAPEK.{GetCFM, GetContextData, RunPlanner}
import tcep.graph.transition.mapek.contrast._
import tcep.placement.PlacementStrategy
import tcep.prediction.PredictionHelper.{EndToEndLatencyAndThroughputPrediction, OfflineAndOnlinePredictions, Throughput}
import tcep.prediction.QueryPerformancePredictor.GetPredictionForPlacement
import tcep.prediction.{PredictionHttpClient, QueryPerformancePredictor}
import tcep.utils.{SpecialStats, TCEPUtils}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter
import scala.util.{Failure, Success}

class ExchangeablePerformanceModelMAPEK(query: Query, mode: TransitionConfig, startingPlacementStrategy: String, consumer: ActorRef)(implicit cluster: Cluster, context: ActorContext)
  extends MAPEK(context) {
  val log = LoggerFactory.getLogger(getClass)
  override val monitor: ActorRef = context.actorOf(Props(classOf[DecentralizedMonitor], this, cluster), "MonitorEndpoint")
  override val analyzer: ActorRef = context.actorOf(Props(classOf[ExchangeablePerformanceModelAnalyzer], this))
  override val planner: ActorRef = context.actorOf(Props(classOf[ExchangeablePerformanceModelPlanner], this, cluster))
  override val executor: ActorRef = context.actorOf(Props(classOf[ExchangeablePerformanceModelExecutor], this))
  override val knowledge: ActorRef = context.actorOf(Props(classOf[ExchangeablePerformanceModelKnowledge], this, query, mode, startingPlacementStrategy), "knowledge")
  log.info("creating exchangeable performanceModel MAPEK")

  def getQueryRoot: Query = query
}

/**
  * M of the MAPE-K cycle
  * responsible for retrieving sample data from the distributed monitor instances on operators and the brokers they are deployed on and sending it to knowledge
  */
class DecentralizedMonitor(mapek: MAPEK)(implicit cluster: Cluster) extends MonitorComponent(mapek) with ActorLogging with Timers with PredictionHttpClient {

  implicit val askTimeout: Timeout = Timeout(FiniteDuration(ConfigFactory.load().getInt("constants.default-request-timeout"), TimeUnit.SECONDS))
  val shiftTarget: Boolean = false
  var headersInit: Map[ActorRef, Boolean] = Map()
  val sampleFileHeader: String = algorithmNames.mkString(";") + ";" + DynamicCFMNames.ALL_FEATURES.mkString(";") + ";" + DynamicCFMNames.ALL_TARGET_METRICS.mkString(";")
  val transitionsEnabled: Boolean = ConfigFactory.load().getBoolean("constants.mapek.transitions-enabled") // mapek planner enabled with prediction endpoint interaction
  val transitionTesting: Boolean = ConfigFactory.load().getBoolean("constants.transition-testing")
  lazy val predictionDispatcher: ExecutionContext = context.system.dispatchers.lookup("prediction-dispatcher")


  override def preStart(): Unit = {
    //timers.startTimerAtFixedRate(GetSamplingDataKey, GetSamplingDataTick, samplingInterval)
  }

  override def receive: Receive = super.receive orElse {
    case s@SampleUpdate(query, sample) =>
      // only log if any events arrived
    if(sample._1.ioMetrics.incomingEventRate.amount > 0) {
      logSample(sender(), sample)
      for {
        samplePredictions <- getMetricPredictions (query, sample, currentPlacementStrategy)(predictionDispatcher) // logs to stats-*_predictions.csv
      } yield {
        mapek.knowledge ! SampleAndPredictionsUpdate(s, samplePredictions)
        updateOnlineModel(sample, currentPlacementStrategy)(predictionDispatcher)
      }

    }
    /* old pull-based implementation
    case GetSamplingDataTick => mapek.knowledge ! GetOperators // response below
    case CurrentOperators(placement) =>
      log.debug("received placement {}", placement.mkString("\n"))
      if(placement.nonEmpty) {
        val mostRecentSamples: Future[MonitorSamples] = TCEPUtils.makeMapFuture(placement.toSeq.map(op => op -> (op._2 ? GetSamples).mapTo[Samples]).toMap)
          .map(MonitorSamples(_))
        // log samples to file per operator
        mostRecentSamples.foreach(f => f.sampleMap.foreach(op => {
          log.debug("received {} samples from {}", op._2.size, op._1._1.getClass.toString)
          logSample(op._1._2, op._2)
        }))
        mostRecentSamples.pipeTo(mapek.knowledge)
      }
     */
    case _ =>
  }

  def getShiftedSample(samples: Samples): Sample = if (shiftTarget) samples.last else samples.head

  def logSample(op: ActorRef, sample: Sample): Unit = {
    val logFileString: String = op.toString().split("-").head.split("/").last
    if (!headersInit.contains(op)) {
      SpecialStats.log(logFileString, logFileString, sampleFileHeader)
      headersInit = headersInit.updated(op, true)
    }

    SpecialStats.log(logFileString, logFileString,
      ExchangeablePerformanceModelMAPEK.currentPlacementStrategyToOneHot(algorithmNames, currentPlacementStrategy) + ";" +
      DynamicCFMNames.ALL_FEATURES.map(f => getFeatureValue(sample, f)).mkString(";") + ";" +
        DynamicCFMNames.ALL_TARGET_METRICS.map(m => getTargetMetricValue(sample, m)).mkString(";"))


  }


  private object GetSamplingDataTick
  private object GetSamplingDataKey
}

class ExchangeablePerformanceModelAnalyzer(mapek: ExchangeablePerformanceModelMAPEK) extends ContrastAnalyzer(mapek) {
  override def getCurrentContextConfig(cfm: CFM): Future[Config] = {
    for {
      contextData <- (mapek.knowledge ? GetContextSample).mapTo[Map[Query, (Samples, List[OfflineAndOnlinePredictions])]]
    } yield cfm.asInstanceOf[DynamicCFM].getCurrentContextConfigFromSamples(contextData.map(e => e._1 -> e._2._1.head))
  }
}

class ExchangeablePerformanceModelPlanner(mapek: ExchangeablePerformanceModelMAPEK)(implicit cluster: Cluster) extends MAPEKComponent {
  val optimizationTarget: String = ConfigFactory.load().getString("constants.mapek.exchangeable-model.optimization-target")
  lazy val allPlacementAlgorithms: List[PlacementStrategy] = ConfigFactory.load()
    .getStringList("benchmark.general.algorithms").asScala.toList
    .map(PlacementStrategy.getStrategyByName)
  val queryPerformancePredictor: ActorRef = context.actorOf(Props(classOf[QueryPerformancePredictor], cluster), "queryPerformancePredictor")
  val transitionsEnabled: Boolean = ConfigFactory.load().getBoolean("constants.mapek.transitions-enabled")
  val transitionTesting: Boolean = ConfigFactory.load().getBoolean("constants.transition-testing")

  override def receive: Receive =  {

    case ManualTransition(algorithmName) =>
      log.info("received ManualTransition request to {}", algorithmName)
      val s = System.currentTimeMillis()
      val p = PlacementStrategy.getStrategyByName(algorithmName)
      val rootOperator = mapek.getQueryRoot
      implicit val ec: ExecutionContext = context.system.dispatchers.lookup("transition-dispatcher") // dedicated dispatcher for transition, avoid transition getting stuck
      val executeWithPlacementF = for {
        publishers: Map[String, ActorRef] <- TCEPUtils.getPublisherActors()
        publisherEventRates: Map[String, Throughput] <- TCEPUtils.getPublisherEventRates()
        queryDependencyMap = Queries.extractOperatorsAndThroughputEstimates(rootOperator)(publisherEventRates)
        _ <- p.initialize()(ec, cluster, Some(publisherEventRates))
        placement: Map[Query, Address] <- p.initialVirtualOperatorPlacement(rootOperator, publishers)(ec, cluster, queryDependencyMap).map(_.map(e => e._1 -> e._2.member.address))
      } yield {
        mapek.executor ! ExecuteTransitionWithPlacement(algorithmName, placement)
      }
      executeWithPlacementF.onComplete {
        case Failure(exception) => log.error(exception, "failed to calculate virtual placement for {} after {}ms", p, System.currentTimeMillis() - s)
        case Success(_) => log.info("{} virtual placement calculation for manual transition complete after {}ms", p.name, System.currentTimeMillis() - s)
      }

    case RunPlanner(cfm: CFM, contextConfig: Config, currentLatency: Double, qosRequirements: Set[Requirement]) =>
      log.info("received RunPlanner with requirements {}, transitions enabled: {}, transitionsTesting:", qosRequirements, transitionsEnabled, transitionTesting)
      if(transitionsEnabled && !transitionTesting) {
        for {
          currentPlacement: Option[Map[Query, ActorRef]] <- (mapek.knowledge ? GetOperators).mapTo[CurrentOperators].map(e => Some(e.placement))
          publisherEventRates: Map[String, Throughput] <- TCEPUtils.getPublisherEventRates()
          publishers: Map[String, ActorRef] <- TCEPUtils.getPublisherActors()
          contextSample <- (mapek.knowledge ? GetContextSample).mapTo[Map[Query, (Samples, List[OfflineAndOnlinePredictions])]]
        } yield {
          log.info("received context sample and current placement")
          val rootOperator: Query = mapek.getQueryRoot
          val queryDependencyMap = Queries.extractOperatorsAndThroughputEstimates(rootOperator)(publisherEventRates)
          Future.traverse(allPlacementAlgorithms)(p => {
            val s = System.currentTimeMillis()
            for {
              _ <- p.initialize()(ec, cluster, Some(publisherEventRates))
              placement: Map[Query, Address] <- p.initialVirtualOperatorPlacement(rootOperator, publishers)(ec, cluster, queryDependencyMap).map(_.map(e => e._1 -> e._2.member.address))
              _ = log.info("{} virtual placement calculation complete after {}ms", p.name, System.currentTimeMillis() - s)
              pred <- (queryPerformancePredictor ? GetPredictionForPlacement(rootOperator, currentPlacement, placement, publisherEventRates, Some(contextSample), p.name)).mapTo[EndToEndLatencyAndThroughputPrediction]
            } yield p.name -> (pred, placement)
          }).map(p => {
            log.info("received predictions \n{}", p.map(e => e._1 -> e._2._1).mkString("\n"))
            // check each requirement
            val qosFulfillingPlacements = p.filter(placement => {
              qosRequirements.forall {
                case LatencyRequirement(operator, latency, otherwise, name) => Queries.compareHelper(latency.toMillis, operator, placement._2._1.endToEndLatency.amount.toMillis)
                case f: FrequencyRequirement => Queries.compareHelper(f.getEventsPerSec, f.operator, placement._2._1.throughput.getEventsPerSec)
                case r: Requirement => {
                  log.warning("ignoring requirement {} since there is no prediction model for it", r)
                  true
                }
              }
            })

            if (qosFulfillingPlacements.nonEmpty) {
              val transitionTarget = if (optimizationTarget == "latency") qosFulfillingPlacements.minBy(_._2._1.endToEndLatency.amount)
              else if (optimizationTarget == "throughput") qosFulfillingPlacements.minBy(_._2._1.throughput.getEventsPerSec)
              else throw new IllegalArgumentException(s"unknown optimization target metric $optimizationTarget")

              log.info("found requirement-fulfilling placement strategy {}:\n{}", transitionTarget._1, transitionTarget._2._1)
              mapek.executor ! ExecuteTransitionWithPlacement(transitionTarget._1, transitionTarget._2._2)
            } else {
              log.error("no placement strategy with sufficient predicted performance for the following requirements could be found: \n{}\n placement predictions were \n{}", qosRequirements.mkString("\n"), p.map(e => e._2._1).mkString("\n"))
            }
          })
        }
      }

    case msg => log.error(s"received unhandled msg $msg")
  }
}

class ExchangeablePerformanceModelExecutor(mapek: ExchangeablePerformanceModelMAPEK) extends ExecutorComponent(mapek) {
  override def receive: Receive = super.receive orElse {
    case ExecuteTransitionWithPlacement(strategyName, placement) =>
      log.info(s"received ExecuteTransition to $strategyName")
      for {
        currentStrategyName <- (mapek.knowledge ? GetPlacementStrategyName).mapTo[String]
        client <- (mapek.knowledge ? GetClient).mapTo[ActorRef]
        mode <- (mapek.knowledge ? GetTransitionMode).mapTo[TransitionConfig]
        status <- (mapek.knowledge ? GetTransitionStatus).mapTo[Int]
      } yield {
        if (currentStrategyName != strategyName && status != 1) {
          mapek.knowledge ! SetPlacementStrategy(strategyName)
          log.info(s"executing $mode transition to ${strategyName}")
          client ! TransitionRequest(strategyName, self, TransitionStats(0, 0, System.currentTimeMillis()), Some(placement))
        } else {
          log.info(s"not executing $mode transition to $strategyName (other transition in progress: $status; current strategy: $currentStrategyName)")
        }
      }
  }
}

class ExchangeablePerformanceModelKnowledge(mapek: MAPEK, rootOperator: Query, transitionConfig: TransitionConfig, startingPlacementStrategy: String)
  extends KnowledgeComponent(mapek, rootOperator, transitionConfig, startingPlacementStrategy) {

  var mostRecentSamplesAndPredictions: Map[Query, (Samples, List[OfflineAndOnlinePredictions])] = Map()
  val cfm: DynamicCFM = new DynamicCFM(rootOperator)
  val samplingInterval: FiniteDuration = FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.MILLISECONDS)
  val onlineWarmUpPhase: FiniteDuration = FiniteDuration(ConfigFactory.load().getInt("constants.mapek.exchangeable-model.online-model-warmup-phase"), TimeUnit.MINUTES)
  val minOnlineSamples: Int = onlineWarmUpPhase.div(samplingInterval).toInt

  override def receive: Receive = super.receive orElse {
        // received from DecentralizedMonitor
    case s@SampleAndPredictionsUpdate(sampleUpdate: SampleUpdate, predictions: OfflineAndOnlinePredictions) =>
      val prevSamples = mostRecentSamplesAndPredictions.getOrElse(sampleUpdate.query, (List(), List()))._1.take(minOnlineSamples)
      val prevPredictions = mostRecentSamplesAndPredictions.getOrElse(sampleUpdate.query, (List(), List()))._2.take(minOnlineSamples)
      mostRecentSamplesAndPredictions = mostRecentSamplesAndPredictions.updated(sampleUpdate.query, (sampleUpdate.lastSample :: prevSamples, predictions :: prevPredictions))
      log.debug("received new sample {},\n most recent sample count: {} of {}", sampleUpdate.query, mostRecentSamplesAndPredictions(sampleUpdate.query)._1.size, minOnlineSamples)

    case GetCFM => sender() ! this.cfm
    case GetContextSample => sender() ! mostRecentSamplesAndPredictions
    case GetContextData => sender() ! Map()
    case msg => log.error(s"received unhandled msg $msg")
  }
}

object ExchangeablePerformanceModelMAPEK {
  case object GetContextSample
  def currentPlacementStrategyToOneHot(algorithmNames: Iterable[String], current: String): String = {
    algorithmNames.map {
      case name if name == current => 1
      case _ => 0
    }.mkString(";")
  }
}

