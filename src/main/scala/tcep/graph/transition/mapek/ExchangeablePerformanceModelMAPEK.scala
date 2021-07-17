package tcep.graph.transition.mapek

import akka.actor.{ActorContext, ActorLogging, ActorRef, ActorSelection, Address, Props, Timers}
import akka.cluster.Cluster
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.cardygan.config.Config
import org.slf4j.LoggerFactory
import tcep.data.Queries
import tcep.data.Queries._
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.qos.OperatorQosMonitor._
import tcep.graph.transition.MAPEK._
import tcep.graph.transition._
import tcep.graph.transition.mapek.ExchangeablePerformanceModelMAPEK.{GetContextSample, GetQueryPerformancePrediction}
import tcep.graph.transition.mapek.contrast.ContrastMAPEK.{GetCFM, GetContextData, RunPlanner}
import tcep.graph.transition.mapek.contrast._
import tcep.machinenodes.consumers.Consumer.{AllRecords, GetAllRecords}
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

class ExchangeablePerformanceModelMAPEK(query: Query, mode: TransitionConfig, startingPlacementStrategy: String, val consumer: ActorRef)(implicit cluster: Cluster, context: ActorContext)
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


  override def receive: Receive = super.receive orElse {
    case s@SampleUpdate(query, sample) =>
      // only log if any events arrived
    if(sample._1.ioMetrics.incomingEventRate.amount > 0) {
      logSample(sender(), sample)
      mapek.knowledge.forward(s)
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
        val start = System.nanoTime()
        for {
          currentPlacement: Option[Map[Query, ActorRef]] <- (mapek.knowledge ? GetOperators).mapTo[CurrentOperators].map(e => Some(e.placement))
          publisherEventRates: Map[String, Throughput] <- TCEPUtils.getPublisherEventRates()
          publishers: Map[String, ActorRef] <- TCEPUtils.getPublisherActors()
          contextSample <- (mapek.knowledge ? GetContextSample).mapTo[Map[Query, (Samples, List[OfflineAndOnlinePredictions])]]
          currentThroughput <- (mapek.consumer ? GetAllRecords).mapTo[AllRecords].map(_.recordFrequency.lastMeasurement)
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
            SpecialStats.log(this.toString, "placement_performance_predictions", s"${p.map(e => s"${e._1};${e._2._1.endToEndLatency.amount.toNanos / 1e6};${e._2._1.throughput.amount}").mkString(";")};took ${(System.nanoTime() - start) / 1e6}ms")
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
              else if (optimizationTarget == "throughput") qosFulfillingPlacements.maxBy(_._2._1.throughput.getEventsPerSec)
              else throw new IllegalArgumentException(s"unknown optimization target metric $optimizationTarget")

              log.info("found requirement-fulfilling placement strategy {}:\n{}", transitionTarget._1, transitionTarget._2._1)
              val betterThanCurrent: Boolean = optimizationTarget match {
                case "latency" => transitionTarget._2._1.endToEndLatency.amount.toMillis < currentLatency
                case "throughput" => transitionTarget._2._1.throughput.getEventsPerSec > currentThroughput.getOrElse(0)
                case _ => true
              }
              if(betterThanCurrent)
                mapek.executor ! ExecuteTransitionWithPlacement(transitionTarget._1, transitionTarget._2._2)
              else {
                log.info("best predicted latency {},current average {}", transitionTarget._2._1.endToEndLatency, currentLatency)
                log.info("best predicted throughput {}, current average {}", transitionTarget._2._1.throughput.getEventsPerSec, currentThroughput)
                log.info("not executing transition because no improvement in {}", optimizationTarget)
              }
            } else {
              log.warning("no placement strategy with sufficient predicted performance for the following requirements could be found: \n{}\n placement predictions were \n{}", qosRequirements.mkString("\n"), p.map(e => e._2._1).mkString("\n"))
              val transitionTarget = qosRequirements.head match {
                case LatencyRequirement(operator, latency, otherwise, name) =>  p.minBy(_._2._1.endToEndLatency.amount)
                case f: FrequencyRequirement => p.maxBy(_._2._1.throughput.getEventsPerSec)
                case r: Requirement =>
                  log.warning("ignoring requirement {} since there is no prediction model for it", r)
                  p.minBy(_._2._1.endToEndLatency.amount)
              }
              log.info("transiting to placement closest to fulfilling the requirement instead: {}", transitionTarget)
              mapek.executor ! ExecuteTransitionWithPlacement(transitionTarget._1, transitionTarget._2._2)
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
  extends KnowledgeComponent(mapek, rootOperator, transitionConfig, startingPlacementStrategy) with Timers with PredictionHttpClient {

  var mostRecentSamplesAndPredictions: Map[Query, (Samples, List[OfflineAndOnlinePredictions])] = Map()
  var mostRecentSamples: Map[Query, Samples] = Map()
  val cfm: DynamicCFM = new DynamicCFM(rootOperator)
  val minOnlineSamples: Int = ConfigFactory.load().getInt("constants.mapek.exchangeable-model.online-model-warmup-phase")

  private case object GetBatchPredictionTickKey
  private case object GetBatchPredictionTick
  lazy val predictionDispatcher: ExecutionContext = context.system.dispatchers.lookup("prediction-dispatcher")
  val qSize: Int = Queries.getOperators(mapek.asInstanceOf[ExchangeablePerformanceModelMAPEK].getQueryRoot).size
  var mostRecentQueryPerformancePrediction: Option[EndToEndLatencyAndThroughputPrediction] = None
  val queryPerformancePredictor: ActorSelection = cluster.system.actorSelection(mapek.planner.path.child("queryPerformancePredictor"))

  override def preStart(): Unit = {
    super.preStart()
    timers.startTimerWithFixedDelay(GetBatchPredictionTickKey, GetBatchPredictionTick, samplingInterval)
  }

  override def receive: Receive = super.receive orElse {

    case SampleUpdate(operator, sample) =>
      val prevSamples = mostRecentSamples.getOrElse(operator, List()).take(minOnlineSamples) // keep only most recent

      if(prevSamples.isEmpty) {
        // check if there exists another entry for the same operator;
        //  bug related to default hashing caused these two operator types to be not considered equal after a transition, resulting in two map entries where there should only be one, and the new operator starting without the samples of the old one (messing up the model predictions and weighting)
        val prevOtherEntry = operator match { // TODO this is a temporary workaround; will remove other operators if there are multiple of these operators in a query
          case _: ShrinkingFilterQuery => mostRecentSamples.find(e => e._1.isInstanceOf[ShrinkingFilterQuery])
          case _: WindowStatisticQuery => mostRecentSamples.find(e => e._1.isInstanceOf[WindowStatisticQuery] )
          case _ => None
        }
        log.info("received first sample from {}, operator entry exists: {}, other entries of same type: {}", sender(), prevSamples.nonEmpty, prevOtherEntry.size)
        if(prevOtherEntry.isDefined) {
          // this should not occur anymore
          log.warning("current op id is \n{} other entry is \n{} \nwith class {}", operator.id, prevOtherEntry.get._1.id, prevOtherEntry.get._1)
          log.warning("operator entries are equal: {} {} \n ", prevOtherEntry.get._1.equals(operator), prevOtherEntry.get._1 == operator)
          val othersSamples = mostRecentSamples(prevOtherEntry.get._1).take(minOnlineSamples)
          mostRecentSamples = mostRecentSamples.-(prevOtherEntry.get._1)
          mostRecentSamples = mostRecentSamples.updated(operator, sample :: othersSamples)
          val othersSamplesAndPredictions = mostRecentSamplesAndPredictions(prevOtherEntry.get._1)
          mostRecentSamplesAndPredictions = mostRecentSamplesAndPredictions.-(prevOtherEntry.get._1)
          mostRecentSamplesAndPredictions = mostRecentSamplesAndPredictions.updated(operator, othersSamplesAndPredictions)

        } else mostRecentSamples = mostRecentSamples.updated(operator, sample :: List())
      } else {
        mostRecentSamples = mostRecentSamples.updated(operator, sample :: prevSamples)
        log.debug("received new sample {},\n most recent sample count: {} of {}", sample, mostRecentSamples(operator).size, minOnlineSamples)
      }

    case GetBatchPredictionTick =>
      if(mostRecentSamples.size >= operators.size && mostRecentSamples.size >= qSize ) {
        val lastSamples = mostRecentSamples.map(s => s._1 -> s._2.head)
        for {
          samplePredictions <- getBatchMetricPredictions(lastSamples, currentPlacementStrategy)(predictionDispatcher)// logs to stats-*_predictions.csv
        } yield {
          mostRecentSamplesAndPredictions = lastSamples.map(e => e._1 -> {
            val prevSamples = mostRecentSamplesAndPredictions.getOrElse(e._1, (List(), List()))._1.take(minOnlineSamples)
            val prevPredictions = mostRecentSamplesAndPredictions.getOrElse(e._1, (List(), List()))._2.take(minOnlineSamples)
            (e._2 :: prevSamples, samplePredictions(e._1) :: prevPredictions)
          })
          log.debug("received predictions, now {} operators mostRecentSamplesAndPredictions: \n{}", mostRecentSamplesAndPredictions.size, mostRecentSamplesAndPredictions.toList.map(e => e._1.getClass.getSimpleName -> (e._2._1.size, e._2._2.size)).mkString("\n"))
          log.debug("current mostRecentSamples: {}\n{}", mostRecentSamples.size, mostRecentSamples.toList.map(e => e._1.getClass.getSimpleName -> e._2.size).mkString("\n"))
          updateBatchOnlineModel(lastSamples, currentPlacementStrategy)(predictionDispatcher)
          for {
            publisherEventRates: Map[String, Throughput] <- TCEPUtils.getPublisherEventRates()(cluster, predictionDispatcher)
            queryPred <- (queryPerformancePredictor ? GetPredictionForPlacement(rootOperator, Some(operators), operators.map(e => e._1 -> e._2.path.address), publisherEventRates, Some(mostRecentSamplesAndPredictions), currentPlacementStrategy)).mapTo[EndToEndLatencyAndThroughputPrediction]
          } yield mostRecentQueryPerformancePrediction = Some(queryPred)
        }
      }


    case GetQueryPerformancePrediction => sender() ! mostRecentQueryPerformancePrediction
    case GetCFM => sender() ! this.cfm
    case GetContextSample => sender() ! mostRecentSamplesAndPredictions
    case GetContextData => sender() ! Map()
    case msg => log.error(s"received unhandled msg $msg")
  }
}

object ExchangeablePerformanceModelMAPEK {
  case object GetContextSample
  case object GetQueryPerformancePrediction
  def currentPlacementStrategyToOneHot(algorithmNames: Iterable[String], current: String): String = {
    algorithmNames.map {
      case name if name == current => 1
      case _ => 0
    }.mkString(";")
  }
}

