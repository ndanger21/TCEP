package tcep.graph.transition.mapek

import akka.actor.{ActorContext, ActorLogging, ActorRef, Address, Props, Timers}
import akka.cluster.Cluster
import akka.pattern.{ask, pipe}
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
import tcep.graph.transition.mapek.ExchangeablePerformanceModelMAPEK.{GetContextSample, MonitorSamples}
import tcep.graph.transition.mapek.contrast.ContrastMAPEK.{GetCFM, GetContextData, RunPlanner}
import tcep.graph.transition.mapek.contrast._
import tcep.placement.PlacementStrategy
import tcep.prediction.PredictionHelper.{MetricPredictions, Throughput}
import tcep.prediction.QueryPerformancePredictor
import tcep.prediction.QueryPerformancePredictor.GetPredictionForPlacement
import tcep.utils.{SpecialStats, TCEPUtils}

import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

class ExchangeablePerformanceModelMAPEK(query: Query, mode: TransitionConfig, startingPlacementStrategy: String, consumer: ActorRef)(implicit cluster: Cluster, context: ActorContext)
  extends MAPEK(context) {
  val log = LoggerFactory.getLogger(getClass)
  override val monitor: ActorRef = context.actorOf(Props(classOf[DecentralizedMonitor], this, cluster), "MonitorEndpoint")
  override val analyzer: ActorRef = context.actorOf(Props(classOf[ExchangeablePerformanceModelAnalyzer], this))
  override val planner: ActorRef = context.actorOf(Props(classOf[ExchangeablePerformanceModelPlanner], this, cluster))
  override val executor: ActorRef = context.actorOf(Props(classOf[ExchangeablePerformanceModelExecutor], this))
  override val knowledge: ActorRef = context.actorOf(Props(classOf[ExchangeablePerformanceModelKnowledge], query, mode, startingPlacementStrategy), "knowledge")
  log.info("creating exchangeable performanceModel MAPEK")

  def getQueryRoot: Query = query
}

/**
  * M of the MAPE-K cycle
  * responsible for retrieving sample data from the distributed monitor instances on operators and the brokers they are deployed on and sending it to knowledge
  */
class DecentralizedMonitor(mapek: MAPEK)(implicit cluster: Cluster) extends MonitorComponent(mapek) with ActorLogging with Timers {

  val samplingInterval: FiniteDuration = FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.MILLISECONDS)
  implicit val askTimeout: Timeout = Timeout(FiniteDuration(ConfigFactory.load().getInt("constants.default-request-timeout"), TimeUnit.SECONDS))
  val shiftTarget: Boolean = false
  var headersInit: Map[ActorRef, Boolean] = Map()
  val sampleFileHeader: String = DynamicCFMNames.ALL_FEATURES.mkString(";") + ";" + DynamicCFMNames.ALL_TARGET_METRICS.mkString(";")

  override def preStart(): Unit = {
    timers.startTimerWithFixedDelay(GetSamplingDataKey, GetSamplingDataTick, samplingInterval)
  }

  override def receive: Receive = super.receive orElse {
    case GetSamplingDataTick => mapek.knowledge ! GetOperators // response below
    case CurrentOperators(placement) =>
      log.debug("received placement {}", placement.mkString("\n"))
      if(placement.nonEmpty) {
        val mostRecentSamples: Future[MonitorSamples] = TCEPUtils.makeMapFuture(placement.toSeq.map(op => op -> (op._2 ? GetSamples).mapTo[Samples]).toMap)
          .map(MonitorSamples(_))
        // log samples to file per operator
        mostRecentSamples.foreach(f => f.sampleMap.foreach(op => {
          log.debug("received {} samples from {}", op._2.size, op._1._1.getClass.toString)
          if(op._2.nonEmpty) {
            val logFileString: String = op._1._2.toString().split("-").head.split("/").last
            if (!headersInit.contains(op._1._2)) {
              SpecialStats.log(logFileString, logFileString, sampleFileHeader)
              headersInit = headersInit.updated(op._1._2, true)
            }

            def getShiftedSample: Sample = if (shiftTarget) op._2.last else op._2.head

            if (!shiftTarget || shiftTarget && op._2.size > 1) {
              SpecialStats.log(logFileString, logFileString,
                DynamicCFMNames.ALL_FEATURES.map(f => getFeatureValue(op._2.head, f)).mkString(";") + ";" +
                  DynamicCFMNames.ALL_TARGET_METRICS.map(m => getTargetMetricValue(getShiftedSample, m)).mkString(";"))
            }
          }
        }))
        mostRecentSamples.pipeTo(mapek.knowledge)
      }
    case _ =>
  }

  private object GetSamplingDataTick
  private object GetSamplingDataKey
}

class ExchangeablePerformanceModelAnalyzer(mapek: ExchangeablePerformanceModelMAPEK) extends ContrastAnalyzer(mapek) {
  override def getCurrentContextConfig(cfm: CFM): Future[Config] = {
    for {
      contextData <- (mapek.knowledge ? GetContextSample).mapTo[Map[Query, Sample]]
    } yield cfm.asInstanceOf[DynamicCFM].getCurrentContextConfigFromSamples(contextData)
  }
}

class ExchangeablePerformanceModelPlanner(mapek: ExchangeablePerformanceModelMAPEK)(implicit cluster: Cluster) extends PlannerComponent(mapek) {
  val optimizationTarget: String = ConfigFactory.load().getString("constants.mapek.exchangeable-model.optimization-target")
  lazy val allPlacementAlgorithms: List[PlacementStrategy] = ConfigFactory.load()
    .getStringList("benchmark.general.algorithms").asScala.toList
    .map(PlacementStrategy.getStrategyByName)
  lazy val queryPerformancePredictor: ActorRef = context.actorOf(Props(classOf[QueryPerformancePredictor], cluster))

  override def receive: Receive = super.receive orElse {
    case RunPlanner(cfm: CFM, contextConfig: Config, currentLatency: Double, qosRequirements: Set[Requirement]) =>
      for {
        currentPlacement: Option[Map[Query, ActorRef]] <- (mapek.knowledge ? GetOperators).mapTo[CurrentOperators].map(e => Some(e.placement))
        publisherEventRates: Map[String, Throughput] <- TCEPUtils.getPublisherEventRates()
        publishers: Map[String, ActorRef] <- TCEPUtils.getPublisherActors()
        contextSample <- (mapek.knowledge ? GetContextSample).mapTo[Map[Query, Sample]]
      } yield {
        val rootOperator: Query = mapek.getQueryRoot
        val queryDependencyMap = Queries.extractOperatorsAndThroughputEstimates(rootOperator)(publisherEventRates)
        Future.traverse(allPlacementAlgorithms)(p => {
          for {
            _ <- p.initialize()(ec, cluster, Some(publisherEventRates))
            placement: Map[Query, Address] <- p.initialVirtualOperatorPlacement(rootOperator, publishers)(ec, cluster, queryDependencyMap).map(_.map(e => e._1 -> e._2.member.address))
            pred <- (queryPerformancePredictor ? GetPredictionForPlacement(rootOperator, currentPlacement, placement, publisherEventRates, Some(contextSample))).mapTo[MetricPredictions]
          } yield p.name -> (pred, placement)
        }).map(p => {
          log.info("received predictions \n{}", p.map(e => e._1 -> e._2._1).mkString("\n"))
          // check each requirement
          val qosFulfillingPlacements = p.filter(placement => {
            qosRequirements.forall {
              case LatencyRequirement(operator, latency, otherwise, name) => Queries.compareHelper(latency.toMillis, operator, placement._2._1.E2E_LATENCY.amount.toMillis)
              case f: FrequencyRequirement => Queries.compareHelper(f.getEventsPerSec, f.operator, placement._2._1.THROUGHPUT.getEventsPerSec)
              case r: Requirement => {
                log.warning("ignoring requirement {} since there is no prediction model for it", r)
                true
              }
            }
          })

          if(qosFulfillingPlacements.nonEmpty) {
            val transitionTarget = if (optimizationTarget == "latency") qosFulfillingPlacements.minBy(_._2._1.E2E_LATENCY.amount)
            else if (optimizationTarget == "throughput") qosFulfillingPlacements.minBy(_._2._1.THROUGHPUT.getEventsPerSec)
            else throw new IllegalArgumentException(s"unknown optimization target metric $optimizationTarget")

            log.info("found requirement-fulfilling placement strategy {}:\n{}", transitionTarget._1, transitionTarget._2._1)
            mapek.executor ! ExecuteTransitionWithPlacement(transitionTarget._1, transitionTarget._2._2)
          } else {
            log.error("no placement strategy with sufficient predicted performance for the following requirements could be found: \n{}\n placement predictions were \n{}", qosRequirements.mkString("\n"), p.map(e => e._2._1).mkString("\n"))
          }
        })
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

class ExchangeablePerformanceModelKnowledge(query: Query, transitionConfig: TransitionConfig, startingPlacementStrategy: String)
  extends KnowledgeComponent(query, transitionConfig, startingPlacementStrategy) {

  var mostRecentSamples: Map[Query, Samples] = Map()
  val cfm: DynamicCFM = new DynamicCFM(query)

  override def receive: Receive = super.receive orElse {
        // received from DecentralizedMonitor
    case m: MonitorSamples =>
      mostRecentSamples = m.sampleMap.map(e => e._1._1 -> e._2)
      log.info("received context sample update, {} operators,  {} samples each", mostRecentSamples.size, mostRecentSamples.head._2.size)

    case GetCFM => sender() ! this.cfm
    case GetContextSample => sender() ! mostRecentSamples.map(e => e._1 -> e._2.head)
    case GetContextData => sender() ! Map()
    case msg => log.error(s"received unhandled msg $msg")
  }
}

object ExchangeablePerformanceModelMAPEK {
  case class MonitorSamples(sampleMap: Map[(Query, ActorRef), Samples])
  case object GetContextSample
}

