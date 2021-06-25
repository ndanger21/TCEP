package tcep.graph.transition.mapek

import akka.actor.{ActorContext, ActorLogging, ActorRef, Address, Props, Timers}
import akka.cluster.Cluster
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.cardygan.config.Config
import org.discovery.vivaldi.Coordinates
import tcep.data.Queries.{Query, Requirement}
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.qos.OperatorQosMonitor.{GetSamples, Sample, Samples}
import tcep.graph.transition.MAPEK.{CurrentOperators, GetOperators}
import tcep.graph.transition.mapek.contrast.ContrastMAPEK.{GetCFM, GetContextData, RunPlanner}
import tcep.graph.transition.mapek.contrast._
import tcep.graph.transition.{KnowledgeComponent, MAPEK}
import tcep.placement.PlacementStrategy
import tcep.prediction.PredictionHelper.Throughput
import tcep.prediction.QueryPerformancePredictor
import tcep.prediction.QueryPerformancePredictor.GetPredictionForPlacement
import tcep.utils.TCEPUtils

import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

class ExchangeablePerformanceModelMAPEK(query: Query, mode: TransitionConfig, startingPlacementStrategy: PlacementStrategy, consumer: ActorRef)(implicit cluster: Cluster, context: ActorContext)
  extends ContrastMAPEK(context, query, mode, startingPlacementStrategy, Map(), consumer) {

  override val monitor: ActorRef = context.actorOf(Props(classOf[DecentralizedMonitor], this, cluster), "MonitorEndpoint")
  override val analyzer: ActorRef = context.actorOf(Props(new ContrastAnalyzer(this)))
  override val planner: ActorRef = context.actorOf(Props(classOf[ExchangeablePerformanceModelPlanner], this))
  override val executor: ActorRef = context.actorOf(Props(classOf[ContrastExecutor],  this))
  override val knowledge: ActorRef = context.actorOf(Props(classOf[ExchangeablePerformanceModelKnowledge], query, mode, startingPlacementStrategy), "knowledge")
  def getQueryRoot: Query = query
}

/**
  * M of the MAPE-K cycle
  * responsible for retrieving sample data from the distributed monitor instances on operators and the brokers they are deployed on and sending it to knowledge
  */
class DecentralizedMonitor(mapek: MAPEK)(implicit cluster: Cluster) extends MonitorComponent(mapek) with ActorLogging with Timers {

  val samplingInterval: FiniteDuration = FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.MILLISECONDS)
  implicit val askTimeout: Timeout = Timeout(FiniteDuration(ConfigFactory.load().getInt("constants.default-request-timeout"), TimeUnit.SECONDS))
  implicit val blockingIoDispatcher: ExecutionContext = cluster.system.dispatchers.lookup("blocking-io-dispatcher")

  override def preStart(): Unit = {
    timers.startTimerWithFixedDelay(GetSamplingDataKey, GetSamplingDataTick, samplingInterval)
  }

  override def receive: Receive = {
    case GetSamplingDataTick => mapek.knowledge ! GetOperators
    case CurrentOperators(placement) =>
      if(placement.nonEmpty) {
        val mostRecentSamples: Future[Map[Query, Samples]] = TCEPUtils.makeMapFuture(placement.map(op => op._1 -> (op._2 ? GetSamples).mapTo[Samples]))
        mostRecentSamples.pipeTo(mapek.knowledge)
      }
    case _ =>
  }

  private object GetSamplingDataTick
  private object GetSamplingDataKey
}

class ExchangeablePerformanceModelAnalyzer(mapek: ExchangeablePerformanceModelMAPEK) extends ContrastAnalyzer(mapek) {
  override def getCurrentContextConfig(cfm: CFM): Future[Config] = for {
    contextData <- (mapek.knowledge ? GetContextData).mapTo[Map[Query, Sample]]
  } yield cfm.asInstanceOf[DynamicCFM].getCurrentContextConfig(contextData)
}

class ExchangeablePerformanceModelPlanner(mapek: ExchangeablePerformanceModelMAPEK)(implicit cluster: Cluster) extends ContrastPlanner(mapek) {
  lazy val allPlacementAlgorithms: List[PlacementStrategy] = ConfigFactory.load()
    .getStringList("benchmark.general.algorithms").asScala.toList
    .map(PlacementStrategy.getStrategyByName)
  lazy val queryPerformancePredictor: ActorRef = context.actorOf(Props(classOf[QueryPerformancePredictor], cluster))

  //TODO continue here
  override def receive: Receive = {
    case RunPlanner(cfm: CFM, contextConfig: Config, currentLatency: Double, qosRequirements: Set[Requirement]) =>
      for {
        currentPlacement: Option[Map[Query, ActorRef]] <- (mapek.knowledge ? GetOperators).mapTo[CurrentOperators].map(e => Some(e.placement))
        publisherEventRates: Map[String, Throughput] <- TCEPUtils.getPublisherEventRates()
      } yield {

        val rootOperator: Query = mapek.getQueryRoot

        allPlacementAlgorithms.map(p => {
          val newPlacement = for {
            _ <- p.initialize()(ec, cluster, Some(publisherEventRates))
            placementCoords: Map[Query, Coordinates] <- p.initialVirtualOperatorPlacement()
            placement: Map[Query, Address] <- placementCoords.map(e => e._1 -> p.findHost(e._1, e._2))
          }
          queryPerformancePredictor ? GetPredictionForPlacement(rootOperator, currentPlacement, newPlacement, publisherEventRates)
        })

      }

    case _ =>
  }
}

class ExchangeablePerformanceModelKnowledge(query: Query, transitionConfig: TransitionConfig, startingPlacementStrategy: PlacementStrategy)
  extends KnowledgeComponent(query, transitionConfig, startingPlacementStrategy) {

  var mostRecentSamples: Map[Query, Samples] = Map()
  val cfm: DynamicCFM = new DynamicCFM(query)

  override def receive: Receive = super.receive orElse {
        // received from DecentralizedMonitor
    case monitorSamples: Map[Query, Samples] => mostRecentSamples = monitorSamples

    case GetCFM => sender() ! this.cfm
    case GetContextData => sender() ! mostRecentSamples.map(e => e._1 -> e._2.head)
    case _ =>
  }
}


