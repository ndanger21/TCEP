package tcep.graph.transition

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Address, PoisonPill}
import akka.cluster.Cluster
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import tcep.data.Events.Event
import tcep.data.Queries
import tcep.data.Queries._
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.transition.MAPEK._
import tcep.graph.transition.mapek.ExchangeablePerformanceModelMAPEK
import tcep.graph.transition.mapek.contrast.ContrastMAPEK
import tcep.graph.transition.mapek.learnon.LearnOnMAPEK
import tcep.graph.transition.mapek.lightweight.LightweightMAPEK
import tcep.graph.transition.mapek.requirementBased.RequirementBasedMAPEK
import tcep.machinenodes.helper.actors.TransitionControlMessage
import tcep.placement.PlacementStrategy
import tcep.placement.benchmarking.{BenchmarkingNode, NetworkChurnRate}
import tcep.placement.sbon.PietzuchAlgorithm
import tcep.utils.SpecialStats

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait MAPEKComponent extends Actor with ActorLogging {
  val cluster = Cluster(context.system)
  implicit val timeout: Timeout = Timeout(5 seconds)
  // do not use the default dispatcher for futures to avoid starvation at high event rates
  lazy val blockingIoDispatcher: ExecutionContext = context.system.dispatchers.lookup("blocking-io-dispatcher")
  implicit val ec: ExecutionContext = blockingIoDispatcher
}
abstract class MonitorComponent(mapek: MAPEK) extends MAPEKComponent {
	var currentPlacementStrategy: String = ""
  override def receive: Receive = {
    case r: AddRequirement =>
      mapek.knowledge ! r
      mapek.analyzer ! r

    case r: RemoveRequirement => mapek.knowledge ! r
    case SetPlacementStrategy(newStrategy) => currentPlacementStrategy = newStrategy
  }
}

trait AnalyzerComponent extends MAPEKComponent
abstract class PlannerComponent(mapek: MAPEK) extends MAPEKComponent {

  override def receive: Receive = {

    case ManualTransition(algorithmName) =>
      log.info(s"sending manualTransition request for $algorithmName to executor ")
      mapek.executor ! ExecuteTransition(algorithmName)
  }
}

abstract class ExecutorComponent(mapek: MAPEK) extends MAPEKComponent {

  override def preStart() = {
    super.preStart()
    log.info("starting MAPEK Executor")
  }

  override def receive: Receive = {

    case ExecuteTransition(strategyName) =>
      log.info(s"received ExecuteTransition to $strategyName")
      for {
        currentStrategyName <- (mapek.knowledge ? GetPlacementStrategyName).mapTo[String]
        client<- (mapek.knowledge ? GetClient).mapTo[ActorRef]
        mode <- (mapek.knowledge ? GetTransitionMode).mapTo[TransitionConfig]
        status <- (mapek.knowledge ? GetTransitionStatus).mapTo[Int]
      } yield {
        if (currentStrategyName != strategyName && status != 1) {
          mapek.knowledge ! SetPlacementStrategy(strategyName)
          log.info(s"executing $mode transition to ${strategyName}")
          client ! TransitionRequest(strategyName, self, TransitionStats(0, 0, System.currentTimeMillis()), None)
        } else log.info(s"received ExecuteTransition message: not executing $mode transition to $strategyName " +
          s"since it is already active or another transition is still in progress (status: $status)")
      }
  }
}

abstract case class KnowledgeComponent(mapek: MAPEK, query: Query, var transitionConfig: TransitionConfig, var currentPlacementStrategy: String) extends MAPEKComponent {
  var client: ActorRef = _
  protected val requirements: scala.collection.mutable.Set[Requirement] = scala.collection.mutable.Set(pullRequirements(query, List()).toSeq: _*)
  var deploymentComplete: Boolean = false
  var lastTransitionEnd: Long = System.currentTimeMillis()
  var lastTransitionDuration: Long = 0
  var transitionStatus: Int = 0
  var lastTransitionStats: TransitionStats = TransitionStats()
  var previousLatencies: Vector[(Long, Long)] = Vector()
  //val freqMonitor = AverageFrequencyMonitorFactory(query, None).createNodeMonitor
  var operators: Map[Query, ActorRef] = Map()
  var backupOperators: Set[ActorRef] = Set()

  override def receive: Receive = {

    case IsDeploymentComplete => sender() ! this.deploymentComplete
    case SetDeploymentStatus(isComplete) => this.deploymentComplete = isComplete
    case GetPlacementStrategyName => sender() ! currentPlacementStrategy
    case SetPlacementStrategy(newStrategy) =>
      this.currentPlacementStrategy = newStrategy
			mapek.monitor ! SetPlacementStrategy(newStrategy)
      log.info(s"updated current placementStrategy to $currentPlacementStrategy")
    case GetRequirements => sender() ! this.requirements.toList
    case ar: AddRequirement =>
      ar.requirements.foreach(req => this.requirements += req)
      log.info(s"added requirements ${ar.requirements}")
    case rm: RemoveRequirement =>
      rm.requirements.foreach(req => this.requirements -= req)
      log.info(s"removed requirements ${rm.requirements}")
    case GetTransitionMode => sender() ! transitionConfig
    case SetTransitionMode(mode: TransitionConfig) => this.transitionConfig = mode
    case GetTransitionStatus => sender() ! this.transitionStatus
    case SetTransitionStatus(status: Int) =>
      this.transitionStatus = status
      this.client ! SetTransitionStatus(status)
    case GetLastTransitionStats => sender() ! (this.lastTransitionStats, this.lastTransitionDuration)
    case SetLastTransitionStats(stats) =>
      this.lastTransitionEnd = System.currentTimeMillis()
      this.lastTransitionStats = stats
      this.lastTransitionDuration = lastTransitionEnd - stats.transitionStartAtKnowledge
      val reqname = if(requirements.nonEmpty) requirements.head.name
      SpecialStats.log(this.getClass.toString, s"transitionStats-perQuery-${transitionConfig}-${currentPlacementStrategy}}",
        s"total;transition time;${lastTransitionDuration};ms;" +
          s"placementOverheadKBytes;${lastTransitionStats.placementOverheadBytes / 1000.0};" +
          s"transitionOverheadKBytes;${lastTransitionStats.transitionOverheadBytes / 1000.0};" +
          s"combinedOverheadKBytes;${(lastTransitionStats.transitionOverheadBytes + lastTransitionStats.placementOverheadBytes) / 1000.0}")
      //lastTransitionStats.transitionTimesPerOperator.foreach(op =>
      // SpecialStats.log(this.getClass.toString, s"transitionStats-$transitionConfig-${currentPlacementStrategy.name}-${reqname}", s"operator;${op._1};${op._2};"))

    case GetLastTransitionDuration => sender() ! this.lastTransitionDuration
    case GetLastTransitionEnd => sender() ! lastTransitionEnd
    case GetClient => sender() ! this.client
    case SetClient(clientNode: ActorRef) => this.client = clientNode
    case AddOperator(operator: (Query, ActorRef)) =>
      this.operators = operators.+(operator)
      log.info(s"updated operator placement:\n {} -> {}\n now {} total)", operator._1, operator._2, operators.size)
    case AddBackupOperator(operator: ActorRef) => this.backupOperators = backupOperators.+(operator)
    case RemoveBackupOperator(operator: ActorRef) => this.backupOperators = backupOperators.-(operator)
    case GetOperators => sender() ! CurrentOperators(operators)
    case GetBackupOperators => sender() ! this.backupOperators.toList
    case GetOperatorCount => sender() ! this.operators.size
    case NotifyOperators(msg: TransitionControlMessage) =>
      log.info(s"broadcasting message $msg to ${operators.size} operators: \n ${operators.mkString("\n")}")
      operators.foreach { op => op._2 ! msg }

    case GetAverageLatency(intervalMs) => sender() ! this.calculateMovingAvg(intervalMs)

    case UpdateLatency(latency) =>
      //freqMonitor.onEventEmit(Event1(true)(cluster.selfAddress), 0)
      previousLatencies = (System.nanoTime(), latency) +: previousLatencies
      if(previousLatencies.size > 1e6) { // prevent the queue from growing endlessly when not calling calculateMovingAvg
        log.warning("list of previous latency values has reached 1000000 entries, discarding half of the entries")
        previousLatencies = previousLatencies.take(500000)
      }

    case TransitionStatsSingle(operator, timetaken, placementOverheadBytes, transitionOverheadBytes) =>
      SpecialStats.log(this.getClass.toString, s"transitionStats-perOperator-$transitionConfig-${currentPlacementStrategy}",
        s"operator;$operator;$timetaken;ms;${placementOverheadBytes / 1000.0};kByte;${transitionOverheadBytes / 1000.0};kByte;${(transitionOverheadBytes + placementOverheadBytes) / 1000.0};kByte")

  }

  /**
    * calculate average latency over the given last x milliseconds
    * discards all older values from the queue
    */
  def calculateMovingAvg(intervalMs: Long): Double = {
    val start = System.nanoTime()
    val intervalInNS: Double = intervalMs * 1e6
    val lastIntervalLatencySum = previousLatencies.foldLeft((0.0, 0))((acc, e) =>
      if(System.nanoTime() - e._1 <= intervalInNS) (acc._1 + e._2, acc._2 + 1)
      else acc
    )
    val avg = if(lastIntervalLatencySum._2 != 0) lastIntervalLatencySum._1 / lastIntervalLatencySum._2 else 0
    this.previousLatencies = previousLatencies.take(lastIntervalLatencySum._2) // keep only the latency values of the last interval
    //log.info(s"avg for last $intervalMs ms is ${avg}, ${lastIntervalLatencySum._2} of ${previousLatencies.size} are in interval; calc took ${(System.nanoTime() - start) / 1e6}ms")
    avg
  }

}

abstract class MAPEK(context: ActorContext) {
  val samplingInterval = new FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.MILLISECONDS)
  val monitor: ActorRef
  val analyzer: ActorRef
  val planner: ActorRef
  val executor: ActorRef
  val knowledge: ActorRef

  def stop(): Unit = {
    monitor ! PoisonPill
    analyzer ! PoisonPill
    planner ! PoisonPill
    executor ! PoisonPill
    knowledge ! PoisonPill
  }
}

object MAPEK {
  val log = LoggerFactory.getLogger(getClass)
  def createMAPEK(mapekType: String, query: Query, transitionConfig: TransitionConfig, startingPlacementStrategy: Option[String], consumer: ActorRef, fixedSimulationProperties: Map[Symbol, Int] = Map(), pimPaths: (String, String))(implicit cluster: Cluster, context: ActorContext): MAPEK = {

    val placementAlgorithm = // get correct PlacementAlgorithm case class for both cases (explicit starting algorithm and implicit via requirements)
      if(startingPlacementStrategy.isEmpty) BenchmarkingNode.selectBestPlacementAlgorithm(List(), Queries.pullRequirements(query, List()).toList) // implicit
      else BenchmarkingNode.algorithms.find(_.placement.name == startingPlacementStrategy.getOrElse(PietzuchAlgorithm.name)).getOrElse( // explicit
        throw new IllegalArgumentException(s"missing configuration in application.conf for algorithm $startingPlacementStrategy"))
    val availableMapekTypes = ConfigFactory.load().getStringList("constants.mapek.availableTypes")
    assert(availableMapekTypes.contains(mapekType), s"mapekType must be either of $availableMapekTypes but was: $mapekType")
    if(startingPlacementStrategy.isDefined) assert(BenchmarkingNode.algorithms.exists(p => p.placement.name == startingPlacementStrategy.get), s"unknown starting placement algorithm name: $startingPlacementStrategy")
    val stratName = startingPlacementStrategy.getOrElse(PietzuchAlgorithm.name)
    mapekType match {
        // if no algorithm is specified, start with pietzuch, since no context information available yet
      case "requirementBased" => new RequirementBasedMAPEK(context, query, transitionConfig, placementAlgorithm)
      case "CONTRAST" => new ContrastMAPEK(context, query, transitionConfig, stratName, fixedSimulationProperties, consumer)
      case "lightweight" => new LightweightMAPEK(context, query, transitionConfig, placementAlgorithm.placement.name, consumer)
      case "LearnOn" => new LearnOnMAPEK(context, transitionConfig, query, stratName, fixedSimulationProperties, consumer, pimPaths)
      case "ExchangeablePerformanceModel" => new ExchangeablePerformanceModelMAPEK(query, transitionConfig, stratName, consumer)
      case _ =>
        log.error(s"unknown mapek name: $mapekType")
        throw new IllegalArgumentException(s"unknown mapek name: $mapekType")
    }

  }

  // message for inter-MAPEK-component communication
  case class AddRequirement(requirements: Seq[Requirement])
  case class RemoveRequirement(requirements: Seq[Requirement])
  case class ManualTransition(algorithmName: String)
  case class ExecuteTransition(algorithmName: String)
  case class ExecuteTransitionWithPlacement(algorithmName: String, placement: Map[Query, Address]) extends TransitionControlMessage
  case object GetRequirements
  case object GetTransitionMode
  case class SetTransitionMode(mode: TransitionConfig)
  case object GetTransitionStatus
  case class SetTransitionStatus(status: Int)
  case object GetLastTransitionStats
  case object GetLastTransitionDuration
  case class SetLastTransitionStats(stats: TransitionStats)
  case object GetLastTransitionEnd
  case object GetClient
  case class SetClient(clientNode: ActorRef)
  case class NotifyOperators(msg: TransitionControlMessage)
  case class AddOperator(opInstance: (Query, ActorRef))
  case class AddBackupOperator(ref: ActorRef)
  case class RemoveOperator(opInstance: (Query, ActorRef))
  case class RemoveBackupOperator(operator: ActorRef)
  case object GetOperators
  case class CurrentOperators(placement: Map[Query, ActorRef])
  case object GetBackupOperators
  case object GetOperatorCount
  case object GetPlacementStrategyName
  case class SetPlacementStrategy(strategy: String)
  case object IsDeploymentComplete
  case class SetDeploymentStatus(complete: Boolean)
  case class GetAverageLatency(fromLastIntervalMs: Long)
  case class UpdateLatency(latency: Long)
}

case class ChangeInNetwork(networkProperty: NetworkChurnRate)
case class ScheduleTransition(strategy: PlacementStrategy)

case class TransitionRequest(placementStrategyName: String, requester: ActorRef, transitionStats: TransitionStats, placement: Option[Map[Query, Address]]) extends TransitionControlMessage
case class StopExecution() extends TransitionControlMessage
case class StartExecution(algorithmType: String) extends TransitionControlMessage
case class AcknowledgeStart() extends TransitionControlMessage
case class SaveStateAndStartExecution(state: List[Any]) extends TransitionControlMessage
case class StartExecutionWithData(downTime:Long, startTime: Long, subscribers: List[(ActorRef, Query)], data: List[(ActorRef, Event)], algorithmType: String) extends TransitionControlMessage
case class StartExecutionAtTime(subscribers: List[(ActorRef, Query)], startTime: Instant, algorithmType: String) extends TransitionControlMessage
case class TransferEvents(downTime: Long, startTime: Long, subscribers: List[(ActorRef, Query)], windowEvents: List[(ActorRef, Event)], unsentEvents: List[(ActorRef, Event)], algorithmName: String) extends TransitionControlMessage
case class TransferredState(placementAlgo: String, newParent: ActorRef, oldParent: ActorRef, transitionStats: TransitionStats, lastOperator: Query, placement: Option[Map[Query, Address]]) extends TransitionControlMessage
case class MoveOperator(requester: ActorRef, algorithm: String, stats: TransitionStats, placement: Option[Map[Query, Address]]) extends TransitionControlMessage
case object SuccessorStart extends TransitionControlMessage
// only used inside TransitionRequest and TransferredState; transitionOverheadBytes must be updated on receive of TransitionRequest and TransferredState, placementOverheadBytes on operator placement completion
case class TransitionStats(
                            placementOverheadBytes: Long = 0, transitionOverheadBytes: Long = 0, transitionStartAtKnowledge: Long = System.currentTimeMillis(),
                            transitionTimesPerOperator: Map[ActorRef, Long] = Map(), // record transition duration per operator
                            transitionEndParent: Long = System.currentTimeMillis()) // for SMS mode delay estimation
case class TransitionStatsSingle(operator: ActorRef, timetaken: Long, placementOverheadBytes: Long, transitionOverheadBytes: Long)
case class TransitionDuration(operator: ActorRef, timetaken: Long)
