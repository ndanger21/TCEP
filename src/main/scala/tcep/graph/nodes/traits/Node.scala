package tcep.graph.nodes.traits

import akka.actor.{ActorLogging, ActorRef, Address, Cancellable, PoisonPill, Props}
import akka.event.LoggingReceive
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import tcep.data.Queries._
import tcep.graph.nodes.traits.Node.{NodeProperties, OperatorMigrationNotice, UnSubscribe, UpdateTask}
import tcep.graph.nodes.traits.TransitionExecutionModes.ExecutionMode
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.graph.qos.OperatorQosMonitor.{GetOperatorQoSMetrics, GetSamples, UpdateEventRateOut, UpdateEventSizeOut}
import tcep.graph.transition._
import tcep.graph.{CreatedCallback, EventCallback}
import tcep.machinenodes.helper.actors.{CreateRemoteOperator, PlacementMessage, RemoteOperatorCreated, TransitionControlMessage}
import tcep.machinenodes.qos.BrokerQoSMonitor.{CPULoadUpdate, GetIOMetrics}
import tcep.placement._
import tcep.placement.manets.StarksAlgorithm
import tcep.placement.mop.RizouAlgorithm
import tcep.placement.sbon.PietzuchAlgorithm
import tcep.publishers.Publisher.AcknowledgeSubscription
import tcep.simulation.adaptive.cep.SystemLoad
import tcep.simulation.tcep.GUIConnector
import tcep.utils.TCEPUtils

import java.time.Instant
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * The base class for all of the nodes
  **/
trait Node extends MFGSMode with SMSMode with NaiveMovingStateMode with NaiveStopMoveStartMode with ActorLogging {
  val np: NodeProperties
  val query: Query
  val hostInfo: HostInfo
  val name: String = self.path.name
  @volatile var started: Boolean = false
  val placementUpdateInterval: Int = ConfigFactory.load().getInt("constants.placement.update-interval")
  private var currAlgorithm: String = ""
  implicit val creatorAddress: Address = cluster.selfAddress

  var updateTask: Cancellable = _

  override def executeTransition(requester: ActorRef, algorithm: String, stats: TransitionStats, placement: Option[Map[Query, Address]]): Unit = {
    log.info(s"${self.path.name} executing transition on ${np.transitionConfig}")
    if (np.transitionConfig.transitionStrategy == TransitionModeNames.SMS) super[SMSMode].executeTransition(requester, algorithm, stats, placement)
    else if(np.transitionConfig.transitionStrategy == TransitionModeNames.NaiveMovingState) super[NaiveMovingStateMode].executeTransition(requester, algorithm, stats, placement)
    else if(np.transitionConfig.transitionStrategy == TransitionModeNames.NaiveStopMoveStart) super[NaiveStopMoveStartMode].executeTransition(requester, algorithm, stats, placement)
    else super[MFGSMode].executeTransition(requester, algorithm, stats, placement)
  }

  override def preStart(): Unit = {
    super.preStart()
    //executionStarted()
    // must explicitly start execution by receiving StartExecution(WithData/WithDependencies)
    // message from knowledge actor (initial) or predecessor (when transiting)
    scheduleHeartBeat()
    log.info(s"starting operator $self in transition mode $np.transitionConfig")
  }

  override def postStop(): Unit = {
    getParentActors().foreach(_ ! UnSubscribe())
    transitionLog(s"unsubscribed from parents and stopped self $self")
    super.postStop()
  }

  def scheduleHeartBeat() = {
    if (np.backupMode) {
      started = false
      this.context.watch(np.mainNode.get)
    }
  }

  def childNodeReceive: Receive

  override def receive: Receive = LoggingReceive {
    placementReceive orElse (
      if (np.transitionConfig.transitionStrategy == TransitionModeNames.SMS)
        super[SMSMode].transitionReceive
      else if(np.transitionConfig.transitionStrategy == TransitionModeNames.NaiveMovingState)
        super[NaiveMovingStateMode].transitionReceive
      else if(np.transitionConfig.transitionStrategy == TransitionModeNames.NaiveStopMoveStart)
        super[NaiveStopMoveStartMode].transitionReceive
      else
        super[MFGSMode].transitionReceive
      ) andThen childNodeReceive
  }

  def placementReceive: Receive = {
    case GetSamples => operatorQoSMonitor.forward(GetSamples)
    case UpdateEventRateOut(rate) => eventRateOut = rate
    case UpdateEventSizeOut(size) => eventSizeOut = size
    case CPULoadUpdate(load) => currentCPULoad = load // received from operatorQosMonitor after each sampling tick
    case GetIOMetrics => operatorQoSMonitor.forward(GetIOMetrics)
    case GetOperatorQoSMetrics => operatorQoSMonitor.forward(GetOperatorQoSMetrics)
    case AcknowledgeSubscription(_) if getParentActors.contains(sender()) => log.debug(s"received subscription ACK from ${sender()}")
    case StartExecution(algorithm) =>
      val s = sender()
      if(!started) {
        started = true
        self ! UpdateTask(algorithm)
        // during tests, this message is sent only to the root and then forwarded to parents to ensure sequential start
        // during normal operation, the message is broadcast to all operators at once
        for {
          subscribed <- subscribeToParents()
          parentsStarted <- Future.traverse(getParentActors())(parent => (parent ? StartExecution(algorithm))(retryTimeout).mapTo[AcknowledgeStart])
        } yield {
          transitionLog(s"received StartExecution($algorithm): started operator $this $started and ${getParentActors().size} parents")
          s ! AcknowledgeStart()
        }
      }

    // placement update task that must be set/sent by the actor deploying this operator
    // 1. receive notification after initial deployment, setting up a task on each operator
    // 2. periodically execute placement algorithm locally, migrate to new node (see transition) if necessary
    // 3. notify dependencies of new location
    case UpdateTask(algorithmType: String) => {
      currAlgorithm = algorithmType
      val algorithm = algorithmType match {
        case PietzuchAlgorithm.name => PietzuchAlgorithm
        case RizouAlgorithm.name => RizouAlgorithm
        case StarksAlgorithm.name => StarksAlgorithm
        case RandomAlgorithm.name => RandomAlgorithm
        case MobilityTolerantAlgorithm.name => MobilityTolerantAlgorithm
        case GlobalOptimalBDPAlgorithm.name => GlobalOptimalBDPAlgorithm
        case other: String => throw new NoSuchElementException(s"need to add algorithm type $other to updateTask!")
      }
      if (algorithm.hasPeriodicUpdate()) {
        val task: Runnable = () => {
          implicit val ec: ExecutionContext = blockingIoDispatcher
          val startTime = System.currentTimeMillis()
          val dependencies = getDependencies()
          for {
            init <- algorithm.initialize()
            optimalHost: HostInfo <- algorithm.findOptimalNode(hostInfo.operator, hostInfo.operator, dependencies, HostInfo(cluster.selfMember, hostInfo.operator))
          } yield {
            if (!cluster.selfAddress.equals(optimalHost.member.address)) {
              log.info(s"Relaxation periodic placement update for ${query}: " +
                s"\n moving operator from ${cluster.selfMember} to ${optimalHost.member} " +
                s"\n with dependencies $dependencies")
              // creates copy of operator on new host
              for { newOperator: ActorRef <- createDuplicateNode(optimalHost) } yield {

              this.started = false
              val downTime = System.currentTimeMillis()
                // TODO support other modes
              if (np.transitionConfig.transitionStrategy == TransitionModeNames.MFGS) newOperator ! StartExecutionWithData(downTime, startTime, subscribers.toList, slidingMessageQueue.toList, algorithmType)
              else newOperator ! StartExecutionAtTime(subscribers.toList, Instant.now(), algorithmType)
              newOperator ! UpdateTask(algorithmType)
              // inform subscribers about new operator location; parent will receive
              subscribers.keys.foreach(_ ! OperatorMigrationNotice(self, newOperator))

              // taken from MFGSMode
              // TODO maybe make regular operator migrations distinguishable from transitions
              val timestamp = System.currentTimeMillis()
              val migrationTime = timestamp - downTime
              val nodeSelectionTime = timestamp - startTime
              GUIConnector.sendOperatorTransitionUpdate(self, newOperator, algorithm.name, timestamp, migrationTime, nodeSelectionTime, getParentActors(), optimalHost, np.isRootOperator)(selfAddress = cluster.selfAddress, blockingIoDispatcher)

              log.info(s"${self} shutting down Self")
              SystemLoad.operatorRemoved()
              updateTask.cancel()
              self ! PoisonPill

            }} else log.info(s"periodic placement update for ${query}: no change of host \n dependencies: ${dependencies}")
          }
        }

        updateTask = context.system.scheduler.schedule(placementUpdateInterval seconds, placementUpdateInterval seconds, task)
        log.info(s"received placement update task, setting up periodic placement update every ${placementUpdateInterval}s")
      }
    }
  }


  def createDuplicateNode(duplicateHostInfo: HostInfo): Future[ActorRef] = {
    val startTime = System.currentTimeMillis()
    val props = Props(getClass, query, duplicateHostInfo, np)
    for {
      taskManager <- TCEPUtils.getTaskManagerOfMember(cluster, duplicateHostInfo.member)
      deployDuplicate <- TCEPUtils.guaranteedDelivery(context, taskManager, CreateRemoteOperator(duplicateHostInfo, props), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher)).mapTo[RemoteOperatorCreated]
    } yield {
      log.info(s"$self spent ${System.currentTimeMillis() - startTime} milliseconds to create duplicate ${deployDuplicate.ref}")
      deployDuplicate.ref
    }
  }


  def toAnyRef(any: Any): AnyRef = {
    // Yep, an `AnyVal` can safely be cast to `AnyRef`:
    // https://stackoverflow.com/questions/25931611/why-anyval-can-be-converted-into-anyref-at-run-time-in-scala
    any.asInstanceOf[AnyRef]
  }

}



object Node {
  case class NodeProperties(transitionConfig: TransitionConfig,
                            backupMode: Boolean,
                            mainNode: Option[ActorRef],
                            createdCallback: Option[CreatedCallback],
                            eventCallback: Option[EventCallback],
                            isRootOperator: Boolean,
                            monitorCentral: ActorRef,
                            var parentActor: Seq[ActorRef]
                           )
  case class Subscribe(subscriber: ActorRef, operator: Query) extends PlacementMessage
  case class UnSubscribe() extends PlacementMessage
  case class UpdateTask(algorithmType: String) extends PlacementMessage
  /*
  object Dependencies {
    def buildFromActorMap(parents: Map[ActorRef, Query], children: Map[Option[ActorRef], Query])(implicit cluster: Cluster): Dependencies = {
      Dependencies(parents, children)
        parents.map(e => e._2 -> cluster.state.members.find(_.address == e._1.path.address).getOrElse(cluster.selfMember)),
        children.map(e =>
                       if(e._1.isDefined) e._2 -> Some(cluster.state.members.find(_.address == e._1.get.path.address).getOrElse(cluster.selfMember))
                       else e._2 -> None
                     )
        )
    }
  }
  */
  // subscribers: None means that subscriber is not yet instantiated/deployed
  case class Dependencies(parents: Map[ActorRef, Query], subscribers: Map[Option[ActorRef], Query])
  case class OperatorMigrationNotice(oldOperator: ActorRef, newOperator: ActorRef) extends PlacementMessage
  case object GetWindowStateEvents extends TransitionControlMessage
}

case class TransitionConfig(
                             transitionStrategy: Mode = TransitionModeNames.MFGS,
                             transitionExecutionMode: ExecutionMode = TransitionExecutionModes.CONCURRENT_MODE)

object TransitionExecutionModes extends Enumeration {
  type ExecutionMode = Value
  val CONCURRENT_MODE, SEQUENTIAL_MODE = Value
}

object TransitionModeNames extends Enumeration {
  type Mode = Value
  val SMS, MFGS, NaiveMovingState, NaiveStopMoveStart = Value
}
