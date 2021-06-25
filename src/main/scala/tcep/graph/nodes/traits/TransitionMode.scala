package tcep.graph.nodes.traits

import akka.actor.{ActorLogging, ActorRef, Address, Props, Terminated}
import akka.cluster.Cluster
import tcep.ClusterActor
import tcep.data.Events.{Event, updateMonitoringData}
import tcep.data.Queries.Query
import tcep.graph.EventCallback
import tcep.graph.nodes.traits.Node.{Dependencies, Subscribe, UnSubscribe}
import tcep.graph.qos.OperatorQosMonitor
import tcep.graph.transition.MAPEK.{AddOperator, RemoveOperator}
import tcep.graph.transition._
import tcep.machinenodes.helper.actors.ACK
import tcep.placement.{HostInfo, OperatorMetrics, PlacementStrategy}
import tcep.publishers.Publisher.AcknowledgeSubscription
import tcep.utils.TCEPUtils

import java.time.Duration
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
/**
  * Common methods for different transition modes
  */
trait TransitionMode extends ClusterActor with SystemLoadUpdater with ActorLogging {
  val query: Query
  val modeName: String = "transition-mode name not specified"
  val parentOperators: mutable.Map[ActorRef, Query] = mutable.Map()
  val subscribers: mutable.Map[ActorRef, Query] = mutable.Map()
  val isRootOperator: Boolean
  @volatile var started: Boolean
  val backupMode: Boolean
  val mainNode: Option[ActorRef]
  val hostInfo: HostInfo
  val transitionConfig: TransitionConfig
  var transitionInitiated = false
  val slidingMessageQueue: ListBuffer[(ActorRef, Event)]
  var eventRateOut: Double = 0.0d
  var eventSizeOut: Long = 0
  val operatorQoSMonitor: ActorRef = context.actorOf(Props(classOf[OperatorQosMonitor], self), "operatorQosMonitor") //TODO use custom dispatcher?
  val brokerQoSMonitor = context.system.actorSelection(context.system./("TaskManager*")./("BrokerQosMonitor*"))

  def createDuplicateNode(hostInfo: HostInfo): Future[ActorRef]
  // subscribe to events from parent actors (and acquire their operator type)
  def subscribeToParents(): Future[Unit] = {
    for {
      acks <- Future.traverse(getParentActors())(parent => {
        transitionLog(s"subscribing for events from $parent due to subscribeToParents() call")
        TCEPUtils.guaranteedDelivery(context, parent, Subscribe(self, query))(blockingIoDispatcher).mapTo[AcknowledgeSubscription]
          .map(ack => parentOperators += parent -> ack.acknowledgingParent)
      })
    } yield {
      acks.head
    }
  }

  protected def updateParentOperatorMap(oldParent: ActorRef, successor: ActorRef): Unit = {
    val op = parentOperators.remove(oldParent).getOrElse(throw new RuntimeException(
      s"trying to remove non-existent parent $oldParent from \n $parentOperators"))
    parentOperators += successor -> op
  }

  def getParentOperatorMap(): Map[ActorRef, Query] = parentOperators.toMap
  def getParentActors(): List[ActorRef]
  def getChildOperators(): Map[ActorRef, Query] = subscribers.toMap
  def getDependencies(): Dependencies = Dependencies(getParentOperatorMap(), getChildOperators().map(e => Some(e._1) -> e._2))
  def maxWindowTime(): Duration = Duration.ofSeconds(0)
  def getWindowStateEvents(): List[Any] = List()
  def insertWindowStateEvents(data: List[Any]): Unit = {}


  def emitEvent(event: Event, eventCallback: Option[EventCallback]): Unit = {
    if (started) {
      updateMonitoringData(log, event, hostInfo, currentLoad, eventRateOut, eventSizeOut)
      subscribers.keys.foreach(sub => {
        //SpecialStats.log(s"$this", s"sendEvent_${currAlgorithm}_${self.path.name}", s"STREAMING EVENT $event FROM ${s} TO ${sub}")
        //log.debug(s"STREAMING EVENT $event TO ${sub}")
        if (eventCallback.isDefined) {
          log.debug(s"applying callback to event $event")
          eventCallback.get.apply(event)
        }
        sub ! event
        operatorQoSMonitor ! event
      })
    } else {
      log.info(s"discarding event $event, started $started, parents: $getParentActors")
    }
  }

  // Notice I'm using `PartialFunction[Any,Any]` which is different from akka's default
  // receive partial function which is `PartialFunction[Any,Unit]`
  // because I needed to `cache` some messages before forwarding them to the child nodes
  // @see the receive method in Node trait for more details.
  def transitionReceive: PartialFunction[Any, Any] = super.receive orElse {
    case Subscribe(sub, op) =>
      subscribers += sub -> op
      sender() ! AcknowledgeSubscription(query)
      Future { context.watchWith(sub, UnSubscribe()) }(blockingIoDispatcher) // remove terminated operators from subscriber list
      transitionLog(s"new subscription by ${sub.path.name}, now ${subscribers.size} subscribers")
      log.info(s"self ${this.self.path.name} \n was subscribed by ${sub.path.name}, \n now has ${subscribers.size} subscribers")

    case UnSubscribe() =>
      val s = sender()
      subscribers -= s
      transitionLog(s"unsubscribed by ${s.path.name}, now ${subscribers.size} subscribers")
      log.info(s"self ${this.self.path.name} was unsubscribed by ${s.path.name}, \n now has ${subscribers.size} subscribers")

   case TransitionRequest(algorithm, requester, stats, placement) =>
      val s = sender()
      s ! ACK()
      if(!transitionInitiated){
        transitionInitiated = true
        handleTransitionRequest(requester, algorithm, updateTransitionStats(stats, s, transitionRequestSize(s)), placement)
      }

    case TransferredState(algorithm, successor, oldParent, stats, lastOperator, placement) =>
      sender() ! ACK() // this is a temporary deliverer actor created by oldParent
      val transitionStart: Long = stats.transitionTimesPerOperator.getOrElse(oldParent, 0)
      val transitionDuration = System.currentTimeMillis() - transitionStart
      val opmap = stats.transitionTimesPerOperator.updated(oldParent, transitionDuration)
      //log.info(s"old parent $oldParent  \n transition start: $transitionStart, transitionDuration: ${transitionDuration} , operator stats: \n ${opmap.mkString("\n")}")
      TransferredState(algorithm, successor, oldParent, updateTransitionStats(stats, oldParent, transferredStateSize(oldParent), updatedOpMap = Some(opmap) ), lastOperator, placement) //childReceive will handle this message

    case Terminated(killed) =>
      if (killed.equals(mainNode.get)) {
        started = true
      }

    case unhandled =>
      unhandled //forwarding to childreceive

  }

  def handleTransitionRequest(requester: ActorRef, algorithmName: String, stats: TransitionStats, placement: Option[Map[Query, Address]]): Unit

  def executeTransition(requester: ActorRef, algorithmName: String, stats: TransitionStats, placement: Option[Map[Query, Address]]): Unit

  def sendTransitionStats(operator: ActorRef, transitionTime: Long, placementBytes: Long, transitionBytes: Long): Future[Unit] = {
    implicit val timeout = TCEPUtils.timeout
    for {
      knowledgeActor <- TCEPUtils.selectKnowledge(cluster).resolveOne().mapTo[ActorRef]
    } yield {
      knowledgeActor ! TransitionStatsSingle(self, transitionTime, placementBytes, transitionBytes)
    }
  }

  def notifyMAPEK(cluster: Cluster, successor: ActorRef): Future[Unit] = {
    implicit val timeout = TCEPUtils.timeout
    brokerQoSMonitor ! AddOperator((query, successor))
    brokerQoSMonitor ! RemoveOperator((query, self))
    for {
      knowledgeActor <- TCEPUtils.selectKnowledge(cluster).resolveOne().mapTo[ActorRef]
    } yield {
      transitionLog(s"notifying MAPEK knowledge component ${knowledgeActor} and broker QoS monitor $brokerQoSMonitor about changed operator ${successor}")
      knowledgeActor ! AddOperator((query, successor))
      knowledgeActor ! RemoveOperator((query, self))
    }
  }

  // helper functions for retrying intermediate steps upon failure
  def findSuccessorHost(algorithmName: String, dependencies: Dependencies, placement: Option[Map[Query, Address]])(implicit ec: ExecutionContext): Future[HostInfo] = {
    if (placement.isDefined) {
      val givenPlacement = if(placement.get.contains(query)) {
        val targetMember = cluster.state.members.find(_.address == placement.get(query)).get
        Future(HostInfo(targetMember, query))
      } else {
        transitionLog(s"did not find query among given placement map: \n $query, ${placement.get.mkString("\n")}")
        findSuccessorHost(algorithmName, dependencies, None)
      }
      givenPlacement
    } else {
      val algorithm = PlacementStrategy.getStrategyByName(algorithmName)
      val req = for {
        wasInitialized <- algorithm.initialize()
        successorHost: HostInfo <- {
          transitionLog(s"initialized algorithm $algorithm (was initialized: $wasInitialized), looking for new host...");
          algorithm.findOptimalNode(hostInfo.operator, hostInfo.operator, dependencies, HostInfo(cluster.selfMember, hostInfo.operator, OperatorMetrics()))
        }
      } yield {
        transitionLog(s"found new host ${successorHost.member.address}")
        successorHost
      }
      req.recoverWith { case e: Throwable =>
        transitionLog(s"failed to find successor host, retrying... ${e.getMessage}")
        findSuccessorHost(algorithmName, dependencies, placement)
      }
    }
  }

  def notifyChild(requester: ActorRef, successor: ActorRef, transferredStateMsg: TransferredState, updatedStats: TransitionStats): Future[ACK] = {
    TCEPUtils.guaranteedDelivery(context, requester, transferredStateMsg, tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))
      .recoverWith { case e: Throwable => transitionLog(s"failed to notify child retrying... ${e.toString}".toUpperCase()); notifyChild(requester, successor, transferredStateMsg, updatedStats) }
  }.mapTo[ACK]
}