package tcep.graph.nodes.traits

import akka.actor.{ActorRef, Address, PoisonPill}
import tcep.data.Events.Event
import tcep.data.Queries.Query
import tcep.graph.nodes.traits.Node.{UnSubscribe, UpdateTask}
import tcep.graph.transition._
import tcep.machinenodes.helper.actors.ACK
import tcep.placement.HostInfo
import tcep.simulation.adaptive.cep.SystemLoad
import tcep.simulation.tcep.GUIConnector
import tcep.utils.TransitionLogActor.{OperatorTransitionBegin, OperatorTransitionEnd}
import tcep.utils.{SizeEstimator, TCEPUtils}

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Handling Cases of MFGS Transition
  **/
trait MFGSMode extends TransitionMode {
  override val modeName = "MFGS mode"

  override def preStart(): Unit = {
    super.preStart()
    messageQueueManager()
  }
  override val slidingMessageQueue: ListBuffer[(ActorRef, Event)] = ListBuffer()


  def messageQueueManager() = {
    if (!maxWindowTime().isZero) {
      context.system.scheduler.schedule(
        FiniteDuration(maxWindowTime().getSeconds, TimeUnit.SECONDS),
        FiniteDuration(maxWindowTime().getSeconds, TimeUnit.SECONDS), () => {
          slidingMessageQueue.clear()
        }
      )
    }
  }

  override def transitionReceive = this.mfgsReceive orElse super.transitionReceive

  private def mfgsReceive: PartialFunction[Any, Any] = {


    case StartExecutionWithData(downtime, t, subs, data, algorithm) => {
      sender() ! ACK()
      transitionLog(s"received StartExecutionWithData from predecessor $sender")
      if(!this.started) {
        log.debug("data (items: ${data.size}): ${data} \n subs : ${subs}")
        transitionLog(s"received StartExecutionWithData (downtime: ${System.currentTimeMillis() - downtime}ms) from old node ${sender()}")
        this.started = true
        this.subscribers ++= subs
        data.foreach(m => self.!(m._2)(m._1))
        subscribeToParents()
        self ! UpdateTask(algorithm)
        //Unit //Nothing to forward for childReceive
      }
    }

    case message: Event if(!maxWindowTime().isZero) =>
      slidingMessageQueue += Tuple2(sender(), message) // check window time to avoid storing all incoming events without ever clearing them -> memory problem
      message //caching message and forwarding to the childReceive
  }

  override def executeTransition(requester: ActorRef, algorithm: String, stats: TransitionStats, placement: Option[Map[Query, Address]]): Unit = {

    try {
      transitionLog(s"executing MFGS ${this.isInstanceOf[MFGSMode]} transition to ${algorithm} requested by ${requester}")
      transitionLogPublisher ! OperatorTransitionBegin(self)
      val startTime = System.currentTimeMillis()
      var downTime: Option[Long] = None
      @volatile var succStarted = false
      implicit val timeout = retryTimeout
      //implicit val ec: ExecutionContext = blockingIoDispatcher
      implicit val ec: ExecutionContext = context.system.dispatchers.lookup("transition-dispatcher") // dedicated dispatcher for transition, avoid transition getting stuck
      val dependencies = getDependencies()

      // helper functions for retrying intermediate steps upon failure
      def startSuccessorActor(successor: ActorRef, newHostInfo: HostInfo): Future[TransitionStats] = {
        if(succStarted) return Future { stats } // avoid re-sending loop
        this.started = false
        if(downTime.isEmpty) downTime = Some(System.currentTimeMillis())
        // resend incoming messages to self on new operator if windowed so we don't lose any events
        val msgQueue = if (!this.maxWindowTime().isZero) slidingMessageQueue.toList else List[(ActorRef, Event)]()
        log.info(s"sending StartExecutionWithData to $successor with timeout $retryTimeout")
        val startExecutionMessage = StartExecutionWithData(downTime.get, startTime, subscribers.toList, msgQueue, algorithm)
        val msgACK = TCEPUtils.guaranteedDelivery(context, successor, startExecutionMessage, tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))
        msgACK.map(ack => {
          succStarted = true
          val timestamp = System.currentTimeMillis()
          val migrationTime = timestamp - downTime.get
          val nodeSelectionTime = timestamp - startTime
          //log.info(s"\nBDP DEBUG========\nself: $self \n successor: $successor\n parents: \n${getParentActors().mkString("\n")} \n dependencies parents: \n ${dependencies.parents.mkString("\n")} \n newHostInfo: \n${newHostInfo.operatorMetrics.operatorToParentBDP.mkString("\n")} \n============")
          GUIConnector.sendOperatorTransitionUpdate(self, successor, algorithm, timestamp, migrationTime, nodeSelectionTime, getParentActors(), newHostInfo, np.isRootOperator)(cluster.selfAddress, blockingIoDispatcher)
          notifyMAPEK(cluster, successor) // notify mapek knowledge about operator change
          transitionLog(s"received ACK for StartExecutionWithData from successor ${System.currentTimeMillis() - downTime.get}ms after stopping events (total time: $migrationTime ms), handing control over to requester and shutting down self")
          val placementOverhead = newHostInfo.operatorMetrics.accPlacementMsgOverhead
          // remote operator creation, state transfer and execution start, subscription management
          val transitionOverhead = remoteOperatorCreationOverhead(successor) + SizeEstimator.estimate(startExecutionMessage) + ackSize + subUnsubOverhead(successor, getParentActors())
          TransitionStats(stats.placementOverheadBytes + placementOverhead, stats.transitionOverheadBytes + transitionOverhead, stats.transitionStartAtKnowledge, stats.transitionTimesPerOperator)
        }).mapTo[TransitionStats].recoverWith { case e: Throwable => transitionLog(s"failed to start successor actor (started: $succStarted, retrying... ${e.toString}".toUpperCase()); startSuccessorActor(successor, newHostInfo) }
      }

      transitionLog( s"MFGS transition: looking for new host of ${self} dependencies: $dependencies")
      // retry indefinitely on failure of intermediate steps without repeating the previous ones (-> no duplicate operators)
      val transitToSuccessor = for {
        host: HostInfo <- findSuccessorHost(algorithm, dependencies, placement)
        successor: ActorRef <- { val s = createDuplicateNode(host); transitionLog(s"created successor duplicate ${s} on new host after ${System.currentTimeMillis() - startTime}ms, stopping events and sending StartExecutionWithData"); s }
        updatedStats <- startSuccessorActor(successor, host)
        childNotified <- notifyChild(requester, successor,  TransferredState(algorithm, successor, self, updatedStats, query, placement), updatedStats)
      } yield {
        SystemLoad.operatorRemoved()
        getParentActors().foreach(_ ! UnSubscribe())
        transitionLogPublisher ! OperatorTransitionEnd(self)
        sendTransitionStats(self, System.currentTimeMillis() - startTime, updatedStats.placementOverheadBytes, updatedStats.transitionOverheadBytes)
        context.system.scheduler.scheduleOnce(FiniteDuration(10, TimeUnit.SECONDS), self, PoisonPill) // delay self shutdown so that transferredState msg etc can be safely transmitted
      }

      transitToSuccessor.onComplete {
        case Success(_) =>
          transitionLog(s"successfully started successor operator and handed control back to requesting child ${requester.path}")
          log.info(s"transition of $this took ${System.currentTimeMillis() - startTime}ms")
        case Failure(exception) =>
          log.error(exception, s"failed to find new host for $this, see placement and transition logs")
          transitionLog(s"failed to transit to successor due to $exception")
      }

    } catch {
      case e: Throwable =>
        transitionLog(s"failed to find new host due to ${e.toString}")
        log.error(e, s"caught exception while executing transition for $this")
    }
  }

}