package tcep.graph.nodes.traits

import akka.actor.{ActorRef, Address, Cancellable, PoisonPill}
import tcep.data.Events.Event
import tcep.data.Queries.Query
import tcep.graph.EventCallback
import tcep.graph.nodes.traits.Node.{UnSubscribe, UpdateTask}
import tcep.graph.transition._
import tcep.machinenodes.helper.actors.{GetEventPause, GetMaxEventInterval, GetTimeSinceLastEvent, _}
import tcep.placement.HostInfo
import tcep.simulation.adaptive.cep.SystemLoad
import tcep.simulation.tcep.GUIConnector
import tcep.utils.TransitionLogActor.{OperatorTransitionBegin, OperatorTransitionEnd}
import tcep.utils.{SizeEstimator, TCEPUtils}

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  *1. stop sending events upon receiving TransitionRequest, store events to be sent and state events (join window contents)
  *2. wait for parent transition to complete and remaining events in mailbox to be emitted (and stored for transfer)
  *3. find successor, transfer stored events and state if necessary (join window contents), but keep them until started
  *4. stop self
  *5. successor is started by clientNode once all operators have transitioned
  */
trait NaiveStopMoveStartMode extends TransitionMode {

  override val modeName = "NaiveStopMoveStartMode"
  val unsentEvents: mutable.Queue[(ActorRef, Event)] = mutable.Queue()
  private var messageQueueClearingTask: Cancellable = _
  private var startOperatorMigrationTask: Cancellable = _
  // largest time between arrivals of two events before the event stream is considered suspended
  private var maxEventIntervalNanos: Option[Long] = None
  private var lastEventArrivalNanos: Option[Long] = None
  private var sampleCount: Int = 0
  private var parentTransitionsComplete: Int = if (this.isInstanceOf[LeafNode]) 1 else 0
  private var downTime: Option[Long] = None

  override def emitEvent(event: Event, eventCallback: Option[EventCallback]): Unit = {
    if (started) super.emitEvent(event, eventCallback)
    // store events that were not sent while waiting to move operator
    else if (transitionInitiated && np.transitionConfig.transitionStrategy == TransitionModeNames.NaiveStopMoveStart)
      unsentEvents += Tuple2(self, event)
  }

  override def preStart(): Unit = {
    super.preStart()
    if (!maxWindowTime().isZero) {
      messageQueueClearingTask = context.system.scheduler.scheduleWithFixedDelay(
        FiniteDuration(maxWindowTime().getSeconds, TimeUnit.SECONDS),
        FiniteDuration(maxWindowTime().getSeconds, TimeUnit.SECONDS))(() => {
        slidingMessageQueue.clear()
      })(ec)
    }
  }

  override def transitionReceive: PartialFunction[Any, Any] = this.stopMoveStartReceive orElse super.transitionReceive

  private def stopMoveStartReceive: PartialFunction[Any, Any] = {

    case t: TransitionRequest =>
      lastEventArrivalNanos = Some(System.nanoTime())
      // immediately stop processing events
      this.started = false
      downTime = Some(System.currentTimeMillis())
      // leaf operator (stream or sequence) unsubscribes from producer -> no new events
      if (this.isInstanceOf[LeafNode])
        getParentActors().foreach(_ ! UnSubscribe())
      super.transitionReceive(t) // forward to transitionReceive (forwards to parent if not a leaf, otherwise starts operator transition)

    case TransferEvents(_, _, subs, window, unsent, algorithm) =>
      sender() ! ACK()
      transitionLog(s"received TransferEvents with ${window.size + unsent.size} events from predecessor $sender")
      if (!this.started) {
        log.debug(s"window data (items:${window.size}): ${window}, unsent: ${unsent.size} \n subs : ${subs}")
        this.subscribers ++= subs
        // store events until SuccessorStart is received from ClientNode (once entire graph transition is complete)
        unsentEvents ++= unsent
        slidingMessageQueue ++= window
        subscribeToParents()
        self ! UpdateTask(algorithm)
      }

    case t: TransferredState =>
      parentTransitionsComplete += 1
      super.transitionReceive(t) // forward to transitionReceive

    // wait for explicit start message from ClientNode signaling that all operators have transited, then re-send transferred events
    case SuccessorStart =>
      transitionLog(s"received SuccessorStart, sending ${unsentEvents.size} unsent events to subscribers and inserting ${slidingMessageQueue.size} to self")
      this.started = true
      unsentEvents.foreach(e => subscribers.keys.foreach(_ ! e._2))
      slidingMessageQueue.foreach(e => self.!(e._2)(e._1))
      unsentEvents.clear()
      slidingMessageQueue.clear()
      // forward to parents
      getParentActors().foreach(_ ! SuccessorStart)

    case e: Event if sampleCount < 10 || transitionInitiated =>
      // reset timer whenever an event arrives after the transition request has arrived
      // limit this to then first 10 events and events during transition to make overhead smaller during normal processing
      val now = System.nanoTime()
      val timeSinceLast = now - lastEventArrivalNanos.getOrElse(now - 1L)
      maxEventIntervalNanos = Some(math.min(maxEventIntervalNanos.getOrElse(-1L), timeSinceLast))
      lastEventArrivalNanos = Some(now)
      sampleCount += 1
      //transitionLog(s"received event $e number $sampleCount ${timeSinceLast / 1e6} vs max: ${maxEventIntervalNanos.getOrElse(-1L) / 1e6}")
      if (!maxWindowTime().isZero)
        slidingMessageQueue += Tuple2(sender(), e)
      e // forward to childNodeReceive

    // store events if we have a window (e.g. join) or a transitionRequest has been received
    case e: Event if (!maxWindowTime().isZero) =>
      slidingMessageQueue += Tuple2(sender(), e)
      e // forward to childNodeReceive

    case MoveOperator(requester, algorithm, stats, placement) =>
      if (startOperatorMigrationTask != null) startOperatorMigrationTask.cancel()
      val now = System.nanoTime()
      val remainingEventsProcessed = now - lastEventArrivalNanos.getOrElse(now) >= 2 * maxEventIntervalNanos.getOrElse(Long.MaxValue)
      transitionLog(s"received MoveOperator from ${sender()}, parent transitions complete: $parentTransitionsComplete, " +
        s"remainingEventsProcessed: $remainingEventsProcessed; maxEventInterval*2: ${2 * maxEventIntervalNanos.getOrElse(-1L) / 1e6}ms ${maxEventIntervalNanos.isDefined}; " +
        s"time since last event: ${(now - lastEventArrivalNanos.getOrElse(now)) / 1e6}ms ${lastEventArrivalNanos.isDefined}")
      // execute transition only if no more events arrive (i.e. remaining events in mailbox have been processed) and parents have transited
      if (sender() == self && parentTransitionsComplete >= getParentActors().size && remainingEventsProcessed || maxEventIntervalNanos.isEmpty)
        moveOperatorNSMS(requester, algorithm, stats, placement)
      else
        executeTransition(requester, algorithm, stats, placement)

    case GetTimeSinceLastEvent => sender() ! System.nanoTime() - lastEventArrivalNanos.getOrElse(System.nanoTime())
    case GetMaxEventInterval => sender() ! maxEventIntervalNanos.getOrElse(Long.MaxValue)
    case GetEventPause => sender() ! System.nanoTime() - lastEventArrivalNanos.getOrElse(System.nanoTime()) > maxEventIntervalNanos.getOrElse(0L)
  }

  /**
    * wait until all remaining events in the mailbox have been processed
    * since we cannot directly check akka mailbox size, we wait a little longer than the usual time between two events
    * and check if a new event has arrived in that interval. Only start the actual transition if no new events have arrived,
    * otherwise check again after that interval
    *
    */
  override def executeTransition(requester: ActorRef, algorithm: String, stats: TransitionStats, placement: Option[Map[Query, Address]]): Unit = {
    transitionLog(s"scheduling operator transition check in ${maxEventIntervalNanos.getOrElse(-1L) / 1e6} ms")
    startOperatorMigrationTask = context.system.scheduler.scheduleOnce(
      new FiniteDuration(maxEventIntervalNanos.getOrElse(1e9.toLong), TimeUnit.NANOSECONDS), self, MoveOperator(requester, algorithm, stats, placement)
    )
  }

  def moveOperatorNSMS(requester: ActorRef, algorithm: String, stats: TransitionStats, placement: Option[Map[Query, Address]]): Unit = {
    try {
      transitionLog(s"executing NaiveStopMoveStartMode ${this.isInstanceOf[NaiveStopMoveStartMode]} transition to ${algorithm} requested by ${requester}")
      transitionLogPublisher ! OperatorTransitionBegin(self)
      val startTime = System.currentTimeMillis()

      @volatile var eventsTransferred = false
      implicit val timeout = retryTimeout
      implicit val ec: ExecutionContext = context.system.dispatchers.lookup("transition-dispatcher") // dedicated dispatcher for transition, avoid transition getting stuck
      val parents = getParentActors()
      val child = getChildOperators()
      val dependencies = getDependencies()

      // helper functions for retrying intermediate steps upon failure
      def transferEvents(successor: ActorRef, newHostInfo: HostInfo): Future[TransitionStats] = {
        if (eventsTransferred) return Future {
          stats
        } // avoid re-sending loop
        // include events from window if windowed so  new operator doesn't lose any events
        val windowEvents = if (!maxWindowTime().isZero) slidingMessageQueue.toList else List()
        transitionLog(s"sending TransferEvents (${windowEvents.size} window state events and ${unsentEvents.size} unsent events) to $successor with timeout $retryTimeout")
        // do NOT send subscriber set unless the subscriber is the clientNode (we do not want to receive new events on old operators); instead, let successor of child subscribe by itself
        val subs = if (np.isRootOperator) subscribers.toList else List()
        val transferEventsMessage = TransferEvents(downTime.getOrElse(System.currentTimeMillis()), startTime, subs, windowEvents, unsentEvents.toList, algorithm)
        val transferACK = TCEPUtils.guaranteedDelivery(context, successor, transferEventsMessage, tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))
        transferACK.map(ack => {
          eventsTransferred = true
          val timestamp = System.currentTimeMillis()
          val migrationTime = timestamp - downTime.get
          val nodeSelectionTime = timestamp - startTime
          GUIConnector.sendOperatorTransitionUpdate(self, successor, algorithm, timestamp, migrationTime, nodeSelectionTime, parents, newHostInfo, np.isRootOperator)(cluster.selfAddress, blockingIoDispatcher)
          // notify mapek knowledge about operator change
          notifyMAPEK(cluster, successor)
          val placementOverhead = newHostInfo.operatorMetrics.accPlacementMsgOverhead
          // remote operator creation, state transfer and execution start, subscription management
          val startMsgSize = SizeEstimator.estimate(transferEventsMessage)
          val transitionOverhead = remoteOperatorCreationOverhead(successor) + startMsgSize + ackSize + subUnsubOverhead(successor, parents.toList)
          transitionLog(s"received ACK for TransferEvents (msg size: $startMsgSize, events in windowData: ${windowEvents.size}) from successor ${System.currentTimeMillis() - downTime.get}ms after stopping events (total time: $migrationTime ms), handing control over to requester and shutting down self")
          TransitionStats(stats.placementOverheadBytes + placementOverhead, stats.transitionOverheadBytes + transitionOverhead, stats.transitionStartAtKnowledge, stats.transitionTimesPerOperator)
        }).mapTo[TransitionStats]
          .recoverWith { case e: Throwable =>
            transitionLog(s"failed to transfer events to successor actor (transferred: $eventsTransferred, retrying... ${e.toString}".toUpperCase())
            transferEvents(successor, newHostInfo)
          }
      }

      transitionLog(s"NSMS transition: looking for new host of ${self} dependencies: $dependencies")
      // retry indefinitely on failure of intermediate steps without repeating the previous ones (-> no duplicate operators)
      val transitToSuccessor = for {
        host: HostInfo <- findSuccessorHost(algorithm, dependencies, placement)
        successor: ActorRef <- {
          transitionLog(s"creating duplicate operator on ${host.member.address}...")
          createDuplicateNode(host)
        }
        updatedStats <- {
          transitionLog(s"created successor duplicate ${successor} on new host after ${System.currentTimeMillis() - startTime}ms");
          transferEvents(successor, host)
        }
        _ <- notifyChild(requester, successor, TransferredState(algorithm, successor, self, updatedStats, query, placement), updatedStats)
      } yield {
        SystemLoad.operatorRemoved()
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
