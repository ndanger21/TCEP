package tcep.graph.nodes.traits

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Cancellable, PoisonPill}
import tcep.data.Events.Event
import tcep.graph.nodes.traits.Node.{UnSubscribe, UpdateTask}
import tcep.graph.transition._
import tcep.machinenodes.helper.actors._
import tcep.placement.{HostInfo, PlacementStrategy}
import tcep.simulation.adaptive.cep.SystemLoad
import tcep.simulation.tcep.GUIConnector
import tcep.utils.TransitionLogActor.{OperatorTransitionBegin, OperatorTransitionEnd}
import tcep.utils.{SizeEstimator, TCEPUtils}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * implementation of strategy described in
  * "Dynamic plan migration for continuous queries over data streams" by Zhu et al.
  * http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.530.6975
  *
  * state matching + state recomputing not necessary since we make no operator graph optimizations/changes,
  * i.e. the state/tuple schema between operators remains unchanged (the paper sees operator migration as a consequence of
  * query optimizations, not operator placement algorithm change)
  */
trait NaiveMovingStateMode extends TransitionMode {

  override val modeName = "NaiveMovingStateMode"
  private var startOperatorMigrationTask: Cancellable = _
  // largest time between arrivals of two events before the event stream is considered suspended
  private var maxEventIntervalNanos: Option[Long] = None
  private var lastEventArrivalNanos: Option[Long] = None
  private var sampleCount: Int = 0
  private var parentTransitionsComplete: Int = if (this.isInstanceOf[LeafNode]) 1 else 0
  override val slidingMessageQueue: ListBuffer[(ActorRef, Event)] = ListBuffer()

  def scheduleMessageQueueClearing(): Unit = {
    if (!maxWindowTime().isZero) {
      context.system.scheduler.scheduleWithFixedDelay(
        FiniteDuration(maxWindowTime().getSeconds, TimeUnit.SECONDS),
        FiniteDuration(maxWindowTime().getSeconds, TimeUnit.SECONDS))(() => { slidingMessageQueue.clear() })(ec)
    }
  }

  // check window time to avoid storing all incoming events without ever clearing them -> memory problem
  def storeEvent(event: Event, s: ActorRef): Unit = if(!maxWindowTime().isZero) slidingMessageQueue += Tuple2(s, event)

  override def preStart(): Unit = {
    super.preStart()
    scheduleMessageQueueClearing()
  }

  override def transitionReceive: PartialFunction[Any, Any] = this.movingStateReceive orElse super.transitionReceive

  private def movingStateReceive: PartialFunction[Any, Any] = {

    case t: TransitionRequest =>
      lastEventArrivalNanos = Some(System.nanoTime())
      // leaf operator (stream or sequence) unsubscribes from producer -> no new events
      if(this.isInstanceOf[LeafNode])
        getParentActors().foreach(_ ! UnSubscribe())
      super.transitionReceive(t) // forward to transitionReceive (forwards to parent if not a leaf, otherwise starts operator transition)

    case StartExecutionWithData(downtime, t, subs, data, algorithm) =>
      sender() ! ACK()
      transitionLog(s"received StartExecutionWithData with ${data.size} events from predecessor $sender")
      if (!this.started) {
        log.debug(s"data (items:${data.size}): ${data} \n subs : ${subs}")
        this.started = true
        this.subscribers ++= subs
        data.foreach(m => self.!(m._2)(m._1))
        //insertWindowStateEvents(data)
        subscribeToParents()
        self ! UpdateTask(algorithm)
      }

    case t: TransferredState =>
      parentTransitionsComplete += 1
      super.transitionReceive(t) // forward to transitionReceive

    case e: Event if sampleCount < 10 || transitionInitiated =>
      // reset timer whenever an event arrives after the transition request has arrived
      // limit this to then first 10 events and events during transition to make overhead smaller during normal processing
      val now = System.nanoTime()
      val timeSinceLast = now - lastEventArrivalNanos.getOrElse(now - 1L)
      maxEventIntervalNanos = Some(math.max(maxEventIntervalNanos.getOrElse(-1L), timeSinceLast))
      lastEventArrivalNanos = Some(now)
      sampleCount += 1
      //transitionLog(s"received event $e number $sampleCount ${timeSinceLast / 1e6} vs max: ${maxEventIntervalNanos.getOrElse(-1L) / 1e6}")
      if(!maxWindowTime().isZero)
        storeEvent(e, sender())
      e // forward to childNodeReceive

    case e: Event if(!maxWindowTime().isZero) =>
      storeEvent(e, sender())
      e

    case MoveOperator(requester, algorithm, stats) =>
      if(startOperatorMigrationTask != null) startOperatorMigrationTask.cancel()
      val now = System.nanoTime()
      val remainingEventsProcessed = now - lastEventArrivalNanos.getOrElse(now) >= 2 * maxEventIntervalNanos.getOrElse(Long.MaxValue)
      transitionLog(s"received MoveOperator from ${sender()}, parent transitions complete: $parentTransitionsComplete, " +
        s"remainingEventsProcessed: $remainingEventsProcessed; maxEventInterval*1.5: ${1.5*maxEventIntervalNanos.getOrElse(-1L) / 1e6}ms ${maxEventIntervalNanos.isDefined}; " +
        s"time since last event: ${(now - lastEventArrivalNanos.getOrElse(now)) / 1e6}ms ${lastEventArrivalNanos.isDefined}")
      // execute transition only if no more events arrive (i.e. remaining events coming from parents have been processed) and parents have transited
      if(sender() == self && parentTransitionsComplete >= getParentActors().size && remainingEventsProcessed || maxEventIntervalNanos.isEmpty)
        moveOperatorNMS(requester, algorithm, stats)
      else
        executeTransition(requester, algorithm, stats)

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
  override def executeTransition(requester: ActorRef, algorithm: PlacementStrategy, stats: TransitionStats): Unit = {
    transitionLog(s"scheduling operator transition in ${maxEventIntervalNanos.getOrElse(-1L) / 1e6} ms")
    startOperatorMigrationTask = context.system.scheduler.scheduleOnce(
      new FiniteDuration(maxEventIntervalNanos.getOrElse(1e9.toLong), TimeUnit.NANOSECONDS), self, MoveOperator(requester, algorithm, stats)
    )
  }

  // 1. stop events, wait until remaining events have been processed (no new events arrive for 1.5*maxEventInterval)
  // 2. find successor
  // 3. start successor, send state if necessary (join window contents)
  // 4. stop self
  def moveOperatorNMS(requester: ActorRef, algorithm: PlacementStrategy, stats: TransitionStats): Unit = {
   try {
     if(startOperatorMigrationTask != null) startOperatorMigrationTask.cancel()
     transitionLog(s"executing NaiveMovingStateMode transition to ${algorithm.name} requested by ${requester}")
     transitionLogPublisher ! OperatorTransitionBegin(self)
     val startTime = System.currentTimeMillis()
     // immediately stop processing events
     this.started = false
     val downTime: Option[Long] = Some(System.currentTimeMillis())
     @volatile var succStarted = false
     implicit val timeout = retryTimeout
     implicit val ec: ExecutionContext = blockingIoDispatcher

     val parents = getParentActors()
     val dependencies = getDependencies()

     // helper functions for retrying intermediate steps upon failure
     def startSuccessorActor(successor: ActorRef, newHostInfo: HostInfo): Future[TransitionStats] = {
       if (succStarted)  return Future { stats } // avoid re-sending loop
       log.info(s"sending StartExecutionWithWindowData to $successor with timeout $retryTimeout")
       // include events from window if windowed so  new operator doesn't lose any events
       val windowEvents = if(!maxWindowTime().isZero) slidingMessageQueue.toList else List()
       val subs = if(isRootOperator) subscribers.toList else List()
       // do NOT send subscriber set unless the subscriber is the clientNode (we do not want to receive new events on old operators); instead, let successor of child subscribe by itself
       val startExecutionMessage = StartExecutionWithData(downTime.get, startTime, subs, windowEvents, algorithm.name)
       val startACK = TCEPUtils.guaranteedDelivery(context, successor, startExecutionMessage, tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))
       startACK.map(ack => {
         succStarted = true
         val timestamp = System.currentTimeMillis()
         val migrationTime = timestamp - downTime.get
         val nodeSelectionTime = timestamp - startTime
         GUIConnector.sendOperatorTransitionUpdate(self, successor, algorithm.name, timestamp, migrationTime, nodeSelectionTime, parents, newHostInfo, isRootOperator)(cluster.selfAddress, blockingIoDispatcher)
         // notify mapek knowledge about operator change
         notifyMAPEK(cluster, successor)
         val placementOverhead = newHostInfo.operatorMetrics.accPlacementMsgOverhead
         // remote operator creation, state transfer and execution start, subscription management
         val startMsgSize = SizeEstimator.estimate(startExecutionMessage)
         val transitionOverhead = remoteOperatorCreationOverhead(successor) + startMsgSize + ackSize + subUnsubOverhead(successor, parents.toList)
         transitionLog(s"received ACK for StartExecutionWithWindowData (msg size: $startMsgSize, events in windowData: ${windowEvents.size}) from successor ${System.currentTimeMillis() - downTime.get}ms after stopping events (total time: $migrationTime ms), handing control over to requester and shutting down self")
         TransitionStats(stats.placementOverheadBytes + placementOverhead, stats.transitionOverheadBytes + transitionOverhead, stats.transitionStartAtKnowledge, stats.transitionTimesPerOperator)
       }).mapTo[TransitionStats]
         .recoverWith { case e: Throwable =>
           transitionLog(s"failed to start successor actor (started: $succStarted, retrying... ${e.toString}".toUpperCase())
           startSuccessorActor(successor, newHostInfo)
         }
     }


     transitionLog(s"NMS transition: looking for new host of ${self} dependencies: $dependencies")
     // retry indefinitely on failure of intermediate steps without repeating the previous ones (-> no duplicate operators)
     val transitToSuccessor = for {
       host: HostInfo <- findSuccessorHost(algorithm, dependencies)
       successor: ActorRef <- {
         transitionLog(s"creating duplicate operator on ${host.member.address}...")
         createDuplicateNode(host) }
       updatedStats <- {
         transitionLog(s"created successor duplicate ${successor} on new host after ${System.currentTimeMillis() - startTime}ms");
         startSuccessorActor(successor, host) }
       _ <- notifyChild(requester, successor, TransferredState(algorithm, successor, self, updatedStats, query), updatedStats)
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
