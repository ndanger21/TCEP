package tcep.publishers

import akka.actor.Timers
import tcep.data.Events._
import tcep.prediction.PredictionHelper.Throughput
import tcep.publishers.Publisher.StartStreams
import tcep.utils.SizeEstimator

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.language.postfixOps

object RegularPublisher {
  case object SendEventTickKey
  case object SendEventTick
  case object LogEventsSentTickKey
  case object LogEventsSentTick
  case object GetEventsPerSecond
}
/**
  * Publishes the events at regular interval
  *
  * @param waitTime the interval for publishing the events in MICROSECONDS
  * @param createEventFromId function to convert Id to an event
  */
case class RegularPublisher(waitTime: Long, createEventFromId: Integer => Event) extends Publisher with Timers {
  import RegularPublisher._

  val eventSizeOut: Long = SizeEstimator.estimate(createEventFromId(0))
  var eventRateOut: Throughput = Throughput(1000000 / waitTime, FiniteDuration(1, TimeUnit.SECONDS)) // events/s
  var eventDelays: List[Long] = 1 to 5 map(waitTime / _) toList
  val eventRateChangeDelay: FiniteDuration = FiniteDuration(1, TimeUnit.HOURS)

  override def preStart() = {
    log.info(s"starting regular publisher with interval $waitTime microseconds (${1e6 / waitTime }/s) and roles ${cluster.getSelfRoles}; event rate increases to ${eventDelays.map(1e6 / _)} each hour")
    timers.startTimerWithFixedDelay(ChangeEventRateTickKey, ChangeEventRateTick, eventRateChangeDelay)
    super.preStart()
  }
  override def postStop(): Unit = {
    if(emitEventTask != null)
      emitEventTask.cancel(true)
    super.postStop()
  }

  override def receive: Receive = {
    super.receive orElse {
      case ChangeEventRateTick =>
        emitEventTask.cancel(true)
        eventDelays = eventDelays.tail
        if(eventDelays.nonEmpty) {
          emitEventTask = sched.scheduleAtFixedRate(() => self ! SendEventTick, 0, eventDelays.head, TimeUnit.MICROSECONDS)
          log.info("increased event rate to {}", 1e6 / eventDelays.head)
        } else log.info("all event rates processed, stopping events")


      case StartStreams() =>
        if (!startedPublishing) {
          log.info("starting to stream events!")
          startedPublishing = true
          timers.startTimerAtFixedRate(LogEventsSentTickKey, LogEventsSentTick, samplingInterval)
          emitEventTask = sched.scheduleAtFixedRate(() => self ! SendEventTick, 10, waitTime, TimeUnit.MICROSECONDS)
          //timers.startTimerAtFixedRate(SendEventTickKey, SendEventTick, FiniteDuration(waitTime, TimeUnit.MICROSECONDS)) // this sends way too slow
        }

      case SendEventTick =>
        val event: Event = createEventFromId(id.incrementAndGet())
        event.updateDepartureTimestamp(eventSizeOut, eventRateOut)
        subscribers.keys.foreach(_ ! event)
    }
  }

  private case object ChangeEventRateTick
  private case object ChangeEventRateTickKey
}
