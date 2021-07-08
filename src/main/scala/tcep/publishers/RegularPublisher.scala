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
case class RegularPublisher(eventRate: Throughput, createEventFromId: Integer => Event) extends Publisher with Timers {
  import RegularPublisher._

  val eventSizeOut: Long = SizeEstimator.estimate(createEventFromId(0))
  var eventRateOut: Throughput = eventRate // events/s
  var nextEventRates: List[Throughput] = 1 to 3 map(i => Throughput(eventRate.getEventsPerSec + i * 20, FiniteDuration(1, TimeUnit.SECONDS))) toList
  val eventRateChangeDelay: FiniteDuration = FiniteDuration(150, TimeUnit.MINUTES)

  override def preStart() = {
    log.info(s"starting regular publisher with publishing rate $eventRate and roles ${cluster.getSelfRoles}; event rate increases to ${nextEventRates} each hour")
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
        if(nextEventRates.nonEmpty) {
          emitEventTask = sched.scheduleAtFixedRate(() => self ! SendEventTick, 0, nextEventRates.head.getDelayBetweenEventsMicros, TimeUnit.MICROSECONDS)
          log.info("increased event rate to {}", nextEventRates.head)
	  nextEventRates = nextEventRates.tail
        } else {
	  log.info("all event rates processed, stopping events")
	}

      case StartStreams() =>
        if (!startedPublishing) {
          log.info("starting to stream events!")
          startedPublishing = true
          timers.startTimerAtFixedRate(LogEventsSentTickKey, LogEventsSentTick, samplingInterval)
          emitEventTask = sched.scheduleAtFixedRate(() => self ! SendEventTick, 10, eventRate.getDelayBetweenEventsMicros, TimeUnit.MICROSECONDS)
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
