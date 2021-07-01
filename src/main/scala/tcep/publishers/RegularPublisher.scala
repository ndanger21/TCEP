package tcep.publishers

import akka.actor.Timers
import com.typesafe.config.ConfigFactory
import tcep.data.Events._
import tcep.prediction.PredictionHelper.Throughput
import tcep.publishers.Publisher.StartStreams
import tcep.utils.SizeEstimator

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}
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
  val publisherName: String = self.path.name
  val id: AtomicInteger = new AtomicInteger(0)
  var startedPublishing = false
  var emitEventTask: ScheduledFuture[_] = _
  var sched = Executors.newSingleThreadScheduledExecutor()
  val eventSizeOut: Long = SizeEstimator.estimate(createEventFromId(0))
  var eventRateOut: Throughput = Throughput(1000000 / waitTime, FiniteDuration(1, TimeUnit.SECONDS)) // events/s
  var lastEventCount = 0
  val samplingInterval: FiniteDuration = FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.MILLISECONDS)

  override def preStart() = {
    log.info(s"starting regular publisher with interval $waitTime microseconds (${1e6 / waitTime }/s) and roles ${cluster.getSelfRoles}")
    super.preStart()
  }
  override def postStop(): Unit = {
    if(emitEventTask != null)
      emitEventTask.cancel(true)
    super.postStop()
  }

  override def receive: Receive = {
    super.receive orElse {
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

      case LogEventsSentTick =>
        eventRateOut = Throughput(id.get() - lastEventCount, samplingInterval)
        lastEventCount = id.get()
        //SpecialStats.log(self.toString(), "eventsSent", s"total: ${id.get()}")

      case GetEventsPerSecond => sender() ! eventRateOut

    }
  }
}
