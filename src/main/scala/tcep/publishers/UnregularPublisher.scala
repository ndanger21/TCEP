package tcep.publishers

import akka.actor.Timers
import tcep.data.Events.Event
import tcep.prediction.PredictionHelper.Throughput
import tcep.publishers.Publisher.StartStreams
import tcep.publishers.RegularPublisher.{LogEventsSentTick, LogEventsSentTickKey}
import tcep.utils.SizeEstimator

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration


case class UnregularPublisher(waitTime: Long, createEventFromId: Integer => (Event, Double), waitTillStart: Option[Long] = None) extends Publisher with Timers {

  val eventSizeOut: Long = SizeEstimator.estimate(createEventFromId(0))
  var eventRateOut: Throughput = Throughput(1 / createEventFromId(0)._2, FiniteDuration(1, TimeUnit.SECONDS)) // events/s

  override def preStart(): Unit = {
    log.info(s"starting unregular publisher with roles ${cluster.getSelfRoles}")
    super.preStart()
  }

  override def postStop(): Unit = {
    emitEventTask.cancel(true)
    super.postStop()
  }

  override def receive: Receive = {
    super.receive orElse {
      case StartStreams() =>
        log.info(s"UNREGULAR PUBLISHER STARTING SCHEDULER, publishing events in $waitTime seconds!")
        emitEventTask = sched.schedule(publishEvent(), waitTime, TimeUnit.SECONDS)
        timers.startTimerAtFixedRate(LogEventsSentTickKey, LogEventsSentTick, samplingInterval)
    }
  }

  def publishEvent(): Runnable = () => {
    val tup: (Event, Double) = createEventFromId(id.incrementAndGet())
    val event = tup._1
    event.updateDepartureTimestamp(eventSizeOut, eventRateOut)
    //log.info(s"Emitting event: $event and waiting for ${waittime.toLong}ms")
    subscribers.keys.foreach(_ ! event)
    emitEventTask = sched.schedule(publishEvent(), (tup._2 * 1000).toLong, TimeUnit.MILLISECONDS)
  }
}
