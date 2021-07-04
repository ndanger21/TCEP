package tcep.publishers

import tcep.data.Events._
import tcep.prediction.PredictionHelper.Throughput
import tcep.utils.SizeEstimator

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

case class RandomPublisher(createEventFromId: Integer => Event) extends Publisher {

  val minimumWait = 2000
  val scheduler =  context.system.scheduler.schedule(
                            FiniteDuration(minimumWait + Random.nextInt(5000), TimeUnit.MILLISECONDS),
                            FiniteDuration(minimumWait + Random.nextInt(5000), TimeUnit.MILLISECONDS),
                            runnable = () => {
                              val event: Event = createEventFromId(id.incrementAndGet())
                              subscribers.keys.foreach(_ ! event)
                              log.info(s"STREAM $publisherName:\t$event")
                            }
                          )

  override def postStop(): Unit = {
    scheduler.cancel()
    super.postStop()
  }

  var eventRateOut: Throughput = Throughput(1000.0 / minimumWait, FiniteDuration(1, TimeUnit.SECONDS))
  val eventSizeOut: Long = SizeEstimator.estimate(createEventFromId(0))
}
