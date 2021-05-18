package tcep.graph.qos

import akka.actor.{Actor, ActorLogging, ActorRef, Timers}
import breeze.stats._
import breeze.stats.meanAndVariance.MeanAndVariance
import com.typesafe.config.ConfigFactory
import tcep.data.Events.Event
import tcep.graph.qos.OperatorQosMonitor._
import tcep.machinenodes.qos.BrokerQoSMonitor.BandwidthUnit.BytePerSec
import tcep.machinenodes.qos.BrokerQoSMonitor._
import tcep.utils.SizeEstimator

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration

/**
  * monitoring actor for each operator
  * keeps track of operator-level metrics:
  * - event size [Bytes]
  * - incoming + outgoing event rate -> selectivity
  * - incoming + outgoing bandwidth
  * - mean + var inter-arrival time
  * - mean + var processing latency
  * - mean + var of network latency to child operator
  */
class OperatorQosMonitor(operator: ActorRef) extends Actor with Timers with ActorLogging {

  val samplingInterval: FiniteDuration = FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.MILLISECONDS)

  var eventSizeIn: ListBuffer[Long] = ListBuffer.empty
  var eventSizeOut: Long = 0
  var eventRateIn: ListBuffer[Double] = ListBuffer.empty
  var eventRateOut: Double = 0.0
  var interArrivalLatency: MeanAndVariance = MeanAndVariance(0.0, 0.0, 0)
  var processingLatency: MeanAndVariance = MeanAndVariance(0.0, 0.0, 0)
  var networkToParentLatency: MeanAndVariance = MeanAndVariance(0.0, 0.0, 0) // binary operators: longest path to a parent
  val eventSamples: ListBuffer[Event] = ListBuffer.empty

  override def preStart(): Unit = {
    super.preStart()
    timers.startTimerWithFixedDelay(SamplingTickKey, SamplingTick, samplingInterval)
  }

  override def receive: Receive = {
    case event: Event =>
      eventSamples += event

    case SamplingTick =>
      eventSizeOut = if(eventSamples.nonEmpty) eventSamples.map(e => SizeEstimator.estimate(e)).sum / eventSamples.size else 0
      // eventSizeIn contains event sizes of 1 or more parent events, 1 list for each event -> split
      // ListBuffer[TupleX[Long]] -> TupleX[List[Long]]
      val parentCount = if(eventSamples.nonEmpty) eventSamples.head.monitoringData.processingStats.eventSizeIn.size else 0
      eventSizeIn.clear()
      eventRateIn.clear()
      for (i <- 0 until parentCount) {
        eventSizeIn += (if(eventSamples.nonEmpty) eventSamples.map(_.monitoringData.processingStats.eventSizeIn(i)).sum / eventSamples.size else 0)
        eventRateIn += (if(eventSamples.nonEmpty) eventSamples.map(_.monitoringData.processingStats.eventRateIn(i)).sum / eventSamples.size else 0)
      }
      eventRateOut = eventSamples.size / samplingInterval.toSeconds
      processingLatency = meanAndVariance(eventSamples.map(_.monitoringData.processingStats.processingLatencyNS.toDouble / 1e6))
      networkToParentLatency = meanAndVariance(eventSamples.map(_.monitoringData.processingStats.lastHopLatency.toDouble))
      val arrivalTimestampsNS = eventSamples.map(_.monitoringData.processingStats.processingStartNS).toVector
      val interArrivals = ListBuffer[Double]()
      for (i <- 0 until arrivalTimestampsNS.size - 1) interArrivals += (arrivalTimestampsNS(i+1) - arrivalTimestampsNS(i)) / 1e6
      interArrivalLatency = meanAndVariance(interArrivals)
      log.info(s"operator SamplingTick: ${eventSamples.size} samples\neventRateIO: $eventRateIn $eventRateOut\neventSizeIO $eventSizeIn $eventSizeOut " +
        s"\nnetworkLatencyParents $networkToParentLatency\nprocessingLatency $processingLatency\ninterarrivalLatency $interArrivalLatency")
      eventSamples.clear()
      operator ! UpdateEventRateOut(eventRateOut)

    case GetIOMetrics =>
      sender() ! IOMetrics(eventRateIn.sum, eventRateOut, bandwidthIn, bandwidthOut)

    case GetOperatorQoSMetrics => sender() ! OperatorQoSMetrics(
      eventSizeOut, selectivity,
      interArrivalLatency, processingLatency, networkToParentLatency,
      IOMetrics(eventRateIn.sum, eventRateOut, bandwidthIn, bandwidthOut)
    )
  }

  def bandwidthIn: Bandwidth = Bandwidth(eventRateIn.zip(eventSizeIn).map(p => p._1 * p._2).sum, BytePerSec)
  def bandwidthOut: Bandwidth = Bandwidth(eventRateOut * eventSizeOut, BytePerSec)
  def selectivity: Double = eventRateOut / eventRateIn.sum

}

object OperatorQosMonitor {
  case object GetOperatorQoSMetrics
  case class OperatorQoSMetrics(eventSize: Long,
                                selectivity: Double,
                                interArrivalLatency: MeanAndVariance,
                                processingLatency: MeanAndVariance,
                                networkToChildLatency: MeanAndVariance,
                                ioMetrics: IOMetrics)
  case class UpdateEventRateOut(rate: Double)
  private case object SamplingTick
  private case object SamplingTickKey

}