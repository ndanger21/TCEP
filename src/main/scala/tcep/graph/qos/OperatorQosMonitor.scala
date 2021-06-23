package tcep.graph.qos

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Timers}
import breeze.stats._
import breeze.stats.meanAndVariance.MeanAndVariance
import com.typesafe.config.ConfigFactory
import tcep.data.Events.Event
import tcep.graph.qos.OperatorQosMonitor._
import tcep.machinenodes.qos.BrokerQoSMonitor.BandwidthUnit.{BytePerSec, KBytePerSec}
import tcep.machinenodes.qos.BrokerQoSMonitor._
import tcep.utils.{SizeEstimator, SpecialStats}

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
  * - mean + var of network latency to parent operator (time between sending from parent until event is taken off mailbox at this operator)
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
  var endToEndLatency: MeanAndVariance = MeanAndVariance(0.0, 0.0, 0)
  val eventSamples: ListBuffer[Event] = ListBuffer.empty
  val brokerQoSMonitor: ActorSelection = context.system.actorSelection(context.system./("TaskManager*")./("BrokerQosMonitor*"))
  val logFileString: String = self.toString().split("-").head.split("/").last

  override def preStart(): Unit = {
    super.preStart()
    timers.startTimerWithFixedDelay(SamplingTickKey, SamplingTick, samplingInterval)
    // logfile heading
    SpecialStats.log(logFileString, logFileString,
      //s"timestamp;timeSinceStart;Actor;" +
        s"eventSizeInKB;eventSizeOutKB;operatorSelectivity;eventRateIn;eventRateOut;" +
        s"interArrivalMean;interArrivalStdDev;networkParentLatencyMean;networkParentLatencyStdDev;processingLatencyMean;processingLatencyStdDev;e2eLatencyMean;e2eLatencyStdDev;" +
        s"brokerCPULoad;brokerThreadCount;brokerOperatorCount;brokerOtherBandwidthInKB;brokerOtherBandwidthOutKB")
  }

  override def receive: Receive = {
    case event: Event =>
      eventSamples += event

    case SamplingTick =>
      val start = System.nanoTime()
      eventSizeOut = if(eventSamples.nonEmpty) SizeEstimator.estimate(eventSamples.head) else 0
      val parentCount = if(eventSamples.nonEmpty) eventSamples.head.monitoringData.processingStats.eventSizeIn.size else 0
      eventSizeIn.clear()
      eventRateIn.clear()
      for (i <- 0 until parentCount) {
        eventSizeIn += (if(eventSamples.nonEmpty) eventSamples.map(_.monitoringData.processingStats.eventSizeIn(i)).sum / eventSamples.size else 0)
        eventRateIn += (if(eventSamples.nonEmpty) eventSamples.map(_.monitoringData.processingStats.eventRateIn(i)).sum / eventSamples.size else 0)
      }
      eventRateOut = eventSamples.size / samplingInterval.toSeconds
      // ms
      val processingLatencySamples = eventSamples.map(_.monitoringData.processingStats.processingLatencyNS.toDouble / 1e6)
      val networkLatencySamples = eventSamples.map(_.monitoringData.processingStats.lastHopLatency.toDouble)
      val e2eLatencySamples = processingLatencySamples.zip(networkLatencySamples).map(l => l._1 + l._2)
      processingLatency = meanAndVariance(processingLatencySamples)
      networkToParentLatency = meanAndVariance(networkLatencySamples)
      endToEndLatency = meanAndVariance(e2eLatencySamples)
      val arrivalTimestampsNS = eventSamples.map(_.monitoringData.processingStats.processingStartNS).toVector
      val interArrivals = ListBuffer[Double]()
      for (i <- 0 until arrivalTimestampsNS.size - 1) interArrivals += (arrivalTimestampsNS(i+1) - arrivalTimestampsNS(i)) / 1e6
      interArrivalLatency = meanAndVariance(interArrivals)

      log.info("operator SamplingTick took {}ms: {} samples\neventRateIO: {} {}\neventSizeIO {} {} \nnetworkLatencyParents {}\nprocessingLatency {}\ne2eLatency {} \ninterArrivalLatency {}",
        Array((System.nanoTime() - start) / 1e6, eventSamples.size, eventRateIn, eventRateOut, eventRateIn, eventSizeOut, networkToParentLatency, processingLatency, endToEndLatency, interArrivalLatency))

      eventSamples.clear()
      operator ! UpdateEventRateOut(eventRateOut)
      operator ! UpdateEventSizeOut(eventSizeOut)
      brokerQoSMonitor ! GetBrokerMetrics(Some(Set(operator)))

    case BrokerQosMetrics(cpuLoad, cpuThreadCount, deployedOperators, ioMetrics, timestamp) =>
      // log feature and metric values
      val currentValues = getCurrentMetrics
      SpecialStats.log(logFileString, logFileString,
        s"${currentValues.eventSizeIn.sum.toDouble / 1024};${currentValues.eventSizeOut / 1024};${selectivity};" +
          s"${currentValues.ioMetrics.incomingEventRate};${currentValues.ioMetrics.outgoingEventRate};" +
          s"${currentValues.interArrivalLatency.mean};${currentValues.interArrivalLatency.stdDev};" +
          s"${currentValues.networkToParentLatency.mean};${currentValues.networkToParentLatency.stdDev};" +
          s"${currentValues.processingLatency.mean};${currentValues.processingLatency.stdDev};" +
          s"${currentValues.endToEndLatency.mean}; ${currentValues.endToEndLatency.stdDev};" +
          s"$cpuLoad;$cpuThreadCount;$deployedOperators;" +
          s"${ioMetrics.incomingBandwidth.toUnit(KBytePerSec).amount};${ioMetrics.outgoingBandwidth.toUnit(KBytePerSec).amount}")


    case GetIOMetrics =>
      sender() ! IOMetrics(eventRateIn.sum, eventRateOut, bandwidthIn, bandwidthOut)

    case GetOperatorQoSMetrics => sender() ! getCurrentMetrics
  }

  def bandwidthIn: Bandwidth = Bandwidth(eventRateIn.zip(eventSizeIn).map(p => p._1 * p._2).sum, BytePerSec)
  def bandwidthOut: Bandwidth = Bandwidth(eventRateOut * eventSizeOut, BytePerSec)
  def selectivity: Double = eventRateOut / eventRateIn.sum
  def getCurrentMetrics: OperatorQoSMetrics = OperatorQoSMetrics(
    eventSizeIn.toList, eventSizeOut, selectivity,
    interArrivalLatency, processingLatency, networkToParentLatency, endToEndLatency,
    IOMetrics(eventRateIn.sum, eventRateOut, bandwidthIn, bandwidthOut)
  )

}

object OperatorQosMonitor {
  case object GetSamples
  type Sample = (OperatorQoSMetrics, BrokerQosMetrics)
  def getFeatureValue(sample: Sample, feature: String): AnyVal = {
    if(ALL_FEATURES.contains(feature)) {
      feature match {
        case EVENTSIZE_IN_KB => sample._1.eventSizeIn.sum
        case EVENTSIZE_OUT_KB => sample._1.eventSizeOut
        case OPERATOR_SELECTIVITY => sample._1.selectivity
        case EVENTRATE_IN => sample._1.ioMetrics.incomingEventRate
        case INTER_ARRIVAL_MEAN_MS => sample._1.interArrivalLatency.mean
        case INTER_ARRIVAL_STD_MS => sample._1.interArrivalLatency.stdDev
        case PARENT_NETWORK_LATENCY_MEAN_MS => sample._1.networkToParentLatency.mean
        case PARENT_NETWORK_LATENCY_STD_MS => sample._1.networkToParentLatency.stdDev
        case PROCESSING_LATENCY_MEAN_MS => sample._1.processingLatency.mean
        case PROCESSING_LATENCY_STD_MS => sample._1.processingLatency.stdDev
        case BROKER_CPU_LOAD => sample._2.cpuLoad
        case BROKER_THREAD_COUNT => sample._2.cpuThreadCount
        case BROKER_OPERATOR_COUNT => sample._2.deployedOperators
        case BROKER_OTHER_BANDWIDTH_IN_KB => sample._2.IOMetrics.incomingBandwidth.toUnit(KBytePerSec).amount
        case BROKER_OTHER_BANDWIDTH_OUT_KB => sample._2.IOMetrics.outgoingBandwidth.toUnit(KBytePerSec).amount
      }
    } else throw new IllegalArgumentException(s"can't retrieve feature value $feature, must be one of ${ALL_FEATURES}")
  }
  type Samples = List[Sample]
  case object GetOperatorQoSMetrics
  case class OperatorQoSMetrics(eventSizeIn: List[Long],
                                eventSizeOut: Long,
                                selectivity: Double,
                                interArrivalLatency: MeanAndVariance,
                                processingLatency: MeanAndVariance,
                                networkToParentLatency: MeanAndVariance,
                                endToEndLatency: MeanAndVariance,
                                ioMetrics: IOMetrics)
  case class UpdateEventRateOut(rate: Double)
  case class UpdateEventSizeOut(size: Long)
  private case object SamplingTick
  private case object SamplingTickKey
}