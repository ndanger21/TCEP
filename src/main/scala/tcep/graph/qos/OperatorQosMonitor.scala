package tcep.graph.qos

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Timers}
import breeze.stats._
import breeze.stats.meanAndVariance.MeanAndVariance
import com.typesafe.config.ConfigFactory
import tcep.data.Events.Event
import tcep.data.Queries.Query
import tcep.graph.qos.OperatorQosMonitor._
import tcep.graph.transition.mapek.DynamicCFMNames._
import tcep.machinenodes.qos.BrokerQoSMonitor.BandwidthUnit.{BytePerSec, KBytePerSec}
import tcep.machinenodes.qos.BrokerQoSMonitor._
import tcep.prediction.PredictionHelper.{OfflineAndOnlinePredictions, Throughput}
import tcep.utils.SizeEstimator

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
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
class OperatorQosMonitor(query: Query, operator: ActorRef, monitorCentral: ActorRef) extends Actor with Timers with ActorLogging {

  val samplingInterval: FiniteDuration = FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.MILLISECONDS)
  implicit val blockingIoDispatcher: ExecutionContext = context.system.dispatchers.lookup("blocking-io-dispatcher")

  var eventSizeIn: ListBuffer[Long] = ListBuffer.empty
  var eventSizeOut: Long = 0
  var eventRateIn: ListBuffer[Double] = ListBuffer.empty
  var eventRateOut: Throughput = Throughput(0, FiniteDuration(1, TimeUnit.SECONDS))
  var interArrivalLatency: MeanAndVariance = MeanAndVariance(0.0, 0.0, 0)
  var processingLatency: MeanAndVariance = MeanAndVariance(0.0, 0.0, 0)
  var networkToParentLatency: MeanAndVariance = MeanAndVariance(0.0, 0.0, 0) // binary operators: longest path to a parent
  var endToEndLatency: MeanAndVariance = MeanAndVariance(0.0, 0.0, 0)
  val eventSamples: ListBuffer[Event] = ListBuffer.empty
  val brokerQoSMonitor: ActorSelection = context.system.actorSelection(context.system./("TaskManager*")./("BrokerQosMonitor*"))
  var lastSamples: Samples = List()

  override def preStart(): Unit = {
    super.preStart()
    timers.startTimerAtFixedRate(SamplingTickKey, SamplingTick, samplingInterval)
  }

  override def receive: Receive = {
    case event: Event =>
      eventSamples += event

    case SamplingTick =>
      val start = System.nanoTime()
      eventSizeOut = if (eventSamples.nonEmpty) SizeEstimator.estimate(eventSamples.head) else 0
      val parentCount = if (eventSamples.nonEmpty) eventSamples.head.monitoringData.processingStats.eventSizeIn.size else 0
      eventSizeIn.clear()
      eventRateIn.clear()
      for (i <- 0 until parentCount) {
        eventSizeIn += (if (eventSamples.nonEmpty) eventSamples.map(_.monitoringData.processingStats.eventSizeIn(i)).sum / eventSamples.size else 0)
        eventRateIn += (if (eventSamples.nonEmpty) eventSamples.map(_.monitoringData.processingStats.eventRateIn(i).amount).sum / eventSamples.size else 0) // total events arrived per sampling interval
      }
      eventRateOut = Throughput(eventSamples.size, samplingInterval)
      // ms
      val processingLatencySamples = eventSamples.map(_.monitoringData.processingStats.processingLatencyNS.toDouble / 1e6)
      val networkLatencySamples = eventSamples.map(_.monitoringData.processingStats.lastHopLatency.toDouble)
      val e2eLatencySamples = processingLatencySamples.zip(networkLatencySamples).map(l => l._1 + l._2)
      processingLatency = meanAndVariance(processingLatencySamples)
      networkToParentLatency = meanAndVariance(networkLatencySamples)
      endToEndLatency = meanAndVariance(e2eLatencySamples)
      val arrivalTimestampsNS = eventSamples.map(_.monitoringData.processingStats.processingStartNS).toVector
      val interArrivals = ListBuffer[Double]()
      for (i <- 0 until arrivalTimestampsNS.size - 1) interArrivals += (arrivalTimestampsNS(i + 1) - arrivalTimestampsNS(i)) / 1e6
      interArrivalLatency = meanAndVariance(interArrivals)

      log.debug("operator SamplingTick took {}ms: {} samples\neventRateIO: {} {}\neventSizeIO {} {} \nnetworkLatencyParents {}\nprocessingLatency {}\ne2eLatency {} \ninterArrivalLatency {}",
        Array((System.nanoTime() - start) / 1e6, eventSamples.size, eventRateIn, eventRateOut, eventRateIn, eventSizeOut, networkToParentLatency, processingLatency, endToEndLatency, interArrivalLatency))

      eventSamples.clear()
      operator ! UpdateEventRateOut(eventRateOut)
      operator ! UpdateEventSizeOut(eventSizeOut)
      brokerQoSMonitor ! GetBrokerMetrics(Some(Set(operator))) // continue in next case

    case b: BrokerQosMetrics =>
      val currentValues = getCurrentMetrics
      operator ! CPULoadUpdate(b.cpuLoad)
      lastSamples = List(Some((currentValues, b)),  lastSamples.headOption).flatten
      monitorCentral ! SampleUpdate(query, lastSamples.head)
      log.debug("received broker samples, last sample is now {}", lastSamples.head)

    case GetIOMetrics =>
      sender() ! IOMetrics(Throughput(eventRateIn.sum, samplingInterval), eventRateOut, bandwidthIn, bandwidthOut)

    case GetOperatorQoSMetrics => sender() ! getCurrentMetrics
    case GetSamples => sender() ! lastSamples
  }

  def bandwidthIn: Bandwidth = Bandwidth(eventRateIn.zip(eventSizeIn).map(p => (p._1 / samplingInterval.toSeconds) * p._2).sum, BytePerSec)
  def bandwidthOut: Bandwidth = Bandwidth(eventRateOut.getEventsPerSec * eventSizeOut, BytePerSec)
  def getCurrentMetrics: OperatorQoSMetrics = OperatorQoSMetrics(
    eventSizeIn.toList, eventSizeOut,
    interArrivalLatency, processingLatency, networkToParentLatency, endToEndLatency,
    IOMetrics(Throughput(eventRateIn.sum, samplingInterval), eventRateOut, bandwidthIn, bandwidthOut)
  )


}

object OperatorQosMonitor {
  case class SampleUpdate(query: Query, lastSample: Sample)
  case class SampleAndPredictionsUpdate(sampleUpdate: SampleUpdate, predictions: OfflineAndOnlinePredictions)
  case object GetSamples
  type Sample = (OperatorQoSMetrics, BrokerQosMetrics)
  type Samples = List[Sample]
  def getFeatureValue(sample: Sample, feature: String): AnyVal = {
    if(ALL_FEATURES.contains(feature)) {
      feature match {
        case EVENTSIZE_IN_KB => sample._1.eventSizeIn.sum.toDouble / 1024
        case EVENTSIZE_OUT_KB => sample._1.eventSizeOut.toDouble / 1024
        case OPERATOR_SELECTIVITY => sample._1.selectivity
        case EVENTRATE_IN => sample._1.ioMetrics.incomingEventRate.amount
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
  def getTargetMetricValue(sample: Sample, metric: String): Double = {
    metric match {
      case PROCESSING_LATENCY_MEAN_MS => sample._1.processingLatency.mean
      case EVENTRATE_OUT => sample._1.ioMetrics.outgoingEventRate.amount
      case END_TO_END_LATENCY_MEAN_MS => sample._1.endToEndLatency.mean
      case END_TO_END_LATENCY_STD_MS => sample._1.endToEndLatency.stdDev
      case _ => throw new IllegalArgumentException(s"can't retrieve metric value $metric, must be one of ${ALL_TARGET_METRICS}")
    }

  }

  case object GetOperatorQoSMetrics
  case class OperatorQoSMetrics(eventSizeIn: List[Long],
                                eventSizeOut: Long,
                                interArrivalLatency: MeanAndVariance,
                                processingLatency: MeanAndVariance,
                                networkToParentLatency: MeanAndVariance,
                                endToEndLatency: MeanAndVariance,
                                ioMetrics: IOMetrics) {
    def selectivity: Double = ioMetrics.outgoingEventRate.getEventsPerSec / ioMetrics.incomingEventRate.getEventsPerSec

  }
  case class UpdateEventRateOut(rate: Throughput)
  case class UpdateEventSizeOut(size: Long)
  private case object SamplingTick
  private case object SamplingTickKey
}