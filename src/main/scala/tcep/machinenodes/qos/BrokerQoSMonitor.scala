package tcep.machinenodes.qos

import akka.actor.{Actor, ActorLogging, ActorRef, Timers}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import tcep.graph.nodes.traits.SystemLoadUpdater
import tcep.graph.transition.MAPEK.{AddOperator, RemoveOperator}
import tcep.machinenodes.qos.BrokerQoSMonitor.BandwidthUnit.{BandwidthUnit, BytePerSec, KBytePerSec, MBytePerSec}
import tcep.machinenodes.qos.BrokerQoSMonitor._

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/**
  * monitoring actor present on every cluster node
  * keeps track of node-level metrics:
  * - #active operators
  * - #CPU threads available
  * over time window:
  * - current CPU load average
  * - incoming bandwidth from events
  * - outgoing bandwidth from events
  * - incoming events from all operators
  * - outgoing events from all operators
  *
  */
class BrokerQoSMonitor extends Actor with SystemLoadUpdater with Timers with ActorLogging {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val askTimeout: Timeout = Timeout(FiniteDuration(ConfigFactory.load().getInt("constants.default-request-timeout"), TimeUnit.SECONDS))
  val cpuThreadCount: Int = Runtime.getRuntime.availableProcessors()
  var operatorsOnNode: mutable.Set[ActorRef] = mutable.Set.empty
  var currentNodeIOMetrics: IOMetrics = IOMetrics()

  override def preStart(): Unit = {
    super.preStart()
    timers.startTimerWithFixedDelay(IOMetricUpdateKey, IOMetricUpdateTick, samplingInterval)
  }

  override def receive: Receive = super.receive orElse {
    case GetCPULoad => sender() ! currentLoad //TODO currently returns avg jvm load of last minute from JMXBean; alternative would be mpstat for entire node over arbitrary interval
    case GetCPUThreadCount => sender() ! cpuThreadCount
    case GetNodeOperatorCount => sender() ! operatorsOnNode.size
    case GetIOMetrics => sender() ! currentNodeIOMetrics
    case GetNodeMetrics => sender() ! NodeQosMetrics(currentLoad, cpuThreadCount, operatorsOnNode.size, currentNodeIOMetrics)
    case AddOperator(ref) => operatorsOnNode += ref
    case RemoveOperator(ref) => operatorsOnNode -= ref
    case IOMetricUpdateTick =>
      log.info(s"IOMetric UpdateTick for operators $operatorsOnNode")
      Future.traverse(operatorsOnNode)(op => (op ? GetIOMetrics).mapTo[IOMetrics])
        .map(e => {
          log.info(s"processing IOMetrics ${e}")
          e.fold(IOMetrics())((a, b) => a + b)
        })
        .map(e => IOMetricUpdate(e))
        .pipeTo(self) // pipe future as msg to self to avoid closing over state (currentNodeIOMetrics)

    case IOMetricUpdate(update) =>
      currentNodeIOMetrics = update
      log.info(s"BrokerNode QoS update: load $currentLoad, threads $cpuThreadCount, operators ${operatorsOnNode.size}, IO $currentNodeIOMetrics}")
  }

}

object BrokerQoSMonitor {
  case object GetCPULoad
  case object GetCPUThreadCount
  case object GetNodeOperatorCount
  case object GetIncomingEventRate
  case object GetOutgoingEventRate
  case object GetIncomingBandwidth
  case object GetOutgoingBandwidth
  case object GetIOMetrics
  case object GetNodeMetrics
  case class NodeQosMetrics(cpuLoad: Double, cpuThreadCount: Int, deployedOperators: Int, IOMetrics: IOMetrics)
  private case class IOMetricUpdate(ioMetrics: IOMetrics)
  private case object IOMetricUpdateTick
  private case object IOMetricUpdateKey
  case class IOMetrics(
                        incomingEventRate: Double = 0.0d,
                        outgoingEventRate: Double = 0.0d,
                        incomingBandwidth: Bandwidth = Bandwidth(0, KBytePerSec),
                        outgoingBandwidth: Bandwidth = Bandwidth(0, KBytePerSec)
                      ) {
    def +(that: IOMetrics) = IOMetrics(
      this.incomingEventRate + that.incomingEventRate,
      this.outgoingEventRate + that.outgoingEventRate,
      this.incomingBandwidth + that.incomingBandwidth,
      this.outgoingBandwidth + that.outgoingBandwidth)
  }
  case class Bandwidth(amount: Double, unit: BandwidthUnit) {
    def +(that: Bandwidth): Bandwidth = {
      val thisBytes = conversionFactor(this.unit) * this.amount
      val thatBytes = conversionFactor(that.unit) * that.amount
      Bandwidth((thisBytes + thatBytes) / conversionFactor(this.unit), this.unit)
    }
    private def conversionFactor(a: BandwidthUnit): Int = {
      a match {
        case BytePerSec => 1
        case KBytePerSec => 1024
        case MBytePerSec => 1024 * 1024
        case _ => throw new IllegalArgumentException(s"unknown bandwidth unit $a")
      }
    }
  }
  object BandwidthUnit extends Enumeration {
    type BandwidthUnit = Value
    val BytePerSec, KBytePerSec, MBytePerSec = Value
  }

}