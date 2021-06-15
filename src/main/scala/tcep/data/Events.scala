package tcep.data

import akka.actor.{ActorRef, Address}
import akka.event.LoggingAdapter
import com.typesafe.config.ConfigFactory
import tcep.machinenodes.helper.actors.{MySerializable, PlacementMessage}
import tcep.placement.HostInfo

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/**
  * Helpers object for conversions and representation of different events in terms of
  * case classes.
  * */
object Events {

  case object DependenciesRequest extends PlacementMessage
  case class DependenciesResponse(dependencies: Seq[ActorRef]) extends PlacementMessage
  //TODO obsolete once operator monitors are implemented
  val eventIntervalMicroseconds: Int = ConfigFactory.load().getInt("constants.event-interval-microseconds")
  val eventsPerSecond: Double = FiniteDuration(eventIntervalMicroseconds, TimeUnit.MICROSECONDS)./(FiniteDuration(1, TimeUnit.SECONDS))

  sealed case class MonitoringData(
                                  var predecessorHost: List[Address] = List(),
                                  var lastUpdate: List[(Address, Long)] = List(),
                                  publishingRate: Double = eventsPerSecond,
                                  var operatorHops: Int = 0,
                                  var messageHops: Int = 0,
                                  var averageLoad: Double = 0.0d,
                                  var latency: Long = 0,
                                  var networkUsage: List[Double] = List(),
                                  var placementOverhead: Long = 0,
                                  var eventOverhead: Long = 0,
                                  var creationTimestamp: Long = System.currentTimeMillis(),
                                  var processingStats: OperatorProcessingStats = OperatorProcessingStats()
                                  )

  sealed case class OperatorProcessingStats(
                                             var processingStartNS: Long = -1,
                                             var processingLatencyNS: Long = -1,
                                             var departureMS: Long = -1,
                                             var lastHopLatency: Long = -1,
                                             var eventSizeIn: Vector[Long] = Vector(),
                                             var eventRateIn: Vector[Double] = Vector(), // updated upon event arrival, unchanged when reaching monitor unlike parentEventRateOut (updated before send);
                                             var parentEventRateOut: Vector[Double] = Vector() // out on previous, in on current operator
                                        )
  sealed abstract class Event(var monitoringData: MonitoringData = MonitoringData()) extends MySerializable {

    def init()(implicit creatorAddress: Address): Unit =  {
      monitoringData.predecessorHost = List(creatorAddress)
      monitoringData.lastUpdate = List((creatorAddress, System.currentTimeMillis()))
      //SpecialStats.log(this.toString, "EvenDataUpdate", s"initialized event monitoring data: $monitoringData;
      // ${monitoringData.predecessorHost} ;${monitoringData.lastUpdate}; created: ${monitoringData.creationTimestamp}")
    }

    def copyMonitoringData(toCopy: MonitoringData): Unit = monitoringData = toCopy
    // called when event is taken off mailbox by operator
    def updateArrivalTimestamp(): Unit = {
      this.monitoringData.processingStats.processingStartNS = System.nanoTime()
      this.monitoringData.processingStats.lastHopLatency = System.currentTimeMillis() - this.monitoringData.processingStats.departureMS
      this.monitoringData.processingStats.eventRateIn = this.monitoringData.processingStats.parentEventRateOut
    }
    // called before event is sent to subscribers
    def updateDepartureTimestamp(eventSizeOut: Long, eventRateOut: Double): Unit = {
      this.monitoringData.processingStats.departureMS = System.currentTimeMillis()
      this.monitoringData.processingStats.processingLatencyNS = System.nanoTime() - this.monitoringData.processingStats.processingStartNS
      this.monitoringData.processingStats.eventSizeIn = Vector(eventSizeOut)
      this.monitoringData.processingStats.parentEventRateOut = Vector(eventRateOut) // out on current, in on next operator
    }
  }


  /**
    * @author Niels
    *         updates the monitoring data attached to each event before sending it to subscriber
    *         depends on whether the previous operator is hosted on this host as well or not
    * @param event the event to update
    */
  def updateMonitoringData(log: LoggingAdapter, event: Event, hostInfo: HostInfo, currentLoad: Double, eventRateOut: Double, eventSizeOut: Long)(implicit ec: ExecutionContext): Unit = {
    try {
      //val start = System.nanoTime()
      //log.debug(s"updating MonitoringData start ")
      //log.debug(s"$hostInfo \n $event \n ${event.monitoringData.map("\n"+_)}")
      if (event.monitoringData == null) throw new IllegalArgumentException(s"received empty monitoringData of ${event} ${event.monitoringData}")
      if (hostInfo == null) throw new IllegalArgumentException(s"updateMonitoringData called with null hostInfo")
      // increment some monitoringData items only when event has a parent on a different host, i.e. had to go over the network
      if (event.monitoringData.predecessorHost.exists(!_.equals(hostInfo.member.address))) {
        event.monitoringData.messageHops = event.monitoringData.messageHops + 1
        val accumulatedBDP = event.monitoringData.networkUsage.sum
        event.monitoringData.networkUsage = List(hostInfo.operatorMetrics.operatorToParentBDP.values.sum + accumulatedBDP)
        // overhead for sending the event over the network
        event.monitoringData.eventOverhead += eventSizeOut
      }

      event.monitoringData.operatorHops += 1
      val hops = event.monitoringData.operatorHops
      event.monitoringData.averageLoad = (currentLoad / hops) + (event.monitoringData.averageLoad * (hops - 1) / hops) // weighted avg
      // keep the larger latency since we want to know the total delay between event creation and reception (i.e. the earliest point any event leading to this event was created)
      val lastUpdates = event.monitoringData.lastUpdate
      val now = System.currentTimeMillis()
      assert(lastUpdates.nonEmpty, s"event monitoringData item lastUpdates was not initialized! $event")
      event.monitoringData.latency = math.max(now - lastUpdates.head._2, now - lastUpdates.last._2)
      event.monitoringData.predecessorHost = List(hostInfo.member.address)
      event.monitoringData.lastUpdate = List((hostInfo.member.address, System.currentTimeMillis()))
      event.monitoringData.placementOverhead += hostInfo.operatorMetrics.accPlacementMsgOverhead
      event.updateDepartureTimestamp(eventSizeOut, eventRateOut)
      //log.info(s"updating MonitoringData complete after ${System.nanoTime() - start}")
    } catch {
      case e: Throwable => log.error(e, s"failed to update monitoringData of event ${event.monitoringData}")
    }
  }
  /**
    * Merges the MonitoringData fields of two events into one so Monitoring information doesn't get lost at binary operators
    * call order on an operator: mergeMonitoringData, then updateMonitoringData
    * @author Niels
    * @param event the event that gets the merged monitoring data attached
    * @param a ListBuffer of the first event to be merged
    * @param b ListBuffer of the other event to be merged
    */
  def mergeMonitoringData(event: Event, a: MonitoringData, b: MonitoringData, log: LoggingAdapter): Event = {
    //log.debug(s"DEBUG entered mergeMonitoringData with \n ${a} \n $b")
    event.monitoringData.messageHops = math.max(a.messageHops, b.messageHops)
    event.monitoringData.operatorHops = math.max(a.operatorHops, b.operatorHops)
    val hopsA = a.operatorHops
    val hopsB = b.operatorHops
    event.monitoringData.averageLoad =
      if (hopsA + hopsB <= 0) 0.5d * (a.averageLoad + b.averageLoad)
      else ((hopsA * a.averageLoad) + (hopsB * b.averageLoad)) / (hopsA + hopsB)
    event.monitoringData.latency = math.max(a.latency, b.latency)
    event.monitoringData.networkUsage = List(a.networkUsage.sum, b.networkUsage.sum)
    event.monitoringData.placementOverhead = a.placementOverhead + b.placementOverhead
    event.monitoringData.eventOverhead = a.eventOverhead + b.eventOverhead
    event.monitoringData.predecessorHost = List(a.predecessorHost.headOption, b.predecessorHost.headOption).flatten // treat empty list with option + flatten
    event.monitoringData.lastUpdate = List(a.lastUpdate.headOption, b.lastUpdate.headOption).flatten
    event.monitoringData.creationTimestamp = math.min(a.creationTimestamp, b.creationTimestamp)
    event.monitoringData.processingStats = OperatorProcessingStats(
      // set by updateMonitoringData before sending event
      processingLatencyNS = -1,
      departureMS = -1,
      // use the last event since its arrival triggers processing
      processingStartNS = math.max(a.processingStats.processingStartNS, b.processingStats.processingStartNS),
      // use the critical path (longest parent latency)
      lastHopLatency = math.max(a.processingStats.lastHopLatency, b.processingStats.lastHopLatency),
      eventSizeIn = Vector(a.processingStats.eventSizeIn.head, b.processingStats.eventSizeIn.head),
      parentEventRateOut = Vector(a.processingStats.parentEventRateOut.head, b.processingStats.parentEventRateOut.head),
      eventRateIn = Vector(a.processingStats.eventRateIn.head, b.processingStats.eventRateIn.head)
    )
    //log.debug(s"DEBUG mergeMonitoringData complete ${event.monitoringData}")
    event
  }


  case class Event1(e1: Any)(implicit creatorAddress: Address)                                              extends Event
  case class Event2(e1: Any, e2: Any)(implicit creatorAddress: Address)                                     extends Event
  case class Event3(e1: Any, e2: Any, e3: Any)(implicit creatorAddress: Address)                            extends Event
  case class Event4(e1: Any, e2: Any, e3: Any, e4: Any)(implicit creatorAddress: Address)                   extends Event
  case class Event5(e1: Any, e2: Any, e3: Any, e4: Any, e5: Any)(implicit creatorAddress: Address)          extends Event
  case class Event6(e1: Any, e2: Any, e3: Any, e4: Any, e5: Any, e6: Any)(implicit creatorAddress: Address) extends Event

  val errorMsg: String = "Panic! Control flow should never reach this point!"

  case class EventToBoolean[A, B](event2: Event2){
    def apply[A, B](f: (A, B) => Boolean): Event => Boolean = {
      case Event2(e1, e2) => f.asInstanceOf[(Any, Any) => Boolean](e1, e2)
      case _ => sys.error(errorMsg)
    }
  }
  def toFunEventAny[A](f: (A) => Any): Event => Any = {
    case Event1(e1) => f.asInstanceOf[(Any) => Any](e1)
    case _ => sys.error(errorMsg)
  }

  def toFunEventAny[A, B](f: (A, B) => Any): Event => Any = {
    case Event2(e1, e2) => f.asInstanceOf[(Any, Any) => Any](e1, e2)
    case _ => sys.error(errorMsg)
  }

  def toFunEventAny[A, B, C](f: (A, B, C) => Any): Event => Any = {
    case Event3(e1, e2, e3) => f.asInstanceOf[(Any, Any, Any) => Any](e1, e2, e3)
    case _ => sys.error(errorMsg)
  }

  def toFunEventAny[A, B, C, D](f: (A, B, C, D) => Any): Event => Any = {
    case Event4(e1, e2, e3, e4) => f.asInstanceOf[(Any, Any, Any, Any) => Any](e1, e2, e3, e4)
    case _ => sys.error(errorMsg)
  }

  def toFunEventAny[A, B, C, D, E](f: (A, B, C, D, E) => Any): Event => Any = {
    case Event5(e1, e2, e3, e4, e5) => f.asInstanceOf[(Any, Any, Any, Any, Any) => Any](e1, e2, e3, e4, e5)
    case _ => sys.error(errorMsg)
  }

  def toFunEventAny[A, B, C, D, E, F](f: (A, B, C, D, E, F) => Any): Event => Any = {
    case Event6(e1, e2, e3, e4, e5, e6) => f.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Any](e1, e2, e3, e4, e5, e6)
    case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A](f: (A) => Boolean): Event => Boolean = {
    case Event1(e1) => f.asInstanceOf[(Any) => Boolean](e1)
    case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A, B](f: (A, B) => Boolean): Event => Boolean = {
    case Event2(e1, e2) => f.asInstanceOf[(Any, Any) => Boolean](e1, e2)
    case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A, B, C](f: (A, B, C) => Boolean): Event => Boolean = {
    case Event3(e1, e2, e3) => f.asInstanceOf[(Any, Any, Any) => Boolean](e1, e2, e3)
    case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A, B, C, D](f: (A, B, C, D) => Boolean): Event => Boolean = {
    case Event4(e1, e2, e3, e4) => f.asInstanceOf[(Any, Any, Any, Any) => Boolean](e1, e2, e3, e4)
    case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A, B, C, D, E](f: (A, B, C, D, E) => Boolean): Event => Boolean = {
    case Event5(e1, e2, e3, e4, e5) => f.asInstanceOf[(Any, Any, Any, Any, Any) => Boolean](e1, e2, e3, e4, e5)
    case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A, B, C, D, E, F](f: (A, B, C, D, E, F) => Boolean): Event => Boolean = {
    case Event6(e1, e2, e3, e4, e5, e6) => f.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Boolean](e1, e2, e3, e4, e5, e6)
    case _ => sys.error(errorMsg)
  }

  def printEvent(event: Event, log: LoggingAdapter): Unit = event match {
    case Event1(e1) => log.info(s"EVENT:\tEvent1($e1)")
    case Event2(e1, e2) => log.info(s"Event:\tEvent2($e1, $e2)")
    case Event3(e1, e2, e3) => log.info(s"Event:\tEvent3($e1, $e2, $e3)")
    case Event4(e1, e2, e3, e4) => log.info(s"Event:\tEvent4($e1, $e2, $e3, $e4)")
    case Event5(e1, e2, e3, e4, e5) => log.info(s"Event:\tEvent5($e1, $e2, $e3, $e4, $e5)")
    case Event6(e1, e2, e3, e4, e5, e6) => log.info(s"Event:\tEvent6($e1, $e2, $e3, $e4, $e5, $e6)")
  }
}