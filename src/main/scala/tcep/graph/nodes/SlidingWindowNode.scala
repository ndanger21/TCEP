package tcep.graph.nodes

import akka.actor.Cancellable
import tcep.data.Events.{Event, Event1, Event6, MonitoringData}
import tcep.data.Queries.SlidingWindowQuery
import tcep.graph.nodes.traits.Node.NodeProperties
import tcep.graph.nodes.traits.UnaryNode
import tcep.placement.HostInfo
import tcep.simulation.tcep.LinearRoadDataNew

import java.time.Duration
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration

case class SlidingWindowNode(query: SlidingWindowQuery, hostInfo: HostInfo, np: NodeProperties) extends UnaryNode {

  var storage: ListBuffer[(Double, Double, MonitoringData)] = ListBuffer.empty
  var lastEmit: Double = System.currentTimeMillis().toDouble
  val rate = 2
  var avgEmitter: Cancellable = _
  //var avgEmitter = context.system.scheduler.schedule(FiniteDuration(0, TimeUnit.SECONDS), FiniteDuration(rate, TimeUnit.SECONDS))(this.emitAvg)

  override def preStart(): Unit = {
    super.preStart()
    avgEmitter = context.system.scheduler.schedule(FiniteDuration(0, TimeUnit.SECONDS), FiniteDuration(rate, TimeUnit.SECONDS))(this.emitAvg)
  }

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event =>
      event.updateArrivalTimestamp()
      val s = sender()
      if (np.parentActor.contains(s)) {
        event match {
          case Event6(e1, e2, e3, e4, e5, e6) =>
            this.store(List(e1.asInstanceOf[LinearRoadDataNew], e2.asInstanceOf[LinearRoadDataNew], e3.asInstanceOf[LinearRoadDataNew], e4.asInstanceOf[LinearRoadDataNew], e5.asInstanceOf[LinearRoadDataNew], e6.asInstanceOf[LinearRoadDataNew]), event.monitoringData)
          case _ => ???
        }
        val eventData = if (this.storage.size > 0) {
           LinearRoadDataNew(-1, this.query.sectionFilter.get, -1, -1, false, Some(this.getWindow()._1))
        } else {
           LinearRoadDataNew(-1, this.query.sectionFilter.get, -1, -1, false, Some(List(0)))
        }
        val avgEvent = Event1(eventData)
        avgEvent.init()
        avgEvent.copyMonitoringData(event.monitoringData)
        emitEvent(avgEvent, np.eventCallback)
      }
    case unhandledMessage =>
  }

  def emitAvg(): Unit = {
    if (this.storage.size > 0) {
      val windowData = this.getWindow()
      val eventData = LinearRoadDataNew(-1, this.query.sectionFilter.get, -1, -1, false, Some(windowData._1))
      val avgEvent = Event1(eventData)
      avgEvent.init()
      avgEvent.copyMonitoringData(windowData._2)
      emitEvent(avgEvent, np.eventCallback)
    } else {
      val eventData = LinearRoadDataNew(-1, this.query.sectionFilter.get, -1, -1, false, Some(List(0)))
      val avgEvent = Event1(eventData)
      avgEvent.init()
      avgEvent.updateArrivalTimestamp()
      emitEvent(avgEvent, np.eventCallback)
    }
  }

  def store(dataList: List[LinearRoadDataNew], monitoringData: MonitoringData) = {
    val currentTime = System.currentTimeMillis() / 1000.0d
    if (this.query.sectionFilter.isDefined) {
      for (value <- dataList) {
        if (value.section == this.query.sectionFilter.get) {
          this.storage += Tuple3(currentTime, value.speed, monitoringData)
        }
      }
    }
  }

  def getWindow(): (List[Double], MonitoringData) = {
    val currentTime = System.currentTimeMillis() / 1000.0d //seconds
    val removeThreshold = currentTime-this.query.windowSize
    while (this.storage.size > 0 && this.storage.head._1 < removeThreshold)
      this.storage = this.storage.tail
    var out = ListBuffer.empty[Double]
    for (event <- this.storage)
      out += (event._2)
    (out.toList, this.storage.last._3)
  }

        /*def store(dataList: List[LinearRoadDataNew]): List[LinearRoadDataNew] = {
    val currentTime = System.currentTimeMillis() / 1000.0d // seconds
    var out: ListBuffer[LinearRoadDataNew] = ListBuffer.empty[LinearRoadDataNew]
    if (this.query.sectionFilter.isDefined) {
      for (value <- dataList){
        if (value.section == this.query.sectionFilter.get) {
          this.storage += (currentTime -> value.speed)
        }
        if (value.change && value.section-1 == this.query.sectionFilter.get) {
          value.dataWindow = Some(this.getWindow())
          out += value
        }
      }
      out.toList
    } else {
      List()
    }
  }

  def getWindow(): List[Double] = {
    val currentTime = System.currentTimeMillis() / 1000.0d //seconds
    val removeThreshold = currentTime-this.query.windowSize
    while (this.storage.size > 0 && this.storage.head._1 < removeThreshold)
      this.storage = this.storage.tail
    var out = ListBuffer.empty[Double]
    for (event <- this.storage)
      out += (event._2)
    out.toList
  }*/

  override def maxWindowTime(): Duration = {
    Duration.ofSeconds(0)
  }
}