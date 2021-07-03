package tcep.graph.nodes

import tcep.data.Events.{Event, Event1}
import tcep.data.Queries.NewAverageQuery
import tcep.graph.nodes.traits.Node.NodeProperties
import tcep.graph.nodes.traits.UnaryNode
import tcep.placement.HostInfo
import tcep.simulation.tcep.LinearRoadDataNew

import scala.collection.mutable.ListBuffer

case class NewAverageNode(query: NewAverageQuery, hostInfo: HostInfo, np: NodeProperties) extends UnaryNode {

  var lastEmit = System.currentTimeMillis()

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event =>
      event.updateArrivalTimestamp()
      val s = sender()
      if (np.parentActor.contains(s)) {
        val avgData = event match {
          case Event1(e1) =>
            this.computeAverage(List(e1))
          case strange =>
            log.info(s"Strange Event received: $strange")
            List()
        }
        if (avgData.size > 0) {
          for (data <- avgData) {
            val avgEvent = Event1(data)
            avgEvent.monitoringData = event.monitoringData
            emitEvent(avgEvent, np.eventCallback)
          }
        } else {
          val emptyEvent = Event1(LinearRoadDataNew(-1, -1, -1, -100, false))
          emptyEvent.monitoringData = event.monitoringData
          emitEvent(emptyEvent, np.eventCallback)
        }
      }
    case unhandledMessage =>
  }

  def computeAverage(dataList: List[Any]): List[LinearRoadDataNew] = {
    var out: ListBuffer[LinearRoadDataNew] = ListBuffer.empty[LinearRoadDataNew]
    for (data <- dataList) {
      data match {
        case l: LinearRoadDataNew =>
          var avg = 0.0
          for (speed <- l.dataWindow.get)
            avg += speed
          if (l.dataWindow.get.size > 0)
            avg = avg / l.dataWindow.get.size
          else
            avg = 0.0
          l.avgSpeed = Some(avg)
          out += l
      }
    }
    out.toList
  }
}
