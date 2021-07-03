package tcep.graph.nodes

import tcep.data.Events._
import tcep.data.Queries.ShrinkingFilterQuery
import tcep.graph.nodes.traits.Node.NodeProperties
import tcep.graph.nodes.traits.UnaryNode
import tcep.placement.HostInfo
import tcep.simulation.tcep.LinearRoadDataNew

import scala.collection.mutable.ListBuffer

case class ShrinkingFilterNode(query: ShrinkingFilterQuery, hostInfo: HostInfo, np: NodeProperties) extends UnaryNode {

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event =>
      event.updateArrivalTimestamp()
      val s = sender()
      if (np.parentActor.contains(s)) {
        val value: List[Any] = event match {
          case Event1(e1) =>
            this.handle(List(e1))
          case Event2(e1, e2) =>
            this.handle(List(e1, e2))
          case Event3(e1, e2, e3) =>
            this.handle(List(e1, e2, e3))
          case Event4(e1, e2, e3, e4) =>
            this.handle(List(e1, e2, e3, e4))
          case Event5(e1, e2, e3, e4, e5) =>
            this.handle(List(e1, e2, e3, e4, e5))
          case Event6(e1, e2, e3, e4, e5, e6) =>
            this.handle(List(e1, e2, e3, e4, e5, e6))
        }
        val outEvent: Option[Event] = value.size match {
          case 0 =>
            if (this.query.emitAlways.isDefined & this.query.emitAlways.get)
              Some(Event1(LinearRoadDataNew(-1, -1, -1, -1, false)))
            else
              None
          case 1 =>
            Some(Event1(value.head))
          case 2 =>
            Some(Event2(value(0), value(1)))
          case 3 =>
            Some(Event3(value(0), value(1), value(2)))
          case 4 =>
            Some(Event4(value(0), value(1), value(2), value(3)))
          case 5 =>
            Some(Event5(value(0), value(1), value(2), value(3), value(4)))
          case 6 =>
            Some(Event6(value(0), value(1), value(2), value(3), value(4), value(5)))
        }
        if (outEvent.isDefined) {
          outEvent.get.monitoringData = event.monitoringData
          emitEvent(outEvent.get, np.eventCallback)
        }
      }
    case unhandledMessage =>
  }

  def handle(dataList: List[Any]) = {
    var out = ListBuffer.empty[Any]
    for(data <- dataList) {
      if(this.query.cond(data)){
        out += data
      }
    }
    out.toList
  }
}
