package tcep.graph.nodes

import tcep.data.Events._
import tcep.data.Queries.ConverterQuery
import tcep.graph.nodes.traits.Node.NodeProperties
import tcep.graph.nodes.traits.UnaryNode
import tcep.placement.HostInfo

case class ConverterNode(query: ConverterQuery, hostInfo: HostInfo, np: NodeProperties) extends UnaryNode {

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event =>
      event.updateArrivalTimestamp()
      val s = sender()
      if (np.parentActor.contains(s)) {
        val eventList: List[Any] = event match {
          case Event1(e1) =>
            this.handle(e1)
          case Event2(e1, e2) =>
            this.handle(e1)++this.handle(e2)
          case Event3(e1, e2, e3) =>
            this.handle(e1)++this.handle(e2)++this.handle(e3)
          case Event4(e1, e2, e3, e4) =>
            this.handle(e1)++this.handle(e2)++this.handle(e3)++this.handle(e4)
          case Event5(e1, e2, e3, e4, e5) =>
            this.handle(e1)++this.handle(e2)++this.handle(e3)++this.handle(e4)++this.handle(e5)
          case Event6(e1, e2, e3, e4, e5, e6) =>
            this.handle(e1)++this.handle(e2)++this.handle(e3)++this.handle(e4)++this.handle(e5)++this.handle(e6)
            //List(e1, e2, e3, e4, e5, e6)
        }
        val convertedEvent = Event1(eventList)
        convertedEvent.monitoringData = event.monitoringData
        emitEvent(convertedEvent, np.eventCallback)
      }
    case unhandledMessage =>
  }

  def handle(value: Any): List[Any] = {
    val outLi = value match {
      case l: List[Any] => l
      case anything => List(anything)
    }
    outLi
  }

}
