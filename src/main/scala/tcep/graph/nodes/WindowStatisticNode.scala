package tcep.graph.nodes

import tcep.data.Events._
import tcep.data.Queries.WindowStatisticQuery
import tcep.graph.nodes.traits.Node.NodeProperties
import tcep.graph.nodes.traits.UnaryNode
import tcep.placement.HostInfo
import tcep.simulation.tcep.{StatisticData, YahooDataNew}

import scala.collection.mutable.{HashMap, ListBuffer}

case class WindowStatisticNode(query: WindowStatisticQuery, hostInfo: HostInfo, np: NodeProperties) extends UnaryNode {

  var storage: HashMap[Int, ListBuffer[Double]] = HashMap.empty[Int, ListBuffer[Double]]

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event =>
      event.updateArrivalTimestamp()
      val updated = event match {
        case Event1(e1) =>
          this.store(List(e1))
        case Event2(e1, e2) =>
          this.store(List(e1, e2))
        case Event3(e1, e2, e3) =>
          this.store(List(e1, e2, e3))
        case Event4(e1, e2, e3, e4) =>
          this.store(List(e1, e2, e3, e4))
        case Event5(e1, e2, e3, e4, e5) =>
          this.store(List(e1, e2, e3, e4, e5))
        case Event6(e1, e2, e3, e4, e5, e6) =>
          this.store(List(e1, e2, e3, e4, e5, e6))
      }
      for (id <- updated) {
        val statEvent = Event1(StatisticData(id, this.storage.get(id).get.size))
        statEvent.monitoringData = event.monitoringData
        emitEvent(statEvent, np.eventCallback)
      }
    case unhandledMessage =>
  }

  def store(dataList: List[Any]) = {
    val currentTime = System.currentTimeMillis() / 1000.0d //seconds
    val out = ListBuffer.empty[Int]
    for (data <- dataList) {
      data match {
        case y: YahooDataNew =>
          if (this.storage.contains(y.campaignId.get))
            this.storage.get(y.campaignId.get).get += currentTime
          else {
            this.storage += (y.campaignId.get -> ListBuffer.empty[Double])
            this.storage.get(y.campaignId.get).get += currentTime
          }
          val threshold = currentTime - this.query.windowSize
          while (this.storage.get(y.campaignId.get).get.size > 0 & this.storage.get(y.campaignId.get).get.head < threshold)
            this.storage += (y.campaignId.get -> this.storage.get(y.campaignId.get).get.tail)
          out += y.campaignId.get
      }
    }
    out.toList
  }

}
