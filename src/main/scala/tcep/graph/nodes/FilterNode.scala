package tcep.graph.nodes

import tcep.data.Events._
import tcep.data.Queries._
import tcep.graph.QueryGraph
import tcep.graph.nodes.traits.Node.NodeProperties
import tcep.graph.nodes.traits._
import tcep.placement.HostInfo

/**
  * Handling of [[tcep.data.Queries.FilterQuery]] is done by FilterNode.
  *
  * @see [[QueryGraph]]
  **/

case class FilterNode(query: FilterQuery, hostInfo: HostInfo, np: NodeProperties) extends UnaryNode {

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event if (np.parentActor.contains(sender())) =>
      event.updateArrivalTimestamp()
      if (query.cond(event))
        emitEvent(event, np.eventCallback)


    case unhandledMessage =>
  }

}

