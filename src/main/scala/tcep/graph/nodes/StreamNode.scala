package tcep.graph.nodes

import akka.actor.{ActorLogging, ActorRef}
import tcep.data.Events._
import tcep.data.Queries._
import tcep.graph.QueryGraph
import tcep.graph.nodes.traits.Node.NodeProperties
import tcep.graph.nodes.traits._
import tcep.placement.HostInfo

/**
  * Handling of [[tcep.data.Queries.StreamQuery]] is done by StreamNode.
  *
  * @see [[QueryGraph]]
  **/
case class StreamNode(query: StreamQuery, hostInfo: HostInfo, np: NodeProperties) extends LeafNode with ActorLogging {

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event if sender().equals(np.parentActor.head) =>
      event.updateArrivalTimestamp()
      emitEvent(event, np.eventCallback)

    case unhandledMessage =>
  }

  override def getParentActors(): List[ActorRef] = np.parentActor.toList
}

