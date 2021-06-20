package tcep.graph.nodes

import akka.actor.{ActorLogging, ActorRef}
import tcep.data.Events._
import tcep.data.Queries._
import tcep.graph.nodes.traits._
import tcep.graph.{CreatedCallback, EventCallback, QueryGraph}
import tcep.placement.HostInfo

/**
  * Handling of [[tcep.data.Queries.StreamQuery]] is done by StreamNode.
  *
  * @see [[QueryGraph]]
  **/
case class StreamNode(transitionConfig: TransitionConfig,
                      hostInfo: HostInfo,
                      backupMode: Boolean,
                      mainNode: Option[ActorRef],
                      query: StreamQuery,
                      createdCallback: Option[CreatedCallback],
                      eventCallback: Option[EventCallback],
                      isRootOperator: Boolean,
                      publisher: ActorRef*) extends LeafNode with ActorLogging {

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event if sender().equals(publisher.head) =>
      event.updateArrivalTimestamp()
      val s = sender()
      if(s.equals(publisher.head)) {
        emitEvent(event, eventCallback)
      }
    case unhandledMessage =>
  }

  override def getParentActors(): List[ActorRef] = publisher.toList
}

