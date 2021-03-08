package tcep.graph.nodes.traits

import akka.actor.{ActorRef, PoisonPill}
import tcep.data.Events.{DependenciesRequest, DependenciesResponse}
import tcep.data.Queries.LeafQuery
import tcep.graph.nodes.ShutDown
import tcep.graph.nodes.traits.Node.UnSubscribe
import tcep.graph.transition.TransitionStats
import tcep.placement.PlacementStrategy

/**
  * Handling of [[tcep.data.Queries.LeafQuery]] is done by LeafNode
  **/
trait LeafNode extends Node {

  val query: LeafQuery

  override def childNodeReceive: Receive = {
    case DependenciesRequest => sender ! DependenciesResponse(Seq.empty)
    case ShutDown() => {
      getParentActors().foreach(_ ! UnSubscribe())
      self ! PoisonPill
    }
  }

  override def handleTransitionRequest(requester: ActorRef, algorithm: PlacementStrategy, stats: TransitionStats): Unit =
    executeTransition(requester, algorithm, stats)
}
