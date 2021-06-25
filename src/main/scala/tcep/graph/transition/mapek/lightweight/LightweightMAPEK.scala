package tcep.graph.transition.mapek.lightweight

import akka.actor.{ActorContext, ActorRef, Props}
import tcep.data.Queries.Query
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.transition.MAPEK

class LightweightMAPEK(context: ActorContext, query: Query,transitionConfig: TransitionConfig, startingPlacementStrategy: String, consumer: ActorRef) extends MAPEK(context) {

  val monitor: ActorRef = context.actorOf(Props(new LightweightMonitor(this)))
  val analyzer: ActorRef = context.actorOf(Props(new LightweightAnalyzer(this)))
  val planner: ActorRef = context.actorOf(Props(new LightweightPlanner(this)))
  val executor: ActorRef = context.actorOf(Props(new LightweightExecutor(this)))
  val knowledge: ActorRef = context.actorOf(Props(new LightweightKnowledge(this, query, transitionConfig, startingPlacementStrategy, consumer)))
}

object LightweightMAPEK {
  case class SetLastTransitionEnd(time: Long)
  case object GetConsumer
}