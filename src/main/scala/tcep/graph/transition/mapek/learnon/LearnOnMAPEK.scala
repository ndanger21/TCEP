package tcep.graph.transition.mapek.learnon

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Props}
import com.typesafe.config.ConfigFactory
import tcep.data.Queries.Query
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.graph.transition.MAPEK
import tcep.graph.transition.mapek.contrast.{CFM, ContrastMonitor}
import tcep.placement.PlacementStrategy

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

class LearnOnMAPEK(context: ActorContext, transitionConfig: TransitionConfig, query: Query, currentPlacementStrategy: PlacementStrategy, fixedSimulationProperties: Map[Symbol, Int], consumer: ActorRef, pimPaths: (String, String))
  extends MAPEK(context) {

  val monitor: ActorRef = context.actorOf(Props(new LearnOnMonitor(this, consumer, fixedSimulationProperties)))
  val analyzer: ActorRef = context.actorOf(Props(new LearnOnAnalyzer(this)))
  val planner: ActorRef = context.actorOf(Props(new LearnOnPlanner(this)))
  val executor: ActorRef = context.actorOf(Props(new LearnOnExecutor(this)))
  val knowledge: ActorRef = context.actorOf(Props(new LearnOnKnowledge(this, transitionConfig, query, currentPlacementStrategy)).withDispatcher("blocking-io-dispatcher"))
  val learningModel: ActorRef = ConfigFactory.load().getString("constants.mapek.learning-model").toLowerCase() match {
    case "learnon" => context.actorOf(Props(new LearnOn(new CFM(this), pimPaths._1, pimPaths._2)))
    case "lightweight" => context.actorOf(Props(new Lightweight()))
    case "rl" => context.actorOf(Props(new ModelRL(this)))
  }

  val rlUpdater: ActorRef = context.actorOf(Props(new ModelRLQUpdater(this.learningModel)))
}
