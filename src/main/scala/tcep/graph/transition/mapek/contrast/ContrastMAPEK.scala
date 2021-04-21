package tcep.graph.transition.mapek.contrast

import java.util.concurrent.TimeUnit

import akka.actor.{ActorContext, ActorRef, Props}
import com.typesafe.config.ConfigFactory
import org.cardygan.config.Config
import tcep.data.Queries.{Query, Requirement}
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.transition.MAPEK
import tcep.placement.PlacementStrategy

import scala.concurrent.duration.FiniteDuration

/**
  * Created by raheel
  * on 02/10/2017.
  * refactored by Niels
  *
  * Holds the 5 components of the MAPE-K loop (monitor, analyze, plan, execute, knowledge)
  * that control the self-adaptation of the system in reaction to context changes
  * @param context akka actorContext
  * @param query currently active CEP query
  * @param fixedSimulationProperties parameters fixed for a simulation run
  */
class ContrastMAPEK(context: ActorContext, query: Query, mode: TransitionConfig, startingPlacementStrategy: PlacementStrategy,
                    fixedSimulationProperties: Map[Symbol, Int], consumer: ActorRef) extends MAPEK(context) {

  val monitor: ActorRef = context.actorOf(Props(new ContrastMonitor(this, consumer, fixedSimulationProperties)))
  val analyzer: ActorRef = context.actorOf(Props(new ContrastAnalyzer(this)))
  val planner: ActorRef = context.actorOf(Props(new ContrastPlanner(this)))
  val executor: ActorRef = context.actorOf(Props(new ContrastExecutor( this)))
  val knowledge: ActorRef = context.actorOf(Props(new ContrastKnowledge(this, query, mode, startingPlacementStrategy)), "knowledge")

}

object ContrastMAPEK {
  case class MonitoringDataUpdate(contextData: Map[String, AnyVal])
  case class RunPlanner(cfm: CFM, config: Config, currentLatency: Double, qosRequirements: Set[Requirement])
  case class OptimalSystemConfiguration(optimalSystemConfig: Config)
  case object GetOperatorTreeDepth
  case object GetCFM
  case object GetContextData
}