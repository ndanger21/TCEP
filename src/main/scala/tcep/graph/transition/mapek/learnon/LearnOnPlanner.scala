package tcep.graph.transition.mapek.learnon

import akka.actor.Cancellable
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import org.cardygan.config.Config
import tcep.data.Queries.Requirement
import tcep.graph.transition.MAPEK.{ExecuteTransition, GetPlacementStrategyName}
import tcep.graph.transition.PlannerComponent
import tcep.graph.transition.mapek.contrast.CFM
import tcep.graph.transition.mapek.contrast.ContrastMAPEK.{GetContextData, RunPlanner}
import tcep.graph.transition.mapek.learnon.LearningModelMessages.GetMechanism

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

class LearnOnPlanner(mapek: LearnOnMAPEK) extends PlannerComponent(mapek){

  var sleepingScheduler: Cancellable = _
  val blacklisted_ops = ConfigFactory.load().getStringList("constants.mapek.blacklisted-algorithms").asScala.toList.map(op => op.substring(2, op.length).toLowerCase)

  override def preStart(): Unit = {
    super.preStart()
    log.info("Starting LearnOn Planner")
  }

  override def receive: Receive = super.receive orElse {
    case RunPlanner(cfm: CFM, config: Config, currentLatency: Double, qosRequirements: Set[Requirement]) =>
      log.info("RunPlanner message received!")
      for {
        current <- (mapek.knowledge ? GetPlacementStrategyName).mapTo[String]
        currentContext <- (mapek.knowledge ? GetContextData).mapTo[Map[String, AnyVal]]
        strategy <- (mapek.learningModel ? GetMechanism(cfm, config, currentLatency, qosRequirements, currentContext, current)).mapTo[String]
      } yield {
        log.info(s"Received algorithm: $strategy")
        if (!current.equals(strategy) && !strategy.equals("NoTransition") && !this.blacklisted_ops.contains(strategy.toLowerCase)) {
          mapek.executor ! ExecuteTransition(strategy)
        } else {
          log.info("Not transitioning because already in OP or no valid OP found.")
        }
      }
  }
}