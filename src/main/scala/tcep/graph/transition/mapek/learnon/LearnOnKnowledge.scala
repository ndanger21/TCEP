package tcep.graph.transition.mapek.learnon

import akka.actor.ActorRef
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import org.cardygan.config.Config
import tcep.data.Queries._
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.graph.transition.KnowledgeComponent
import tcep.graph.transition.MAPEK.GetPlacementStrategyName
import tcep.graph.transition.mapek.contrast.ContrastMAPEK.{GetCFM, GetContextData, GetOperatorTreeDepth, MonitoringDataUpdate}
import tcep.graph.transition.mapek.contrast.{CFM, ContrastKnowledge}
import tcep.graph.transition.mapek.learnon.LearningModelMessages.Receive
import tcep.placement.PlacementStrategy

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

class LearnOnKnowledge(mapek: LearnOnMAPEK, transitionConfig: TransitionConfig, query: Query, currentPlacementStrategy: PlacementStrategy)
  extends ContrastKnowledge(mapek, query, transitionConfig, currentPlacementStrategy) {

  val transitionsEnabled = ConfigFactory.load().getBoolean("constants.mapek.transitions-enabled")

  /**
    * Main functionality is copied from ContrastKnowledge. Only addition is the LearningModel object
    * and functionality to forward context data to the learning model.
    */

  override def updateContextData(data: Map[String, AnyVal]): Unit = {
    super.updateContextData(data)
    if (this.transitionsEnabled) {
      for {
        strategy <- (this.self ? GetPlacementStrategyName).mapTo[String]
        update <- (this.mapek.learningModel ? Receive(cfm, this.contextData, strategy, this.requirements.toList)).mapTo[Boolean]
      } yield {
        //log.info(s"Context Updated! Current placement is: ${strategy} and sending context to Learning Model succeeded: $update")
      }
    }
  }

}
