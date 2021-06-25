package tcep.graph.transition.mapek.learnon

import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import tcep.data.Queries._
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.transition.MAPEK.GetPlacementStrategyName
import tcep.graph.transition.mapek.contrast.ContrastKnowledge
import tcep.graph.transition.mapek.learnon.LearningModelMessages.Receive

class LearnOnKnowledge(mapek: LearnOnMAPEK, transitionConfig: TransitionConfig, query: Query, currentPlacementStrategy: String)
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
