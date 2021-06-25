package contrast.testextensions

import akka.actor.{ActorContext, ActorRef, Props}
import org.cardygan.config.Config
import org.cardygan.fm.FM
import org.coala.model.PerformanceInfluenceModel
import tcep.data.Queries.{Query, Requirement}
import tcep.graph.nodes.traits.{TransitionConfig, TransitionExecutionModes, TransitionModeNames}
import tcep.graph.transition.mapek.contrast.testextensions.TestPlannerComponent
import tcep.graph.transition.mapek.contrast.{CFM, ContrastMAPEK}
import tcep.placement.sbon.PietzuchAlgorithm

/**
  * Created by Niels on 16.03.2018.
  */
class TestMAPEK(context: ActorContext, query: Query, consumer: ActorRef)
  extends ContrastMAPEK(context, query, TransitionConfig(TransitionModeNames.SMS, TransitionExecutionModes.CONCURRENT_MODE), PietzuchAlgorithm.name,
                fixedSimulationProperties = Map(), consumer)
 {

   override val monitor: ActorRef = context.actorOf(Props(new TestMonitorComponent(this, consumer)))
   override val planner: ActorRef = context.actorOf(Props(new TestPlannerComponent(this)))

}

case class TestExtractModelFromFile( cfm: FM, path: String)
case class TestCalculateOptimalSystemConfig(cfm: CFM, contextConfig: Config, performanceModel: PerformanceInfluenceModel, requirements: Set[Requirement])
case object TestUpdateContextData