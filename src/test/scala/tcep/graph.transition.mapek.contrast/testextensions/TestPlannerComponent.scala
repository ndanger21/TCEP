package tcep.graph.transition.mapek.contrast.testextensions

import contrast.testextensions.{TestCalculateOptimalSystemConfig, TestExtractModelFromFile}
import org.cardygan.config.Config
import org.cardygan.fm.FM
import org.coala.model.PerformanceInfluenceModel
import tcep.data.Queries.Requirement
import tcep.graph.transition.mapek.contrast.{CFM, ContrastMAPEK, ContrastPlanner, PlannerHelper}

/**
  * @author Niels
  * @param mapek
  */
class TestPlannerComponent(mapek: ContrastMAPEK) extends ContrastPlanner(mapek) {

  override def receive: Receive = {

    case TestExtractModelFromFile(cfm: FM, path: String) => sender() ! PlannerHelper.extractModelFromFile(cfm, path)
    case TestCalculateOptimalSystemConfig(
    cfm: CFM, contextConfig: Config, performanceModel: PerformanceInfluenceModel, requirements: Set[Requirement]) =>
      val config = PlannerHelper.calculateOptimalSystemConfig(cfm, contextConfig, performanceModel, requirements)
      sender() ! config
  }
}