package tcep.graph.transition.mapek.contrast

import org.cardygan.config.Config
import org.cardygan.config.util.ConfigUtil
import org.cardygan.fm.impl.IntImpl
import org.cardygan.fm.util.FmUtil
import org.cardygan.fm.{FM, Feature}
import org.coala.model.PerformanceInfluenceModel
import org.coala.util.FmUtils
import org.scalatest.FunSuite
import tcep.data.Queries.{LatencyRequirement, MessageHopsRequirement}
import tcep.dsl.Dsl._
import tcep.graph.transition.mapek.contrast.FmNames._

import scala.collection.JavaConverters._


/**
  * Created by Niels
  */
class RequirementCheckerTest extends FunSuite {

  def initRequirementChecker(modelPath: String = "/performanceModels/simpleModel.log", blacklist: List[String] = List()): RequirementChecker = {
    val cfmClass: CFM = new CFM()
    val cfm: FM = cfmClass.getFM
    var contextData = TestUtils.generateMaxedContextData(cfmClass)
    contextData += (GINI_CONNECTION_DEGREE_1_HOP -> 0.2d )
    contextData += (GINI_CONNECTION_DEGREE_2_HOPS -> 0.3)
    //contextData += (BASE_LATENCY -> 30.0d)
    contextData += (NODE_TO_OP_RATIO -> 0.3d)
    val contextConfig: Config = cfmClass.getCurrentContextConfig(contextData)
    val testModel = PlannerHelper.extractModelFromFile(cfm, modelPath).get
    val latencyModel = PlannerHelper.extractModelFromFile(cfm, "/performanceModels/latencyModel.log").get
    val loadModel = PlannerHelper.extractModelFromFile(cfm, "/performanceModels/loadModel.log").get
    val hopsModel = PlannerHelper.extractModelFromFile(cfm, "/performanceModels/msgHopsModel.log").get
    //val overheadModel = PlannerHelper.extractModelFromFile(cfm, "/performanceModels/msgOverheadModel.log").get
    val perfModels: Map[Symbol, PerformanceInfluenceModel] = Map(
      'testModel -> testModel, 'latency -> latencyModel,
      'load -> loadModel, 'hops -> hopsModel/*, 'overhead -> overheadModel*/)
    new RequirementChecker(cfm, contextConfig, blacklist, perfModels)
  }

  test("compareHelper should return true if requirement holds, false if not") {
    val uut: RequirementChecker = initRequirementChecker()

    val reqEQ: LatencyRequirement = latency === timespan(1000.milliseconds) otherwise Option.empty
    val reqNEQ: LatencyRequirement = latency =!= timespan(1000.milliseconds) otherwise Option.empty
    val reqGT: LatencyRequirement = latency > timespan(1000.milliseconds) otherwise Option.empty
    val reqGEQ: LatencyRequirement = latency >= timespan(1000.milliseconds) otherwise Option.empty
    val reqLT: LatencyRequirement = latency < timespan(1000.milliseconds) otherwise Option.empty
    val reqLEQ: LatencyRequirement = latency <= timespan(1000.milliseconds) otherwise Option.empty

    assert(uut.compareHelper(reqEQ.latency.toMillis, reqEQ.operator, 1000.0d))
    assert(!uut.compareHelper(reqEQ.latency.toMillis, reqEQ.operator, 2000.0d))
    assert(uut.compareHelper(reqNEQ.latency.toMillis, reqNEQ.operator, 2000.0d))
    assert(!uut.compareHelper(reqNEQ.latency.toMillis, reqNEQ.operator, 1000.0d))
    assert(uut.compareHelper(reqGT.latency.toMillis, reqGT.operator, 2000.0d))
    assert(!uut.compareHelper(reqGT.latency.toMillis, reqGT.operator, 200.0d))
    assert(uut.compareHelper(reqGEQ.latency.toMillis, reqGEQ.operator, 1000.0d))
    assert(!uut.compareHelper(reqGEQ.latency.toMillis, reqGEQ.operator, 999.0d))
    assert(uut.compareHelper(reqLT.latency.toMillis, reqLT.operator, 999.0d))
    assert(!uut.compareHelper(reqLT.latency.toMillis, reqLT.operator, 1000.0d))
    assert(uut.compareHelper(reqLEQ.latency.toMillis, reqLEQ.operator, 1000.0d))
    assert(!uut.compareHelper(reqLEQ.latency.toMillis, reqLEQ.operator, 1001.0d))
  }

  test("excludeFeatureFromConfiguration should add a cross-tree constraint excluding the feature") {
    val uut: RequirementChecker = initRequirementChecker()

    val starksFeature: Feature = FmUtil.findFeatureByName(uut.cfm, FmNames.STARKS).get
     uut.excludeFeatureFromConfiguration(uut.cfm, starksFeature)
    //println(uut.cfm.getCrossTreeConstraints.asScala.toList)
    assert(uut.cfm.getCrossTreeConstraints.asScala.toList.size == 1)
    assert(uut.cfm.getCrossTreeConstraints.asScala.toList.head.getSource.getName == FmNames.ROOT)
    assert(uut.cfm.getCrossTreeConstraints.asScala.toList.head.getTarget.getName == FmNames.STARKS)
    assert(uut.cfm.getCrossTreeConstraints.asScala.toList.head.getType.getName == "Exclude")
  }

  test("excludeFeatureFromConfiguration excludes all features on a blacklist") {
    val uut: RequirementChecker = initRequirementChecker(blacklist = List(FmNames.RANDOM, FmNames.MOBILITY_TOLERANT))
    val result = uut.cfm.getCrossTreeConstraints.asScala.toList
    assert(result.exists(_.getTarget.getName == FmNames.RANDOM))
    assert(result.exists(_.getTarget.getName == FmNames.MOBILITY_TOLERANT))
    assert(result.size == 2)

    uut.cfm.getCrossTreeConstraints.clear()
    assert(result.size == 2)
    assert(uut.cfm.getCrossTreeConstraints.size == 0)
    uut.excludeUnfitConfigurations(Set(latency === timespan(1000.milliseconds) otherwise None))
    //println(uut.cfm.getCrossTreeConstraints.asScala.map(c => c.getSource + " -> " + c.getTarget).mkString("\n"))
    assert(uut.cfm.getCrossTreeConstraints.size == FmNames.allPlacementAlgorithms.size)
  }

  test("generateSystemConfigs generates all possible system configurations") {
    val uut: RequirementChecker = initRequirementChecker()

    val systemConfigs: List[Config] = uut.generateSystemConfigs
    for(config <- systemConfigs) {

      assert(ConfigUtil.getAllInstances(config).size > 0)
      val featureMap = FmUtils.getFlatConfig(config).getFeatures

      FmNames.allPlacementAlgorithms.foreach(name =>
        assert(featureMap.keySet.asScala.map(f => f.getName).contains(name)))
      // assert that only one placement algorithm feature is active
      assert(featureMap.asScala
          .filter(e => FmNames.allPlacementAlgorithms.contains(e._1.getName))
          .values.toList.count(b => b) == 1)
    }
  }

  test("predictMetric correctly computes the perfModel value") {
    val uut: RequirementChecker = initRequirementChecker() // use simpleModel.log

    val starksSystemConfig: Config = uut.generateSystemConfigs
      .filter(config => ConfigUtil.getFeatureInstanceByName(config.getRoot, FmNames.STARKS) != null).head
    val pietzuchSystemConfig: Config = uut.generateSystemConfigs
      .filter(config => ConfigUtil.getFeatureInstanceByName(config.getRoot, FmNames.RELAXATION) != null).head

    // 5 * root + 1 * fsMDCEP * fcLinkChanges + 1.0 * fsRelaxation * fcNodeCount + 1001 * fsRandom + 1111 * fsProducerConsumer + 2000 * fsRizou
    val perfModel = uut.perfModels('testModel)
    val linkChangesUpperBound = FmUtil.findAttributeByName(uut.cfm, FmNames.LINK_CHANGES).get.getDomain.asInstanceOf[IntImpl].getBounds.getUb
    val nodeUpperBound = FmUtil.findAttributeByName(uut.cfm, FmNames.NODECOUNT).get.getDomain.asInstanceOf[IntImpl].getBounds.getUb
    val predictedValueWithStarksConfig: Double = uut.predictMetricFromModel(starksSystemConfig, performanceModel = perfModel)
    val predictedValueWithPietzuchConfig: Double = uut.predictMetricFromModel(pietzuchSystemConfig, performanceModel = perfModel)
    assert(predictedValueWithStarksConfig == 5 + 1 * linkChangesUpperBound)
    assert(predictedValueWithPietzuchConfig == 5 + 1 * nodeUpperBound)
  }

  // all configurations with placement algorithms that would violate the requirement should be excluded
  test("checkRequirementViolations (value <= 1000) should add two cross-tree constraints for Random and MobilityTolerant") {
    val uut: RequirementChecker = initRequirementChecker()

    val reqLEQ: LatencyRequirement = latency <= timespan(1000.milliseconds) otherwise Option.empty
    uut.checkRequirementViolations(reqLEQ.latency.toMillis.toDouble, reqLEQ.operator, uut.perfModels('testModel))
    //5 * root + 1 * fsMDCEP * fcloadvariance * fcloadvariance + 1.0 * fsRelaxation * fcNodeCount + 1001 * fsRandom + 1111 * fsProducerConsumer
    // Pietzuch should be excluded, Starks and Random not

    assert(uut.cfm.getCrossTreeConstraints.asScala.toList.head.getSource.getName == FmNames.ROOT)
    assert(uut.cfm.getCrossTreeConstraints.asScala.toList.exists(c => c.getTarget.getName == FmNames.RANDOM) &&
      uut.cfm.getCrossTreeConstraints.asScala.toList.exists(c => c.getTarget.getName == FmNames.MOBILITY_TOLERANT))
    assert(uut.cfm.getCrossTreeConstraints.asScala.toList.head.getType.getName == "Exclude")
  }

  test("checkRequirementViolations (value >= 1000) should add two cross-tree constraints (Pietzuch and Starks") {
    val uut: RequirementChecker = initRequirementChecker()

    val reqGEQ: LatencyRequirement = latency >= timespan(1000.milliseconds) otherwise Option.empty
    uut.checkRequirementViolations(reqGEQ.latency.toMillis.toDouble, reqGEQ.operator, uut.perfModels('testModel))

    // Pietzuch should not be excluded, Starks and Random should be
    assert(uut.cfm.getCrossTreeConstraints.asScala.toList.head.getSource.getName == FmNames.ROOT)
    assert(uut.cfm.getCrossTreeConstraints.asScala.toList.exists(c => c.getTarget.getName == FmNames.RELAXATION) &&
      uut.cfm.getCrossTreeConstraints.asScala.toList.exists(c => c.getTarget.getName == FmNames.STARKS))
    assert(uut.cfm.getCrossTreeConstraints.asScala.toList.head.getType.getName == "Exclude")
  }

  test("excludeUnfitConfigurations should exclude all but MobilityTolerant") {
    val uut: RequirementChecker = initRequirementChecker("/performanceModels/msgHopsModel.log")

    val reqGEQ: MessageHopsRequirement = hops > 1 otherwise Option.empty
    uut.excludeUnfitConfigurations(Set(reqGEQ))
    val constrainedCFM = uut.cfm
    // Random should be excluded, Starks and Pietzuch not
    val result = constrainedCFM.getCrossTreeConstraints.asScala.toList
    assert(result.size >= 4)
    assert(constrainedCFM.getCrossTreeConstraints.asScala.toList.head.getSource.getName == FmNames.ROOT)
    assert(result.exists(_.getTarget.getName == FmNames.RANDOM) && result.exists(_.getTarget.getName == FmNames.STARKS) &&
      result.exists(_.getTarget.getName == FmNames.RELAXATION) && result.exists(_.getTarget.getName == FmNames.RIZOU))
    assert(constrainedCFM.getCrossTreeConstraints.asScala.toList.head.getType.getName == "Exclude")
  }
  /*
  test("estimatePerformancePerAlgorithm") {
    val uut: RequirementChecker = initRequirementChecker()
    val res = uut.estimatePerformancePerAlgorithm(uut.contextConfig)
    println(res)
    val str = res.flatMap(a => a._2.map(m => s"${m._2}")).mkString(",")
    println(res.keys)
  }
  */
  /*
  // these test make no assertions, they just print the predicted value under maximum context feature values
  test("predict latencyModel values") {
    val uut: RequirementChecker = initRequirementChecker("/performanceModels/latencyModel.log")
    val systemConfigs = uut.generateNamedSystemConfigs
    val perfModel = uut.perfModels('testModel)
    val res = systemConfigs.map(e => s"${e._1}" -> s"${uut.predictMetricFromModel(e._2, performanceModel = perfModel)}")
    println("\n latency value predictions:")
    println(res.mkString("\n"))
  }


  test("predict loadModel values") {
    val uut: RequirementChecker = initRequirementChecker("/performanceModels/loadModel.log")
    val systemConfigs = uut.generateNamedSystemConfigs
    val perfModel = uut.perfModels('testModel)
    val res = systemConfigs.map(e => s"${e._1}" -> s"${uut.predictMetricFromModel(e._2, performanceModel = perfModel)}")
    println("\n load value predictions:")
    println(res.mkString("\n"))
  }

  test("predict hopsModel values") {
    val uut: RequirementChecker = initRequirementChecker("/performanceModels/msgHopsModel.log")
    val systemConfigs = uut.generateNamedSystemConfigs
    val perfModel = uut.perfModels('testModel)
    val res = systemConfigs.map(e => s"${e._1}" -> s"${uut.predictMetricFromModel(e._2, performanceModel = perfModel)}")
    println("\n hops value predictions:")
    println(res.mkString("\n"))
  }

  test("predict OverheadModel values") {
    val uut: RequirementChecker = initRequirementChecker("/performanceModels/msgOverheadModel.log")
    val systemConfigs = uut.generateNamedSystemConfigs
    val perfModel = uut.perfModels('testModel)
    val res = systemConfigs.map(e => s"${e._1}" -> s"${uut.predictMetricFromModel(e._2, performanceModel = perfModel)}")
    println("\n msgOverhead value predictions:")
    println(res.mkString("\n"))
  }
  */
}
