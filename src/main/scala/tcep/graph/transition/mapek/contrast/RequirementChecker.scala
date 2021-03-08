package tcep.graph.transition.mapek.contrast

import java.util.NoSuchElementException

import org.cardygan.config.impl.{IntAttrInstImpl, RealAttrInstImpl}
import org.cardygan.config.util.{ConfigFactoryUtil, ConfigUtil}
import org.cardygan.config.{AttrInst, Config, Instance}
import org.cardygan.fm._
import org.cardygan.fm.util.FmUtil
import org.coala.model.PerformanceInfluenceModel
import org.slf4j.LoggerFactory
import tcep.data.Queries._
import tcep.graph.transition.mapek.contrast.FmNames._

import scala.collection.JavaConverters._

/**
  * Created by Niels on 14.04.2018.
  *
  * utility class for using performance influence model estimates to determine whether QoS requirements will be violated by system configurations
  * and excluding unfit system configurations
  * @param cfm the CFM to use
  * @param perfModels the list of performance influence models (one for each QoS metric that can have a requirement)
  * @param contextConfig the state of the current context, as a configuration of the context side of the CFM
  */
class RequirementChecker(val cfm: FM, val contextConfig: Config, blacklistedFeatures: List[String] = List(), var perfModels: Map[Symbol, PerformanceInfluenceModel] = Map()) {

  val log = LoggerFactory.getLogger(getClass)
  if(perfModels.isEmpty) {
    val latencyModel = PlannerHelper.extractModelFromFile(cfm, "/performanceModels/latencyModel.log").getOrElse(throw new NoSuchElementException("could not find /performanceModels/latencyModel.log"))
    val loadModel = PlannerHelper.extractModelFromFile(cfm, "/performanceModels/loadModel.log").getOrElse(throw new NoSuchElementException("could not find /performanceModels/loadModel.log"))
    val hopsModel = PlannerHelper.extractModelFromFile(cfm, "/performanceModels/msgHopsModel.log").getOrElse(throw new NoSuchElementException("could not find /performanceModels/msgHopsModel.log"))
    perfModels = Map('latency -> latencyModel, 'load -> loadModel, 'hops -> hopsModel)
  }

  var requirementHolds: Map[Requirement, Boolean] = Map()
  blacklistedFeatures.foreach(name => {
    val feature = FmUtil.findFeatureByName(this.cfm, name).orElseThrow(() => new NoSuchElementException(s"could not find feature $name in cfm!"))
    this.excludeFeatureFromConfiguration(feature = feature)
  })

  /**
    * checks if any of the given requirements is predicted to be violated under any possible system configuration and
    * the given context configuration; if yes, the respective system configurations are excluded
    * @param qosRequirements the set of QoS requirements
    * @return the CFM with additional cross-tree constraints excluding any system features that cannot fulfill the requirements
    */
  def excludeUnfitConfigurations(qosRequirements: Set[Requirement]): Map[Requirement, Boolean] = {

    log.info(s"excludeUnfitConfigurations - called with reqs: ${qosRequirements} and blacklist $blacklistedFeatures")
    blacklistedFeatures.foreach(name => {
      val feature = FmUtil.findFeatureByName(this.cfm, name).orElseThrow(() => new NoSuchElementException(s"could not find feature $name in cfm!"))
      this.excludeFeatureFromConfiguration(feature = feature)
    })
    requirementHolds = requirementHolds.empty

    for(req <- qosRequirements) {
      req match {
        case latencyReq: LatencyRequirement =>
          requirementHolds += (req -> checkRequirementViolations(latencyReq.latency.toMillis.toDouble, latencyReq.operator, perfModels('latency)))
        case loadReq: LoadRequirement =>
          requirementHolds += (req -> checkRequirementViolations(loadReq.machineLoad.value, loadReq.operator, perfModels('load)))
        case hopsReq: MessageHopsRequirement =>
          requirementHolds += (req -> checkRequirementViolations(hopsReq.requirement.toDouble, hopsReq.operator, perfModels('hops)))
        case _ => log.error("requirement not supported by optimization model! " + req)
      }
    }
    log.info(s"requirements that are estimated to be fulfilled by at least one algorithm \n ${requirementHolds.mkString("\n")}")
    requirementHolds
  }

  /**
    * checks if the requirement is predicted to be not fulfilled;
    * excludes all system configurations (placement algorithms) that are incapable of fulfilling the qos requirement
    * @param reqValue the requirement value to be upheld
    * @param operator the comparison operator from the requirement
    * @param perfModel the performance model used for predicting the performance value of a configuration
    * @return true if at least one system config is estimated to be fulfilling the requirement, false if not
    */
  def checkRequirementViolations(reqValue: Double, operator: Operator, perfModel: PerformanceInfluenceModel): Boolean = {

    var requirementHolds = false
    for(systemConfig <- generateSystemConfigs) {

      val predictedValue: Double = predictMetricFromModel(systemConfig = systemConfig, performanceModel = perfModel)
      val activeAlgorithmFeature: Feature = FmUtil.findFeatureByName(cfm, ConfigUtil.getLeafs(systemConfig, systemConfig.getRoot).asScala.toList.head.getName).get()
      log.info(s"checkRequirementViolations - predicted value for system config with ${activeAlgorithmFeature.getName}: $predictedValue")
      // TODO if system configurations are more complex than just one placement algorithm,
      // (i.e. a combination of features) excluding this combination must be done by a direct ILP constraint instead of just a cross-tree constraint
      if (!compareHelper(reqValue, operator, predictedValue)) {
        log.info(s"checkRequirementViolations - excluding $activeAlgorithmFeature since its predicted performance was insufficient")
        excludeFeatureFromConfiguration(cfm, activeAlgorithmFeature)
      } else requirementHolds = true
    }
    requirementHolds
  }

  /**
    * predicts the value of a metric using the given context config, system feature and performance model
    * @param systemConfig the system configuration
    * @param performanceModel the model to predict the metric
    * @return the predicted value of the metric
    */
    def predictMetricFromModel(systemConfig: Config, contextConfig: Config = contextConfig, performanceModel: PerformanceInfluenceModel): Double = {

      val terms = performanceModel.getTerms.asScala.toList
      var predictedValue = 0.0d

      for(term <- terms) {
        // initialize with term weight
        var termValue = term.getWeight
        // if a feature is present in config, multiply by 1, if not, by 0
        term.getFeatures.keySet().forEach(f =>
          if(ConfigUtil.getFeatureInstanceByName(contextConfig.getRoot, f.getName) == null &&
             ConfigUtil.getFeatureInstanceByName(systemConfig.getRoot, f.getName) == null)
            termValue = termValue * 0.0d
          else termValue = termValue * 1.0d
        )
        // multiply by attributes' values
        for(attributeEntry <- term.getAttributes.asScala.toMap) {

          val attribute = attributeEntry._1
          val occurrences = attributeEntry._2.toDouble
          val attrInstance: AttrInst =
            (ConfigUtil.getAttrInstances(contextConfig, contextConfig.getRoot).asScala ++
            ConfigUtil.getAttrInstances(systemConfig, systemConfig.getRoot).asScala)
            .filter(a => a.getName == attribute.getName).head
          val attributeValue = attrInstance match {
            case realAttr: RealAttrInstImpl => realAttr.getVal
            case intAttr: IntAttrInstImpl => intAttr.getVal
          }

          termValue = termValue * math.pow(attributeValue, occurrences)
        }

        predictedValue += termValue
      }
      //log.debug(s"predictMetric() - ${predictedValue} \n ${terms}")
      predictedValue
    }

  /**
    * generates all system configurations; each config has only one placement algorithm active
    * Note: if system configurations become more complex than just one active placement algorithm (e.g. query tree optimization enabled/disabled), this function has to be adapted
    * @return a list of all possible configurations of the system side of the CFM
    */
  def generateSystemConfigs: List[Config] = {

      val algos: List[Feature] = FmUtil.getFeatureByName(cfm, FmNames.PLACEMENT_ALGORITHM).get().getChildren.asScala.toList.filter(f => !blacklistedFeatures.contains(f.getName))
      var res: List[Config] = List()
      algos.foreach(a => {
        val systemConfig: Config = generateSystemConfig(a)
        res = systemConfig :: res
      })

      res
  }

  /**
    * generate a named map of all algorithm system configs
    * @return  a map of all algorithms and their respective system configs
    */
  def generateNamedSystemConfigs: Map[String, Config] = {

    val algos: List[Feature] = FmUtil.getFeatureByName(cfm, FmNames.PLACEMENT_ALGORITHM).get().getChildren.asScala.toList
    algos.map(f => f.getName -> generateSystemConfig(f)).toMap
  }

  /**
    * generates a Config of the system side of the CFM with the passed algorithm feature active
    * @param algorithmFeature the feature to be active/instantiated in the Config
    * @return the system Config
    */
  def generateSystemConfig(algorithmFeature: Feature) = {
    val root: Feature = FmUtil.findFeatureByName(cfm, ROOT).get
    val systemFeatureGroup: Feature = FmUtil.findFeatureByName(cfm, SYSTEM).get
    val mechanismFeatureGroup: Feature = FmUtil.findFeatureByName(cfm, MECHANISMS).get
    val placementAlgorithmFeatureGroup: Feature = FmUtil.findFeatureByName(cfm, PLACEMENT_ALGORITHM).get

    val systemConfig: Config = ConfigFactoryUtil.createConfig(cfm)
    val systemRootInstance: Instance = ConfigFactoryUtil.createInstance(root.getName, null, root)
    systemConfig.setRoot(systemRootInstance)
    val systemGroupInstance: Instance = ConfigFactoryUtil.createInstance(systemFeatureGroup.getName, systemRootInstance, systemFeatureGroup)
    val mechanismGroupInstance: Instance = ConfigFactoryUtil.createInstance(mechanismFeatureGroup.getName, systemGroupInstance, mechanismFeatureGroup)
    val placementAlgorithmGroupInstance: Instance = ConfigFactoryUtil.createInstance(placementAlgorithmFeatureGroup.getName, mechanismGroupInstance, placementAlgorithmFeatureGroup)
    val algoInstance: Instance = ConfigFactoryUtil.createInstance(algorithmFeature.getName, placementAlgorithmGroupInstance, algorithmFeature)
    systemConfig
  }

  /**
    * helper function to compare a requirement value to an actual value
    *
    * @param reqVal value of the requirement
    * @param op comparison operator
    * @param otherVal value to compare to
    * @return true if requirement is condition holds, false if violated
    */
  def compareHelper(reqVal: Double, op: Operator, otherVal: Double): Boolean = {
    op match {
      case Equal => reqVal == otherVal
      case NotEqual => reqVal != otherVal
      case Greater => otherVal > reqVal
      case GreaterEqual => otherVal >= reqVal
      case Smaller => otherVal < reqVal
      case SmallerEqual => otherVal <= reqVal
    }
  }

  /**
    * prevent a feature from being selected in configuration by marking it as exclusive with the root feature
    * @param cfm the cfm to modify
    * @param feature the feature to exclude
    * @return the cfm with an exclude-cross-tree constraint added that excludes the feature
    */
  def excludeFeatureFromConfiguration(cfm: FM = this.cfm, feature: Feature): FM = {

    val factory: FmFactory = FmFactory.eINSTANCE
    val cstr: CrossTreeConstraint = factory.createCrossTreeConstraint()
    cstr.setSource(cfm.getRoot)
    cstr.setTarget(feature)
    cstr.setType(CrossTreeConstraintType.EXCLUDE)
    val existingConstraints = cfm.getCrossTreeConstraints.asScala.toList
    if(!existingConstraints.exists(c => c.getSource.getName == cstr.getSource.getName && c.getTarget.getName == cstr.getTarget.getName))
      cfm.getCrossTreeConstraints.add(cstr)
    cfm
  }

  /**
    *
    * @param contextConfig current context
    * @return map of maps for every algorithm's estimated performance per metric
    */
  def estimatePerformancePerAlgorithm(contextConfig: Config): Map[String, Map[Symbol, Double]] = {

    val systemConfigs: Map[String, Config] = generateNamedSystemConfigs
    val metrics = List('latency, 'load, 'hops)
    //log.debug("estimatePerformance() - " +
      //s"\n systemConfigs: ${systemConfigs}" +
      //s"\n perfModels: ${perfModels}" +
      //s"\n contextConfig: ${contextConfig}")
    systemConfigs.map(nc => {
      val perfPerMetric: Map[Symbol, Double] = metrics.map(m =>
        m -> predictMetricFromModel(systemConfig = nc._2, contextConfig = contextConfig, perfModels(m))).toMap
      nc._1 -> perfPerMetric
    })
  }

}
