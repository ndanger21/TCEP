package tcep.graph.transition.mapek

import org.cardygan.config.util.{ConfigFactoryUtil, ConfigUtil}
import org.cardygan.config.{Config, Instance}
import org.cardygan.fm.util.{FmFactoryUtil, FmUtil}
import org.cardygan.fm.{Attribute, Feature, FmFactory, Interval}
import tcep.data.Queries
import tcep.data.Queries.Query
import tcep.graph.qos.OperatorQosMonitor
import tcep.graph.qos.OperatorQosMonitor.Sample
import tcep.graph.transition.mapek.DynamicCFMNames.{ALL_FEATURES, QUERY}
import tcep.graph.transition.mapek.contrast.FmNames._
import tcep.graph.transition.mapek.contrast.{CFM, FmNames}


class DynamicCFM(rootOperator: Query) extends CFM {
  lazy val operators: List[Query] = Queries.getOperators(rootOperator)

  override def init(): Unit = {

    // root
    val root = buildGroupFeature(ROOT, null, exactlyOne, exactlyOne, buildInterval(2, 2))
    root.setName(ROOT)
    // system
    val systemGroup = buildGroupFeature(SYSTEM, root)
    val mechanismGroup = buildGroupFeature(MECHANISMS, systemGroup)
    val placementGroup = buildGroupFeature(PLACEMENT_ALGORITHM, mechanismGroup)
    FmNames.allPlacementAlgorithms.map(aName => buildBinaryFeature(aName, placementGroup))

    // context
    val contextGroup = buildGroupFeature(CONTEXT, root)
    val queryGroup = buildGroupFeature(QUERY, contextGroup)
    operators.map(op => {
      val opName = op.toString()
      // generate a feature group for each operator
      val opFeature = buildGroupFeature(opName, queryGroup)
      // with a separate attribute for each feature within that group
      ALL_FEATURES.map(f => buildNumericFeature(f + "_" + opName, opFeature, buildInterval(0, 10000), "Real"))
    })

    cfm = FmFactory.eINSTANCE.createFM()
    cfm.setRoot(root)
  }

  /**
    * @param context most recent feature samples of each operator
    * @return the current context config with the given sample values for each operator
    */
  def getCurrentContextConfigFromSamples(context: Map[Query, Sample]): Config = {
    try {
      val root: Feature = FmUtil.findFeatureByName(cfm, ROOT).get
      val contextFeatureGroup: Feature = FmUtil.findFeatureByName(cfm, CONTEXT).get
      val queryFeatureGroup: Feature = FmUtil.findFeatureByName(cfm, QUERY).get
      val systemFeatureGroup: Feature = FmUtil.findFeatureByName(cfm, SYSTEM).get
      val mechanismsGroup: Feature = FmUtil.findFeatureByName(cfm, MECHANISMS).get
      val placementAlgorithmsGroup: Feature = FmUtil.findFeatureByName(cfm, PLACEMENT_ALGORITHM).get

      val contextConfig = ConfigFactoryUtil.createConfig(cfm)
      val rootI = ConfigFactoryUtil.createInstance(root.getName, null, root)
      contextConfig.setRoot(rootI)
      val systemGroupInstance: Instance = ConfigFactoryUtil.createInstance(systemFeatureGroup.getName, rootI, systemFeatureGroup)
      val mechanismsGroupInstance: Instance = ConfigFactoryUtil.createInstance(mechanismsGroup.getName, systemGroupInstance, mechanismsGroup)
      val placementAlgorithmsGroupInstance: Instance = ConfigFactoryUtil.createInstance(placementAlgorithmsGroup.getName, mechanismsGroupInstance, placementAlgorithmsGroup)
      val contextGroupInstance: Instance = ConfigFactoryUtil.createInstance(contextFeatureGroup.getName, rootI, contextFeatureGroup)
      val queryGroupI = ConfigFactoryUtil.createInstance(queryFeatureGroup.getName, contextGroupInstance, queryFeatureGroup)
      operators.foreach(op => {
        val opName = op.toString()
        val opGroup = FmUtil.findFeatureByName(cfm, opName).get
        val opGroupI = ConfigFactoryUtil.createInstance(op.toString(), queryGroupI, opGroup)
        val sample = context.getOrElse(op, throw new IllegalArgumentException(s"missing feature sample for operator $op"))
        ALL_FEATURES.foreach(f => {
          val attr = FmUtil.findAttributeByName(cfm, f + "_" + opName)
          createAttributeInst(attr.get, opGroupI, OperatorQosMonitor.getFeatureValue(sample, f))
        })
      })
      ConfigUtil.checkConfig(contextConfig)
      contextConfig
    } catch {
      case e: Throwable =>
        log.error(s"failed to build current context config from context \n${context} \n for CFM $cfm", e)
        throw e
    }

  }

  def buildBinaryFeature(name: String, parent: Feature): Feature = FmFactoryUtil.createFeature(name, parent, 0, 1)

  def buildNumericFeature(name: String, parent: Feature, range: Interval, domainType: String): Attribute = {
    val attribute = FmFactory.eINSTANCE.createAttribute
    parent.getAttributes.add(attribute)
    attribute.setName(name)
    val domain = domainType match {
      case "Int" =>
        val domainBounds = FmFactory.eINSTANCE.createIntBounds()
        domainBounds.setLb(range.getLowerBound)
        domainBounds.setUb(range.getUpperBound)
        val d = FmFactory.eINSTANCE.createInt()
        d.setBounds(domainBounds)
        d
      case "Real" =>
        val domainBounds = FmFactory.eINSTANCE.createRealBounds()
        domainBounds.setLb(range.getLowerBound)
        domainBounds.setUb(range.getUpperBound)
        val d = FmFactory.eINSTANCE.createReal()
        d.setBounds(domainBounds)
        d
    }
    attribute.setDomain(domain)
    attribute
  }

  def buildGroupFeature(name: String, parent: Feature,
                        featureInstanceCardinality: Interval = exactlyOne,
                        groupInstanceCardinality: Interval = exactlyOne,
                        groupTypeCardinality: Interval = exactlyOne
                  ): Feature = {

    val f = FmFactory.eINSTANCE.createFeature()
    f.setName(name)
    if(parent != null) f.setParent(parent)
    f.setGroupTypeCardinality(groupTypeCardinality)
    f.setGroupInstanceCardinality(groupInstanceCardinality)
    f.setFeatureInstanceCardinality(featureInstanceCardinality)
    f
  }

  def buildInterval(lower: Int, upper: Int): Interval = {
    val i = FmFactory.eINSTANCE.createInterval()
    i.setLowerBound(1)
    i.setUpperBound(1)
    i
  }

  def exactlyOne: Interval = buildInterval(1, 1)
}

object DynamicCFMNames {
  val EVENTSIZE_IN_KB = "eventSizeInKB"
  val EVENTSIZE_OUT_KB = "eventSizeOutKB"
  val OPERATOR_SELECTIVITY = "operatorSelectivity"
  val EVENTRATE_IN = "eventRateIn"
  val EVENTRATE_OUT = "eventRateOut"
  val INTER_ARRIVAL_MEAN_MS = "interArrivalMean"
  val INTER_ARRIVAL_STD_MS = "interArrivalStdDev"
  val PARENT_NETWORK_LATENCY_MEAN_MS = "networkParentLatencyMean"
  val PARENT_NETWORK_LATENCY_STD_MS = "networkParentLatencyStdDev"
  val PROCESSING_LATENCY_MEAN_MS = "processingLatencyMean"
  val PROCESSING_LATENCY_STD_MS = "processingLatencyStdDev"
  val END_TO_END_LATENCY_MEAN_MS = "e2eLatencyMean"
  val END_TO_END_LATENCY_STD_MS = "e2eLatencyStdDev"
  val BROKER_CPU_LOAD = "brokerCPULoad"
  val BROKER_THREAD_COUNT = "brokerThreadCount"
  val BROKER_OPERATOR_COUNT = "brokerOperatorCount"
  // bandwidth generated by other queries (excluding all operators belonging to this query)
  val BROKER_OTHER_BANDWIDTH_IN_KB = "brokerOtherBandwidthInKB"
  val BROKER_OTHER_BANDWIDTH_OUT_KB = "brokerOtherBandwidthOutKB"

  val ALL_FEATURES = List(
    EVENTSIZE_IN_KB, EVENTSIZE_OUT_KB,
    OPERATOR_SELECTIVITY,
    EVENTRATE_IN,
    INTER_ARRIVAL_MEAN_MS, INTER_ARRIVAL_STD_MS,
    PARENT_NETWORK_LATENCY_MEAN_MS, PARENT_NETWORK_LATENCY_STD_MS,
    PROCESSING_LATENCY_MEAN_MS, PROCESSING_LATENCY_STD_MS,
    BROKER_CPU_LOAD,
    BROKER_THREAD_COUNT,
    BROKER_OPERATOR_COUNT,
    BROKER_OTHER_BANDWIDTH_IN_KB, BROKER_OTHER_BANDWIDTH_OUT_KB
  )

  val ALL_TARGET_METRICS = List(
    END_TO_END_LATENCY_MEAN_MS,
    END_TO_END_LATENCY_STD_MS,
    EVENTRATE_OUT
  )

  val QUERY = "fcQuery"
}

