package tcep.graph.transition.mapek.contrast

import tcep.placement.manets.StarksAlgorithm
import tcep.placement.{GlobalOptimalBDPAlgorithm, MobilityTolerantAlgorithm, RandomAlgorithm}
import tcep.placement.mop.RizouAlgorithm
import tcep.placement.sbon.PietzuchAlgorithm

object FmNames {

  /**
    * define all feature names here, must be consistent with cfm.cardy
    * used for uniform naming across monitoring, data collection and CFM handling (-> code completion)
    *
    * ALL NEW/CHANGED FEATURES AND ATTRIBUTES MUST BE ADDED HERE AND IN 'resources/cfm.cardy'
    */
  val ROOT = "root"
  // system features -> elements of the system that can be reconfigured at runtime
  val SYSTEM = "fsSystem"
  val MECHANISMS = "fsMechanisms"
  val PLACEMENT_ALGORITHM = "fsPlacementAlgorithm"
  val STARKS = s"fs${StarksAlgorithm.name}"
  val RELAXATION = s"fs${PietzuchAlgorithm.name}"
  val RANDOM = s"fs${RandomAlgorithm.name}"
  val MOBILITY_TOLERANT = s"fs${MobilityTolerantAlgorithm.name}"
  val RIZOU = s"fs${RizouAlgorithm.name}"
  val GLOBAL_OPTIMAL_BDP = s"fs${GlobalOptimalBDPAlgorithm.name}"

  // context features -> elements that influence system performance, but are beyond our direct control
  val CONTEXT = "fcContext"
  val NETSITUATION = "fcNetworkSituation"
  val FIXED_PROPERTIES = "fcFixedProperties"
  val VARIABLE_PROPERTIES = "fcVariableProperties"

  val OPERATOR_TREE_DEPTH = "fcOperatorTreeDepth"
  val MAX_TOPOLOGY_HOPS_PUB_TO_CLIENT = "fcMaxTopoHopsPubToClient"
  //val BASE_LATENCY = "fcBaseLatency"

  val LOAD_VARIANCE = "fcLoadVariance"
  val AVG_EVENT_ARRIVAL_RATE = "fcAvgEventArrivalRate"
  val EVENT_PUBLISHING_RATE = "fcEventPublishingRate"
  val NODECOUNT = "fcNodeCount"
  val NODECOUNT_CHANGERATE = "fcNodeCountChangerate"
  val NODE_TO_OP_RATIO = "fcNodeToOperatorRatio"
  val JITTER = "fcJitter"
  val AVG_VIV_DISTANCE = "fcAvgVivaldiDistance"
  val VIV_DISTANCE_STDDEV = "fcVivaldiDistanceStdDev"
  val LINK_CHANGES = "fcLinkChanges"
  val MAX_PUB_TO_CLIENT_PING = "fcMaxPublisherPing"
  val GINI_CONNECTION_DEGREE_1_HOP = "fcGiniNodesIn1Hop"
  val GINI_CONNECTION_DEGREE_2_HOPS = "fcGiniNodesIn2Hop"
  val AVG_NODES_IN_1_HOP            = "fcAvgNodesIn1Hop"
  val AVG_NODES_IN_2_HOPS           = "fcAvgNodesIn2Hop"
  val MOBILITY = "fcMobility"
  val AVG_HOPS_BETWEEN_NODES = "fcAvgHopsBetweenNodes"

  // used for rounding in monitor component and for step size in xml converter
  // (important because SPLC checks attribute bounds when reading measurements, must be consistent with CFM.xml file)
  val LATENCY_DIGITS = 1
  val JITTER_DIGITS = 1
  val LOAD_DIGITS = 3
  val EVENT_FREQUENCY_DIGITS = 3
  val NODECOUNT_DIGITS = 1
  val NODE_TO_OP_RATIO_DIGITS = 3
  val NETWORK_USAGE_DIGITS = 0
  val AVG_VIV_DISTANCE_DIGITS = 3
  val GINI_DIGITS = 3

  //metrics to be measured/predicted
  val LATENCY = "mLatency"
  val AVG_LOAD = "mAvgSystemLoad"
  val MSG_HOPS = "mMsgHops"
  val OVERHEAD = "mOverhead"
  val NETWORK_USAGE = "mNetworkUsage"

  // all features of the CFM
  val cfmFeatures: List[String] = List(
    ROOT,
    SYSTEM,
    MECHANISMS,
    PLACEMENT_ALGORITHM,
    STARKS,
    RELAXATION,
    RANDOM,
    MOBILITY_TOLERANT,
    RIZOU,
    GLOBAL_OPTIMAL_BDP,
    CONTEXT,
    NETSITUATION,
    FIXED_PROPERTIES,
    VARIABLE_PROPERTIES
  )

  // 'fixed' means not changing over the course of a simulation run (but can change between simulations)
  val fixedCFMAttributes: List[String] = List(
    OPERATOR_TREE_DEPTH,
    MAX_TOPOLOGY_HOPS_PUB_TO_CLIENT
    //BASE_LATENCY
  )
  val variableCFMAttributes: List[String] = List(
    MOBILITY,
    JITTER,
    LOAD_VARIANCE,
    EVENT_PUBLISHING_RATE,
    AVG_EVENT_ARRIVAL_RATE,
    NODECOUNT,
    NODECOUNT_CHANGERATE,
    LINK_CHANGES,
    NODE_TO_OP_RATIO,
    AVG_VIV_DISTANCE,
    VIV_DISTANCE_STDDEV,
    MAX_PUB_TO_CLIENT_PING,
    GINI_CONNECTION_DEGREE_1_HOP,
    GINI_CONNECTION_DEGREE_2_HOPS,
    AVG_NODES_IN_1_HOP,
    AVG_NODES_IN_2_HOPS,
    AVG_HOPS_BETWEEN_NODES
  )
  val metricElements: List[String] = List(LATENCY, AVG_LOAD, MSG_HOPS, OVERHEAD, NETWORK_USAGE)
  val allPlacementAlgorithms: List[String] = List(STARKS, RELAXATION, RANDOM, MOBILITY_TOLERANT, RIZOU, GLOBAL_OPTIMAL_BDP)
  val allAttributes: List[String] = fixedCFMAttributes ++ variableCFMAttributes
  val allFeaturesAndAttributes: List[String] = cfmFeatures ++ fixedCFMAttributes ++ variableCFMAttributes
}
