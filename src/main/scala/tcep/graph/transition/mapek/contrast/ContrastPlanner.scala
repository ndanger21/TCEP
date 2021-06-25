package tcep.graph.transition.mapek.contrast

import com.typesafe.config.ConfigFactory
import org.cardygan.config.util.ConfigUtil
import org.cardygan.config.{Config, Instance}
import org.cardygan.fm.Feature
import org.cardygan.fm.util.FmUtil
import org.coala.event.{CoalaListener, MilpSolverFinishedEvent, SatSolverFinishedEvent}
import org.slf4j.LoggerFactory
import tcep.data.Queries._
import tcep.data.Structures.MachineLoad
import tcep.graph.transition.MAPEK.ExecuteTransition
import tcep.graph.transition.mapek.contrast.ContrastMAPEK.{OptimalSystemConfiguration, RunPlanner}
import tcep.graph.transition.{MAPEK, PlannerComponent}

import java.time.Duration
import scala.collection.JavaConverters._
import scala.util.Random

/**
  * Created by Niels on 18.02.2018.
  *
  * responsible for determining 1) if to execute a transition 2) which transition to execute
  * done by formulating system configuration selection problem as an optimization problem that
  *  - is constrained by the CFM and QoS requirements
  *  - transforms a learned performance influence model (performanceModels/latencyModel.log) into an objective function to optimize for
  *
  *  for each QoS requirement it is checked whether the respective performance influence model estimates the requirement to be fulfilled by the new system configuration.
  *  if a QoS requirement leads to the problem to be infeasible (i.e. no system configuration is estimated to be able to fulfill the requirements),
  *  the requirement is relaxed several times
  * @param mapek reference to the running MAPEK instance
  */
class ContrastPlanner(mapek: MAPEK) extends PlannerComponent(mapek) {

  private var attempts = 0
  private val improvementThreshold = ConfigFactory.load().getDouble("constants.mapek.improvement-threshold")
  private val blacklistedAlgorithms = ConfigFactory.load().getStringList("constants.mapek.blacklisted-algorithms").asScala.toList
  override def preStart(): Unit = {
    super.preStart()
    log.info("starting MAPEK planner")
  }

  override def receive: Receive = super.receive orElse {

    // received from analyzer
    case RunPlanner(cfm: CFM, contextConfig: Config, currentLatency: Double, qosRequirements: Set[Requirement]) =>

      log.info(s"received RunPlanner message from analyzer")
      //log.debug(s"with the following context config: ${CFM.configToString(contextConfig)}")

      val fm = cfm.getFM
      // clear cross-tree constraints from previous runs
      fm.getCrossTreeConstraints.clear()
      try {
        val reqChecker = new RequirementChecker(fm, contextConfig, blacklistedFeatures = blacklistedAlgorithms)
        // for evaluation purposes
        val randomTransition = false
        if(randomTransition) {
          val algorithms = FmNames.allPlacementAlgorithms
          val randomlyChosenAlgorithm = algorithms(Random.nextInt(algorithms.size))
          val algorithmFeature = FmUtil.getFeatureByName(fm, randomlyChosenAlgorithm).get()
          log.warning(s"RunPlanner - !EVALUATION! transiting to a randomly chosen algorithm: ${randomlyChosenAlgorithm}")
          mapek.executor ! OptimalSystemConfiguration(reqChecker.generateSystemConfig(algorithmFeature))

        } else {

          val fulfillableRequirements = reqChecker.excludeUnfitConfigurations(qosRequirements)
          log.info(s"RunPlanner - cfm with additional cross-tree constraint(s) to exclude requirement-violating configs: \n " +
            s"${fm.getCrossTreeConstraints.asScala.toList.map(c => s"\n${c.getSource}-${c.getType}-${c.getTarget}")}")
          // possible addition: combine different performanceInfluence models into one, weighting each one's influence
          // using only latency model for optimization as of now because it is usually the most important metric;
          // algorithms that would violate a requirement are excluded, so no need to optimize for a requirement's metric
          // -> check coala v0.3.1

          val optimalConfig: Option[Config] =
            if(fulfillableRequirements.values.forall(_ == true)) // for every requirement at least one potentially fulfilling algorithm must exist
              PlannerHelper.calculateOptimalSystemConfig(cfm, contextConfig, reqChecker.perfModels('latency), qosRequirements)
            else None
          //log.debug(s"Planner: optimalconfig is ${optimalConfig}")
          if(optimalConfig.isDefined) {
            val optimalAlgorithmFeature: Feature = ConfigUtil.getFeatureInstanceByName(optimalConfig.get.getRoot, FmNames.PLACEMENT_ALGORITHM).getChildren.get(0).getType
            val optimalSystemConfig = reqChecker.generateSystemConfig(optimalAlgorithmFeature) // FM config without the context side set
            val predictedLatency: Double = reqChecker.predictMetricFromModel(optimalSystemConfig, performanceModel = reqChecker.perfModels('latency))
            log.info(s"RunPlanner - current latency: $currentLatency, predicted latency with ${optimalAlgorithmFeature.getName}: $predictedLatency ")
            // extract placement algorithm from optimal config
            val placementAlgorithmGroup: Instance = ConfigUtil.getFeatureInstanceByName(optimalSystemConfig.getRoot, FmNames.PLACEMENT_ALGORITHM)
            val placementAlgorithmList = placementAlgorithmGroup.getChildren.asScala.toList
            val nextPlacementAlgorithmInstance = placementAlgorithmList.headOption

            if (placementAlgorithmList.size != 1 || nextPlacementAlgorithmInstance.isEmpty) {
              log.error(s"invalid config with more or less than one placement algorithm: $placementAlgorithmList")
            } else if(predictedLatency <= currentLatency * (1 - improvementThreshold)) { // a solution for the ILP model exists within the given requirement constraints; its performance is predicted to be at least x% better
              mapek.executor ! ExecuteTransition(nextPlacementAlgorithmInstance.get.getName.substring(2)) // remove the "fs" prefix
              log.info(s"sending notification to ContrastExecutor for algorithm ${nextPlacementAlgorithmInstance.get.getName}: estimated improvement ${(1 - (predictedLatency / currentLatency)) * 100.0}% (threshold: ${improvementThreshold*100.0})% after relaxing requirement(s) $attempts times ")
            } else {
              log.info(s"not sending new config to ContrastExecutor since it is not estimated to be at least ${improvementThreshold*100.0}% better than the current configuration")
            }

          } else { // no solution feasible

            if (attempts < 3) { // relax requirements up to 3 times
              log.error("RunPlanner - no solution feasible, QoS requirements cannot be fulfilled " +
                "(either the performance model is too inaccurate or this actually is objectively the case)" +
                s"\n relaxing requirement by 10% for the ${attempts+1} time")
              val weakenedRequirements: Set[Requirement] = qosRequirements.map(req => {
                if(!reqChecker.requirementHolds(req)) {
                  req match {
                    case lr: LatencyRequirement =>
                      val delta = lr.latency.dividedBy(10)
                      lr.operator match {
                        case Greater | GreaterEqual => LatencyRequirement(lr.operator, lr.latency.minus(delta), lr.otherwise)
                        case Smaller | SmallerEqual => LatencyRequirement(lr.operator, lr.latency.plus(delta), lr.otherwise)
                        case _ => lr
                      }
                    case lr: LoadRequirement =>
                      val delta = lr.machineLoad.value / 10.0
                      lr.operator match {
                        case Greater | GreaterEqual => LoadRequirement(lr.operator, MachineLoad(lr.machineLoad.value - delta), lr.otherwise)
                        case Smaller | SmallerEqual => LoadRequirement(lr.operator, MachineLoad(lr.machineLoad.value + delta), lr.otherwise)
                        case _ => lr
                      }
                    case hr: MessageHopsRequirement =>
                      hr.operator match {
                        case Greater | GreaterEqual => MessageHopsRequirement(hr.operator, hr.requirement - 1, hr.otherwise)
                        case Smaller | SmallerEqual => MessageHopsRequirement(hr.operator, hr.requirement + 1, hr.otherwise)
                        case _ => hr
                      }
                    case _ => req
                  }
                } else req
              })
              log.info(s"RunPlanner - weakened requirements: ${weakenedRequirements}")

              attempts += 1
              self ! RunPlanner(cfm, contextConfig, currentLatency, weakenedRequirements)
            } else {
              log.warning(s"RunPlanner - weakened the requirement $attempts times, still no solution is feasible.")
              attempts = 0
            }
          }
        }
      } catch {
        case e: Throwable => log.error(e,s"error while calculating optimal system config")
      }
    }

}

class MyListener() extends CoalaListener {

  val log = LoggerFactory.getLogger(getClass)

  override def solverFinished(solverEvent: MilpSolverFinishedEvent) =
    log.info(s"coala listener received result: objective value: ${solverEvent.getObjectiveValue} after ${Duration.ofNanos(solverEvent.getDuration).toMillis}ms")

  override def solverFinished(solverEvent: SatSolverFinishedEvent) =
    log.info(s"coala listener: received event $solverEvent ${solverEvent.getDuration}")
}