package tcep.placement.sbon



import akka.actor.ActorContext
import akka.cluster.{Cluster, Member}
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.Coordinates
import tcep.data.Queries._
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.placement._

import scala.concurrent.{ExecutionContext, Future}


/**
  * Implementation of Relaxation algorithm introduced by Pietzuch et al. in
  * https://www.researchgate.net/publication/220965454_Network-Aware_Operator_Placement_for_Stream-Processing_Systems
  * Aims to place an operator on a host in the cluster so that the resulting network usage (bandwidth-delay-product, BDP) is minimized
  * Differences to RizouAlgorithm: see RizouAlgorithm
  */
object PietzuchAlgorithm extends SpringRelaxationLike {

  val stepAdjustmentEnabled: Boolean = ConfigFactory.load().getBoolean("constants.placement.relaxation-step-adjustment-enabled")
  val iterationLimit = ConfigFactory.load().getInt("constants.placement.max-single-operator-iterations")
  val defaultBandwidth = ConfigFactory.load().getDouble("constants.default-data-rate")
  var stepSize = ConfigFactory.load().getDouble("constants.placement.relaxation-initial-step-size")
  var k = ConfigFactory.load().getInt("constants.placement.physical-placement-nearest-neighbours")
  var nodeLoads = Map[Member, Double]()
  override val name = "Relaxation"
  override def hasInitialPlacementRoutine(): Boolean = true
  override def hasPeriodicUpdate(): Boolean = ConfigFactory.load().getBoolean("constants.placement.update.relaxation")

  // this should only be called during transitions or in case of missing operators during the initial placement
  override def findOptimalNode(operator: Query, rootOperator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                     (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster): Future[HostInfo] = {
    try {
      collectInformationAndExecute(operator, dependencies)
    } catch {
      case e: Throwable =>
        log.error(s"failed to find host for $operator", e)
        throw e
    }
  }

  override def findOptimalNodes(operator: Query, rootOperator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                               (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster): Future[(HostInfo, HostInfo)] = {
    for {
      mainNode <- collectInformationAndExecute(operator, dependencies)
    } yield {
      val backup = HostInfo(cluster.selfMember, operator, OperatorMetrics())
      (mainNode, backup)
    }
  }

  /**
    * Applies Relaxation algorithm to find the optimal virtual coordinates for a single operator
    */
  @scala.annotation.tailrec
  def makeVCStep(previousVC: Coordinates,
                 stepSize: Double,
                 previousRoundForce: Double,
                 iteration: Int = 0,
                 consecutiveStepAdjustments: Int = 0)
                 (implicit ec: ExecutionContext, operator: Query, dependencyCoordinates: QueryDependenciesWithCoordinates,
                 dependenciesDataRateEstimates: Map[Query, Double]): Future[Coordinates] = {
    if(iteration < iterationLimit) {
      //println(s"makeVCStep $operator iteration $iteration - \ncurrCoord $previousVC, currentForce: $previousRoundForce")
      // Relaxation minimizes SUM( datarate(p, v) * latency(p, v)^2 )
      // reason according to paper: 'additional squared exponent in the function ensures that there is a unique solution from a set of placements with equal network usage'
      // since distances between coordinates represent latencies; the length of the vector from A to B is the latency between A and B
      // -> we need to square the length of the vector between A and B
      // -> multiply the vector by its length; the scaled vectors length is the square of the original length (latency)
      // | x * |x| |  = |x|^2
      val parentForces = dependencyCoordinates.parents.map(parent => {
        val vectorToParent = parent._2.sub(previousVC)
        // f = f + datarate(p , v) * (p - v)²
        // latency * dataRate = BDP (Bandwidth-Delay-Product)
        val squaredVectorToParent = vectorToParent.scale(vectorToParent.measure())
        val dependencyDataRate = dependenciesDataRateEstimates(parent._1) // data rate for parent -> operator
        // latency is in [ms], dataRate in [Bytes/s]; use KBytes instead of Bytes since otherwise steps get too large
        val dependencyBDPForce = squaredVectorToParent.scale(dependencyDataRate * 1e-6)
        //println(s"parent ${parent._1.getClass} bdp vector: $dependencyBDPForce vecToDep: ${parent._2.sub(previousVC)} unit: ${ vectorToParent } data rate: $dependencyDataRate")
        dependencyBDPForce
      })
      val childForces = dependencyCoordinates.child.map(child => {
        val vectorToChild = child._2.sub(previousVC)
        val childForce = vectorToChild.scale(vectorToChild.measure()).scale(dependenciesDataRateEstimates(operator) * 1e-6)
        //println(s"child ${child._1.getClass} bdp vector: $childForce vecToDep: ${child._2.sub(previousVC)} unit: ${child._2.sub(previousVC).unity()} data rate: ${dependenciesDataRateEstimates(operator)}")
        childForce
      })
      val currentRoundForceVector: Coordinates = (parentForces ++ childForces).foldLeft(Coordinates(0, 0, 0))(_.add(_))
      val currentRoundForce = currentRoundForceVector.measure()
      val step = currentRoundForceVector.scale(stepSize)
      // use unity vector scaled by step size here to not make ludicrously large steps; relative influence of data rates on f is preserved
        //if(stepAdjustmentEnabled) currentRoundForceVector.scale(stepSize)
        //else currentRoundForceVector.scale(stepSize)
      val updatedVC = previousVC.add(step)
      val delta = previousRoundForce - currentRoundForce
      /*
      println(s"iteration $iteration" +
                s"\ndependencyCoords: ${dependencyCoordinates}" +
                s"\ncurrent round force size: ${currentRoundForce}, forces: $currentRoundForceVector)" +
                s"\ndelta: $delta" +
                s"\nstep vector: ${step} step vector size: ${step.measure()}, stepSize param: $stepSize}" +
                s"\nnext virtualCoords: ${if(currentRoundForce > previousRoundForce) previousVC else updatedVC} ")
      */
      // adjust stepSize to make steps smaller when force vector starts growing between two iterations
      // (we want the force vector's length to approach zero)
      // paper did not feature step size adjustment, but algorithm does not converge well/at all without it
      val forceThreshold = 0.5d
      if(stepAdjustmentEnabled) {
        if(delta > 0 && currentRoundForce >= forceThreshold) {
          makeVCStep(updatedVC, stepSize, currentRoundForce, iteration + 1)
        } else if(delta <= 0) {
          // do not pass updated parameters -> try again with smaller step
          makeVCStep(previousVC, stepSize * 0.5, previousRoundForce, iteration + 1)
        } else {
          // improvement that is smaller than minimum improvement -> terminate
          Future { updatedVC }
        }
        // no step size adjustment
      } else {
        if(currentRoundForce >= forceThreshold) {
          makeVCStep(updatedVC, stepSize, currentRoundForce, iteration + 1)
        } else {
          Future { updatedVC }
        }
      }

    } else { // terminate
      log.warn(s"maximum iteration limit ($iterationLimit) reached, returning virtual coordinates $previousVC now!")
      Future { previousVC }
    }
  }

  /**
    * looks for the k members closest to the virtual coordinates and returns the one with minimum load
    * @param virtualCoordinates
    * @param candidates members to be considered
    * @return member with minimum load
    */
  // TODO once operator re-use is implemented, this must be adapted according to the paper
  def selectHostFromCandidates(virtualCoordinates: Coordinates, candidates: Map[Member, Coordinates], operator: Option[Query] = None)(implicit ec: ExecutionContext, cluster: Cluster): Future[(Member, Map[Member, Double])] = {
    for {
      machineLoads: Map[Member, Double] <- findMachineLoad(getNClosestNeighboursToCoordinatesByMember(k, virtualCoordinates, candidates).map(_._1), operator)
    } yield {
      val host = machineLoads.toList.minBy(_._2)._1
      (host, machineLoads)
    }
  }

}
