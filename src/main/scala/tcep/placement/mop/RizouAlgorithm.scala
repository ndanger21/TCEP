package tcep.placement.mop

import akka.actor.ActorContext
import akka.cluster.{Cluster, Member}
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.Coordinates
import tcep.data.Queries.Query
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.placement._

import scala.concurrent.{ExecutionContext, Future}


/**
  * Implementation of MOP algorithm introduced by Stamatia Rizou et. al.
  * http://ieeexplore.ieee.org/abstract/document/5560127/
  * Note: assumes that there is only one consumer for a graph, i.e. operators must not be shared (re-used) between graphs!
  *
  *  Differences to Relaxation:
  *  1. round-to-round improvement uses estimated BDP if operator were to be placed at current coordinates, instead of sum of dependency forces (not the same)
  *  2. uses actual BDP formula for calculating dependency forces (no squared latency component)
  *  3. mapping of virtual operator coordinate to physical host: chooses among the k closest hosts the closest  non-overloaded host (unix cpu load < 3.0),
  *     instead of the least loaded host among the closest k hosts
  */

object RizouAlgorithm extends SpringRelaxationLike {

  var k = ConfigFactory.load().getInt("constants.placement.physical-placement-nearest-neighbours")
  val loadThreshold = ConfigFactory.load().getDouble("constants.placement.physical-placement-node-overload-threshold")
  val iterationLimit = ConfigFactory.load().getInt("constants.placement.max-single-operator-iterations")
  val defaultBandwidth = ConfigFactory.load().getDouble("constants.default-data-rate")
  override val name = "Rizou"
  override def hasInitialPlacementRoutine(): Boolean = true
  override def hasPeriodicUpdate(): Boolean = ConfigFactory.load().getBoolean("constants.placement.update.rizou")

  /**
    * Applies Rizou's algorithm to find the optimal node for a single operator
    * @param dependencies   parent nodes on which query is dependent (parent and children operators; caller is responsible for completeness!)
    * @return the address of member where operator will be deployed
    */
  // this should only be called during transitions or in case of missing operators during the initial placement
  def findOptimalNode(operator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                     (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster, baseEventRate: Double): Future[HostInfo] = {
    collectInformationAndExecute(operator, dependencies)
  }

  def findOptimalNodes(operator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                      (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster, baseEventRate: Double): Future[(HostInfo, HostInfo)] =
    for {
      mainNode <- collectInformationAndExecute(operator, dependencies)
    } yield {
      val backup = HostInfo(cluster.selfMember, operator, OperatorMetrics())
      (mainNode, backup)
    }

  override def getStartParameters(implicit operator: Query,
                                  dependenciesWithCoordinates: QueryDependenciesWithCoordinates,
                                  dependenciesDataRateEstimates: Map[Query, Double]): (Coordinates, Double, Double) = {

    val (parentCoords, childCoords) = (dependenciesWithCoordinates.parents, dependenciesWithCoordinates.child)
    var startCoordinates = getWeberL1StartCoordinates(childCoords.values ++ parentCoords.values)
    // deadpoint check: see if any of the dependency coords would provide a smaller bdp
    var startPointBDP = estimateBDPAtPosition(startCoordinates)(operator, dependenciesWithCoordinates, dependenciesDataRateEstimates)
    val dependencyCoordMinBDP = (childCoords ++ parentCoords)
      .map(dep => dep -> estimateBDPAtPosition(dep._2)(operator, dependenciesWithCoordinates, dependenciesDataRateEstimates)).minBy(_._2)

    if(dependencyCoordMinBDP._2 < startPointBDP) {
      startCoordinates = dependencyCoordMinBDP._1._2
      startPointBDP =  dependencyCoordMinBDP._2
    }
    // initial step size: maximum distance between start coord and any of the dependency coords
    val stepSize: Double = (childCoords ++ parentCoords).map(c => c._2.distance(startCoordinates)).max
    (startCoordinates, startPointBDP, stepSize)
  }

  @scala.annotation.tailrec
  def makeVCStep(previousVC: Coordinates,
                 stepSize: Double,
                 previousRoundBDP: Double,
                 iteration: Int = 0,
                 consecutiveStepAdjustments: Int = 0)
                (implicit ec: ExecutionContext, operator: Query, dependencyCoordinates: QueryDependenciesWithCoordinates,
                 dependenciesDataRateEstimates: Map[Query, Double]): Future[Coordinates] = {
    if(iteration < iterationLimit) {
      //println(s"makeVCStep $operator iteration $iteration - \ncurrCoord $previousVC, currBDP: $previousRoundBDP")
      // Rizou minimizes SUM( datarate(p, v) * latency(p, v)
      val parentForces = dependencyCoordinates.parents.map(parent => {
        // f = f + datarate(p , v) * unit(p - v)
        // latency space positions determine direction of force, data rate estimate the magnitude
        // note that using unit() here (as in the paper), can impact the total force in some cases e.g.
        // assuming equal data rate of 1000 for all three links:
        // a = (-25.000, 50.000) unit: (-0.447, 0.894)
        // b = (-25.000, -50.000) unit: (-0.447, -0.894)
        // c = (50.000, 0.000) unit: (1.000, 0.000)
        // -> sum(a,b,c): (0, 0) vs unit: (0.106, 0.000)
        val vectorToParent = parent._2.sub(previousVC).unity()
        // latency is in [ms], dataRate in [Bytes/s]
        val dependencyDataRate = dependenciesDataRateEstimates(parent._1) * 0.001
        // latency * dataRate = BDP (Bandwidth-Delay-Product, i.e. network usage)
        val dependencyBDPForce = vectorToParent.scale(dependencyDataRate)
        //println(s"parent ${parent._1.getClass} bdp vector: $dependencyBDPForce vecToDep: ${parent._2.sub(previousVC)} unit: $vectorToParent data rate: $dependencyDataRate")
        dependencyBDPForce
      })
      val childForces = dependencyCoordinates.child.map(child => { // same as parent, but with output data rate estimate of this operator
        val childBDPForce = child._2.sub(previousVC).unity().scale(dependenciesDataRateEstimates(operator) * 0.001)
        //println(s"child ${child._1.getClass} bdp vector: $childBDPForce vecToDep: ${child._2.sub(previousVC)} unit: ${child._2.sub(previousVC).unity()} data rate: ${dependenciesDataRateEstimates(operator)}")
        childBDPForce
      })
      val forcesTotal: Coordinates = (parentForces ++ childForces).foldLeft(Coordinates(0, 0, 0))(_.add(_))
      val step = forcesTotal.unity().scale(stepSize)
      val updatedVC = previousVC.add(step)
      val currentRoundBDP: Double = estimateBDPAtPosition(updatedVC)
      val delta: Double = previousRoundBDP - currentRoundBDP
      /*
      println(s"dependencyCoords: ${dependencyCoordinates}" +
                s"\ncurrent round bdp: ${currentRoundBDP} (vs force vector length: ${forcesTotal.measure()}, forces: $forcesTotal)" +
                s"\ndelta: $delta" +
                s"\nstep vector: ${step} step vector size: ${step.measure()}, stepSize param: $stepSize}" +
                s"\nnext virtualCoords: ${if(currentRoundBDP > previousRoundBDP) previousVC else updatedVC} \n")
      */
      if(delta > 0 && delta >= minimumImprovementThreshold) { // currentRoundBDP < previousRoundBDP
        makeVCStep(updatedVC, stepSize, currentRoundBDP, iteration + 1)
        // adjust stepSize to make steps smaller when network usage stops shrinking between two iterations (i.e. we step further than necessary) only try this 3 times in a row
      } else if(delta <= 0 && consecutiveStepAdjustments < 3) {
        // do not pass updated parameters -> try again with smaller step
        makeVCStep(previousVC, stepSize * 0.5, previousRoundBDP, iteration + 1, consecutiveStepAdjustments + 1)
      } else {
        // improvement that is smaller than minimum improvement -> terminate
        Future { updatedVC }
      }
    } else { // terminate
      log.warn(s"maximum iteration limit ($iterationLimit) reached, returning virtual coordinates $previousVC now!")
     Future { previousVC }
    }
  }

  /**
    * looks for the k members closest to the virtual coordinates and returns the closest that is not overloaded
    * @param virtualCoordinates
    * @param candidates members to be considered
    * @return member with minimum load
    */
  def selectHostFromCandidates(virtualCoordinates: Coordinates, candidates: Map[Member, Coordinates], operator: Option[Query] = None)(implicit ec: ExecutionContext, cluster: Cluster): Future[(Member, Map[Member, Double])] = {
    for {
      machineLoads: Map[Member, Double] <- findMachineLoad(getNClosestNeighboursToCoordinatesByMember(k, virtualCoordinates, candidates).map(_._1), operator)
    } yield {
      val sortedLoads = machineLoads.toList.sortBy(_._2)
      val host = sortedLoads.find(_._2 <= loadThreshold).getOrElse(sortedLoads.head)._1 // chose the closest non-overloaded node
      (host, machineLoads)
    }
  }

}
