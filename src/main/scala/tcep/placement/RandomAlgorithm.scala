package tcep.placement

import akka.actor.ActorContext
import akka.cluster.{Cluster, Member}
import tcep.data.Queries
import tcep.data.Queries.Query
import tcep.graph.nodes.traits.Node.Dependencies

import scala.concurrent.{ExecutionContext, Future}

object RandomAlgorithm extends PlacementStrategy {

  override val name = "Random"

  /**
    * Randomly places the operator on one of the available nodes
    * @param dependencies hosts of operators that the operator to be deployed depends on
    * @return HostInfo, containing the address of the node to host the operator
    */
  def findOptimalNode(operator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                     (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster, baseEventRate: Double): Future[HostInfo] = {
    val candidates: Set[Member] = findPossibleNodesToDeploy(cluster)
    log.info(s"the number of available members are: ${candidates.size}, members: ${candidates.toList.map(c => c.address.host.get)}")
    applyRandomAlgorithm(operator, candidates.toVector, dependencies)
  }

  def findOptimalNodes(operator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                      (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster, baseEventRate: Double): Future[(HostInfo, HostInfo)] = {
    val candidates: Set[Member] = findPossibleNodesToDeploy(cluster)
    for {
      m1 <- applyRandomAlgorithm(operator, candidates.toVector, dependencies)
      m2 <- applyRandomAlgorithm(operator, candidates.filter(!_.equals(m1)).toVector, dependencies)
    } yield (m1, m2)
  }

  /**
    * Applies Random placement algorithm to find a random node for operator deployment
    *
    * @param candidateNodes coordinates of the candidate nodes
    * @return the address of member where operator will be deployed
    */
  def applyRandomAlgorithm(operator: Query, candidateNodes: Vector[Member], dependencies: Dependencies)(implicit ec: ExecutionContext, cluster: Cluster, baseEventRate: Double): Future[HostInfo] = {
    val random: Int = scala.util.Random.nextInt(candidateNodes.size)
    val randomMember: Member = candidateNodes(random)
    for { bdpUpdate <- this.updateOperatorToParentBDP(operator, randomMember, dependencies.parents, Queries.estimateOutputBandwidths(operator, baseEventRate)) } yield {
      log.info(s"findOptimalNode - deploying operator on randomly chosen host: ${randomMember}")
      HostInfo(randomMember, operator, this.getPlacementMetrics(operator))
    }
  }

  override def hasInitialPlacementRoutine(): Boolean = false

  override def hasPeriodicUpdate(): Boolean = false
}
