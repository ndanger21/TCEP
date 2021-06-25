package tcep.placement

import akka.actor.{ActorContext, ActorRef, Address}
import akka.cluster.{Cluster, Member}
import tcep.data.Queries
import tcep.data.Queries._
import tcep.graph.nodes.traits.Node.Dependencies

import scala.concurrent.{ExecutionContext, Future}

object RandomAlgorithm extends PlacementStrategy {

  override val name = "Random"

  /**
    * Randomly places the operator on one of the available nodes
    * @param dependencies hosts of operators that the operator to be deployed depends on
    * @return HostInfo, containing the address of the node to host the operator
    */
  def findOptimalNode(operator: Query, rootOperator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                     (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster): Future[HostInfo] = {
    val candidates: Set[Member] = findPossibleNodesToDeploy(cluster)
    log.info(s"the number of available members are: ${candidates.size}, members: ${candidates.toList.map(c => c.address.host.get)}")
    applyRandomAlgorithm(operator, candidates.toVector, parentAddressTransform(dependencies))
  }

  def findOptimalNodes(operator: Query, rootOperator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                      (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster): Future[(HostInfo, HostInfo)] = {
    val candidates: Set[Member] = findPossibleNodesToDeploy(cluster)
    for {
      m1 <- applyRandomAlgorithm(operator, candidates.toVector, parentAddressTransform(dependencies))
      m2 <- applyRandomAlgorithm(operator, candidates.filter(!_.equals(m1)).toVector, parentAddressTransform(dependencies))
    } yield (m1, m2)
  }

  /**
    * Applies Random placement algorithm to find a random node for operator deployment
    *
    * @param candidateNodes coordinates of the candidate nodes
    * @return the address of member where operator will be deployed
    */
  def applyRandomAlgorithm(operator: Query, candidateNodes: Vector[Member], parentHosts: Map[Query, Address])(implicit ec: ExecutionContext, cluster: Cluster): Future[HostInfo] = {
    val random: Int = scala.util.Random.nextInt(candidateNodes.size)
    val randomMember: Member = candidateNodes(random)
    for {
      _ <- this.initialize()
      bdpUpdate <- this.updateOperatorToParentBDP(operator, randomMember, parentHosts, Queries.estimateOutputBandwidths(operator)) } yield {
      log.info(s"findOptimalNode - deploying operator on randomly chosen host: ${randomMember}")
      HostInfo(randomMember, operator, this.getPlacementMetrics(operator))
    }
  }

  override def hasInitialPlacementRoutine(): Boolean = false

  override def hasPeriodicUpdate(): Boolean = false

  /**
    * starting point of the initial placement for an entire query
    * retrieves the coordinates of dependencies and calculates an initial placement of all operators in a query
    * (without actually placing them yet)
    *
    * @param rootOperator the operator graph to be placed
    * @return a map from operator to host
    */
  override def initialVirtualOperatorPlacement(rootOperator: Query, publishers: Map[String, ActorRef])
                                              (implicit ec: ExecutionContext, cluster: Cluster, queryDependencies: QueryDependencyMap): Future[Map[Query, HostInfo]] = {
    val candidateNodes = findPossibleNodesToDeploy(cluster).toVector
    def findHostRec(op: Query): Future[Map[Query, HostInfo]] = {
      op match {
        case s: SequenceQuery => applyRandomAlgorithm(s, candidateNodes, Map(
          PublisherDummyQuery(s.s1.publisherName) -> publishers(s.s1.publisherName).path.address,
          PublisherDummyQuery(s.s2.publisherName) -> publishers(s.s2.publisherName).path.address))
          .map(hostf => Map(s -> hostf))

        case s: StreamQuery => applyRandomAlgorithm(s, candidateNodes, Map(PublisherDummyQuery(s.publisherName) -> publishers(s.publisherName).path.address))
          .map(hostf => Map(s -> hostf))

        case u: UnaryQuery => findHostRec(u.sq)
          .flatMap(parentPlacement => applyRandomAlgorithm(u, candidateNodes, parentPlacement.map(e => e._1 -> e._2.member.address))
          .map(opPlacement => parentPlacement.updated(u, opPlacement)))

        case b: BinaryQuery => findHostRec(b.sq1).zip(findHostRec(b.sq2)).map(f => f._1 ++ f._2)
          .flatMap(parentPlacement => applyRandomAlgorithm(b, candidateNodes, parentPlacement.map(e => e._1 -> e._2.member.address))
            .map(opPlacement => parentPlacement.updated(b, opPlacement)))

        case _ => throw new IllegalArgumentException(s"unknown operator type $op")
      }
    }
    findHostRec(rootOperator)
  }
}
