package tcep.placement

import akka.actor.{ActorContext, ActorRef, Address}
import akka.cluster.{Cluster, Member}
import tcep.data.Queries
import tcep.data.Queries._
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.utils.TCEPUtils

import scala.concurrent.{ExecutionContext, Future}

object MobilityTolerantAlgorithm extends PlacementStrategy {

  override val name = "ProducerConsumer"

  /**
    * places all stream operators directly on their sources, all other operators on the client node
    * @param context      actor context
    * @param cluster      cluster of nodes
    * @param askerInfo    HostInfo item containing the address of the node asking to deploy the operator (currently always the clientNode)
    * @return HostInfo, containing the address of the node to host the operator
    */
  def findOptimalNode(operator: Query, rootOperator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                     (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster): Future[HostInfo] = {
    val candidates: Set[Member] = findPossibleNodesToDeploy(cluster)
    log.info(s"the number of available members are: ${candidates.size}, members: ${candidates.toList.map(c => c.address.host.get)}")
    applyMobilityTolerantAlgorithm(operator, parentAddressTransform(dependencies))
  }

  override def findOptimalNodes(operator: Query, rootOperator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                               (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster): Future[(HostInfo, HostInfo)] = {
    throw new RuntimeException("algorithm does not support reliability")
  }

  /**
    * Places an operator on the source if it is a stream operator, otherwise on the client node
    ** @return the address of member where operator will be deployed
    */
  def applyMobilityTolerantAlgorithm(operator: Query, parentAddresses: Map[Query, Address])(implicit ec: ExecutionContext, cluster: Cluster): Future[HostInfo] = {

    operator match {
      case l: StreamQuery =>
        for {
          _ <- this.initialize()
          publishers <- TCEPUtils.getPublisherActors()
          publisherMember = cluster.state.members.find(_.address == publishers(l.publisherName).path.address).getOrElse(cluster.selfMember) // if selfMember is the publisher we have a local actorRef without address
          outputBandwidthEstimates = Queries.estimateOutputBandwidths(operator)
          bdpUpdate <- this.updateOperatorToParentBDP(operator, publisherMember, parentAddresses, outputBandwidthEstimates)
        } yield {
          log.info(s"deploying STREAM operator on its publisher: ${publisherMember.address}")
          HostInfo(publisherMember, operator, this.getPlacementMetrics(operator))
        }
      case _ =>
        val client = cluster.state.members.find(_.hasRole("Subscriber")).getOrElse(cluster.selfMember)
        for {
          _ <- this.initialize()
          outputBandwidthEstimates = Queries.estimateOutputBandwidths(operator)
          bdpUpdate <- this.updateOperatorToParentBDP(operator, client, parentAddresses, outputBandwidthEstimates)} yield {
          log.info(s"deploying non-stream operator on clientNode ${client}")
          HostInfo(client, operator, this.getPlacementMetrics(operator))
        }
    }
  }

    def initialVirtualOperatorPlacement(rootOperator: Query, publishers: Map[String, ActorRef])
                                       (implicit ec: ExecutionContext, cluster: Cluster, queryDependencies: QueryDependencyMap
                                       ): Future[Map[Query, HostInfo]] = {
      def findHostRec(curOp: Query): Future[Map[Query, HostInfo]] = {
        curOp match {
          case s: StreamQuery => applyMobilityTolerantAlgorithm(s, Map(PublisherDummyQuery(s.publisherName) -> publishers(s.publisherName).path.address))
            .map(hostf => Map(s -> hostf))

          case u: UnaryQuery =>findHostRec(u.sq)
            .flatMap(parentPlacement => applyMobilityTolerantAlgorithm(u, parentPlacement.map(e => e._1 -> e._2.member.address))
              .map(opPlacement => parentPlacement.updated(u, opPlacement)))

          case b: BinaryQuery => findHostRec(b.sq1).zip(findHostRec(b.sq2)).map(f => f._1 ++ f._2)
            .flatMap(parentPlacement => applyMobilityTolerantAlgorithm(b, parentPlacement.map(e => e._1 -> e._2.member.address))
              .map(opPlacement => parentPlacement.updated(b, opPlacement)))

          case s: SequenceQuery => applyMobilityTolerantAlgorithm(s, Map(
            PublisherDummyQuery(s.s1.publisherName) -> publishers(s.s1.publisherName).path.address,
            PublisherDummyQuery(s.s2.publisherName) -> publishers(s.s2.publisherName).path.address))
            .map(hostf => Map(s -> hostf))

          case _ => throw new IllegalArgumentException(s"unknown operator type $curOp")
        }
      }
      findHostRec(rootOperator)
  }

  override def hasInitialPlacementRoutine(): Boolean = false

  override def hasPeriodicUpdate(): Boolean = false

}
