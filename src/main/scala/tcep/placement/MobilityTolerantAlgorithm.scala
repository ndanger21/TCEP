package tcep.placement

import akka.actor.ActorContext
import akka.cluster.{Cluster, Member}
import tcep.data.Queries
import tcep.data.Queries.{PublisherDummyQuery, Query, QueryDependencyMap, StreamQuery}
import tcep.graph.nodes.traits.Node.Dependencies

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
    applyMobilityTolerantAlgorithm(operator, dependencies)
  }

  override def findOptimalNodes(operator: Query, rootOperator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                               (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster): Future[(HostInfo, HostInfo)] = {
    throw new RuntimeException("algorithm does not support reliability")
  }

  /**
    * Places an operator on the source if it is a stream operator, otherwise on the client node
    ** @return the address of member where operator will be deployed
    */
  def applyMobilityTolerantAlgorithm(operator: Query, dependencies: Dependencies)(implicit ec: ExecutionContext, cluster: Cluster): Future[HostInfo] = {

    operator match {
      case l: StreamQuery =>
        val publisherActor = dependencies.parents.find(_._2.equals(PublisherDummyQuery(l.publisherName))).getOrElse(throw new RuntimeException(s"missing parentActor entry for ${l.publisherName} among $dependencies"))
        val publisherMember = cluster.state.members.find(_.address == publisherActor._1.path.address).getOrElse(cluster.selfMember) // if selfMember is the publisher we have a local actorRef without address
        for {
          _ <- this.initialize()
          outputBandwidthEstimates = Queries.estimateOutputBandwidths(operator)
          bdpUpdate <- this.updateOperatorToParentBDP(operator, publisherMember, dependencies.parents, outputBandwidthEstimates)} yield {
          log.info(s"deploying STREAM operator on its publisher: ${publisherMember.address}")
          HostInfo(publisherMember, operator, this.getPlacementMetrics(operator))
        }
      case _ =>
        //akka.tcp://adaptiveCEP@10.0.0.253:2500
        val client = cluster.state.members.find(_.hasRole("Subscriber")).getOrElse(cluster.selfMember)
        for {
          _ <- this.initialize()
          outputBandwidthEstimates = Queries.estimateOutputBandwidths(operator)
          bdpUpdate <- this.updateOperatorToParentBDP(operator, client, dependencies.parents, outputBandwidthEstimates) } yield {
          log.info(s"deploying non-stream operator on clientNode ${client}")
          HostInfo(client, operator, this.getPlacementMetrics(operator))
        }
    }

  }

  override def hasInitialPlacementRoutine(): Boolean = false

  override def hasPeriodicUpdate(): Boolean = false

}
