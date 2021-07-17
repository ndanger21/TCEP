package tcep.placement
import akka.actor.{ActorContext, ActorRef}
import akka.cluster.{Cluster, Member}
import org.discovery.vivaldi.Coordinates
import tcep.data.Queries
import tcep.data.Queries._
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.utils.TCEPUtils

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object GlobalOptimalBDPAlgorithm extends SpringRelaxationLike {

  override val name: String = "GlobalOptimalBDP"
  var singleNodePlacement: Option[SingleNodePlacement] = None
  case class SingleNodePlacement(host: Member, minBDP: Double)

  override def hasInitialPlacementRoutine(): Boolean = true

  override def hasPeriodicUpdate(): Boolean = false

  /**
    * Find optimal node for the operator to place
    *
    * @return HostInfo, containing the address of the host node
    */
  override def findOptimalNode(operator: Query, rootOperator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                              (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster): Future[HostInfo] =
    applyGlobalOptimalBDPAlgorithm(operator, rootOperator, dependencies).map(_._1)


  /**
    * Find two optimal nodes for the operator placement, one node for hosting the operator and one backup node for high reliability
    *
    * @return HostInfo, containing the address of the host nodes
    */
  override def findOptimalNodes(operator: Query, rootOperator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                               (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster): Future[(HostInfo, HostInfo)] = {
    for {
      mainNode <- findOptimalNode(operator, rootOperator, dependencies, askerInfo)
    } yield {
      val backup = HostInfo(cluster.selfMember, operator, OperatorMetrics())
      (mainNode, backup)
    }
  }

  /**
    * Finds the node with minimum Bandwidth-Delay-Product sum (i.e. minimum number of Bytes on the network at a given time)
    * to all publishers and the consumer, using global knowledge about the latency space and estimated data rate between operators.
    * Successive calls for different operators (supposedly from the same graph) yield the same host (during initial deployment in QueryGraph).
    * Calls for single operators during Transition on different Hosts should result in the same host, as long as the Vivaldi network coordinates are consistent across Hosts (!)
    * publishers and subscriber can host operators only if they also have the "Candidate" role
    * @param operator operator to be placed
    * @return Host with minimum bdp
    */
  def applyGlobalOptimalBDPAlgorithm(operator: Query, rootOperator: Query, dependencies: Dependencies)
                                    (implicit ec: ExecutionContext, cluster: Cluster): Future[(HostInfo, Double)] = {
    try {
      val res = for {
        _ <- this.initialize()
      } yield {
        val queryDependencyMap = Queries.extractOperatorsAndThroughputEstimates(rootOperator)
        val outputBandwidthEstimates = queryDependencyMap.map(e => e._1 -> e._2._4).toMap
        if (this.singleNodePlacement.isDefined) {
          log.info(s"already placed one operator on ${singleNodePlacement.get.host}, reusing ost for current op $operator")
          for {
            bdpUpdate <- this.updateOperatorToParentBDP(operator, this.singleNodePlacement.get.host, parentAddressTransform(dependencies), outputBandwidthEstimates)
          } yield (HostInfo(this.singleNodePlacement.get.host, operator, this.getPlacementMetrics(operator)), this.singleNodePlacement.get.minBDP)
        } else {
          cluster.system.scheduler.scheduleOnce(5 seconds)(() => this.singleNodePlacement = None) // reset
          for {
            allCoordinates <- getCoordinatesOfMembers(cluster.state.members, Some(operator))
            // get publishers that appear in the ENTIRE query
            allPublisherActors: Map[String, ActorRef] <- TCEPUtils.getPublisherActors()
            allQueryPublishers = Queries.getPublishers(rootOperator)
            allQueryPublisherMembers: Map[PublisherDummyQuery, Member] = allPublisherActors
              .filter(p => allQueryPublishers.contains(p._1))
              .map(e => PublisherDummyQuery(e._1) -> cluster.state.members.find(_.address.equals(e._2.path.address)).getOrElse(cluster.selfMember))

            subscriber = {
              val subscriber = cluster.state.members.find(_.hasRole("Subscriber"))
              assert(allQueryPublisherMembers.nonEmpty && subscriber.nonEmpty, "could not find publisher and client members!")
              log.info(s"no operator placed before $operator, \n publishersInQuery are: $allQueryPublisherMembers \n publisherActors are $allPublisherActors")
              subscriber
            }

            minBDPCandidate = allCoordinates.filter(_._1.hasRole("Candidate"))
              .map(c => calculateSingleHostPlacementBDP(c._1, subscriber.get, allQueryPublisherMembers, allCoordinates, outputBandwidthEstimates)).minBy(_._2)
            bdpUpdate <- this.updateOperatorToParentBDP(operator, minBDPCandidate._1, parentAddressTransform(dependencies), outputBandwidthEstimates)
          } yield {
            val hostInfo = HostInfo(minBDPCandidate._1, operator, this.getPlacementMetrics(operator))
            placementMetrics.remove(operator) // placement of operator complete, clear entry
            this.singleNodePlacement = Some(SingleNodePlacement(minBDPCandidate._1, minBDPCandidate._2)) // cache publisher event rate lookup for next operator
            (hostInfo, minBDPCandidate._2)
          }

        }
      }
      res.flatten
    } catch {
      case e: Throwable => log.error("failed to run GlobalOptimalPlacement", e)
        throw e
    }
  }

  def calculateSingleHostPlacementBDP(candidate: Member,
                                      subscriber: Member,
                                      publishers: Map[PublisherDummyQuery, Member],
                                      allCoordinates: Map[Member, Coordinates],
                                      outputBandwidthEstimates: Map[Query, EventBandwidthEstimate]): (Member, Double) = {
    // distance: ms, bandwidthEstimates: Bytes/s
    val bdpsToPublishers = publishers.map(p => allCoordinates(p._2).distance(allCoordinates(candidate)) * outputBandwidthEstimates(p._1) * 0.001)
    val bdpToSubscriber = allCoordinates(subscriber).distance(allCoordinates(candidate)) * outputBandwidthEstimates(ClientDummyQuery()) * 0.001
    candidate -> (bdpsToPublishers.sum + bdpToSubscriber)
  }

  // called during the initial placement in QueryGraph
  override def getVirtualOperatorPlacementCoords(query: Query, publishers: Map[String, ActorRef])
                                                (implicit ec: ExecutionContext, cluster: Cluster, queryDependencies: QueryDependencyMap
                                                ): Future[Map[Query, Coordinates]] = {
    // calling applyGlobalOptimalBDPAlgorithm calculates the optimal single-host placement while using the root operator's estimated data rate
    // (instead of the stream operator's when using the normal recursive initial deployment)
    // dependencies not needed here, BDP is updated when calling findHost
    for {_ <- applyGlobalOptimalBDPAlgorithm(query, query, Dependencies(Map(), Map())) } yield {
      // coordinates are not used during deployment with findHost(), see selectHostFromCandidates() below
      assert(this.singleNodePlacement.isDefined, "should have an optimal host after calling initialVirtualOperatorPlacement()")
      // Coordinates are irrelevant here, findHost() just calls selectHostFromCandidates (below) which uses singleNodePlacement
      val allOps = Queries.getOperators(query)
      allOps.map(op => op -> Coordinates.origin).toMap
    }
  }

  override def selectHostFromCandidates(coordinates: Coordinates,
                                        memberToCoordinates: Map[Member, Coordinates],
                                        operator: Option[Query])
                                       (implicit ec: ExecutionContext,
                                        cluster: Cluster): Future[(Member, Map[Member, Double])] = {
    assert(singleNodePlacement.isDefined, "findHost must be called after virtual placement; no singleNodePlacement is available yet")
    Future {(singleNodePlacement.get.host, Map())}
  }
  /*
  def findGlobalOptimalPlacement(rootOperator: Query, hosts: List[Member], allPairMetrics: Map[(Member, Member), Double],
                                 calculateMetricOfPlacement: (Map[Query, Member], Query, Map[(Member, Member), Double]) => Double,
                                 min: Boolean = true): (List[(Query, Member)], Double) = {

    val operators = this.extractOperators(rootOperator)
    val allPossiblePlacements = findAllPossiblePlacementsRec(operators.keys.toList, hosts)
    val allPlacementMetrics = allPossiblePlacements.map(p => p -> calculateMetricOfPlacement(p.toMap, rootOperator, allPairMetrics))
    if(min) allPlacementMetrics.minBy(_._2)
    else allPlacementMetrics.maxBy(_._2)
  }

  /**
    * recursively calculate all possible placements by iterating over the operators and building a list of placement alternatives for each host
    * note that the result grows according to |hosts|^|operators| , e.g. 5^8 = 390625
    * @param operators operators to be placed
    * @param hosts available hosts
    * @param currentPlacementAcc accumulator holding an incomplete placement
    * @return a list of lists (placements) containing all possible placements
    */
  def findAllPossiblePlacementsRec(operators: List[Query], hosts: List[Member], currentPlacementAcc: List[(Query, Member)] = List()): List[List[(Query, Member)]] = {
    operators match {
      // return a list of placements: for every host, an alternative placement with the current operator is generated
      case op :: remainingOps => hosts.flatMap(host => findAllPossiblePlacementsRec(remainingOps, hosts, (op, host) :: currentPlacementAcc))
      case Nil => List(currentPlacementAcc)
    }
  }

  def calculateBDPOfPlacement(placement: Map[Query, Member], rootOperator: Query, coords: Map[Member, Coordinates], bandwidths: Map[(Member, Member), Double]): Double = {

    def getBandwidth(m1: Option[Member], m2: Option[Member]): Double = {
      if(m1.isDefined && m2.isDefined) bandwidths.getOrElse((m1.get, m2.get), bandwidths.getOrElse((m2.get, m1.get), Double.MaxValue))
      else Double.MaxValue
    }
    val candidates: Set[Member] = coords.keySet.intersect(bandwidths.keySet.map(_._1)).intersect(bandwidths.keySet.map(_._2))
    if(candidates.size < coords.size || candidates.size < bandwidths.size * bandwidths.size) log.warn("")
    val opToParentBDPs = placement.map(op => op._1 -> {
      val opCoords = coords(op._2)
      val bdpToParents = op._1 match { // a parent is a subquery (sq)
        // find stream publisher by member address port (see pNames)
        case s: StreamQuery =>
          val publisherHost = candidates.find(_.address.toString.contains(s.publisherName))
          getBandwidth(Some(op._2), publisherHost) * opCoords.distance(coords.find(e => s.publisherName.contains(e._1.address.port.get.toString)).get._2)

        case s: SequenceQuery =>
          val publisher1Host = candidates.find(_.address.toString.contains(s.s1.publisherName))
          val publisher2Host = candidates.find(_.address.toString.contains(s.s2.publisherName))
          getBandwidth(Some(op._2), publisher1Host) * opCoords.distance(coords(publisher1Host.get)) +
          getBandwidth(Some(op._2), publisher2Host) * opCoords.distance(coords(publisher2Host.get))

        case b: BinaryQuery => getBandwidth(Some(op._2), placement.get(b.sq1)) * opCoords.distance(coords(placement(b.sq1))) + getBandwidth(Some(op._2), placement.get(b.sq2)) * opCoords.distance(coords(placement(b.sq2)))
        case u: UnaryQuery => getBandwidth(Some(op._2), placement.get(u.sq)) * opCoords.distance(coords(placement(u.sq)))
      }
      if(op._1 == rootOperator) {
        val subscriberCoord = coords.find(_._1.hasRole("Subscriber")).getOrElse(throw new RuntimeException(s"cannot find subscriber coords: ${coords.mkString("\n")}"))
        bdpToParents + getBandwidth(Some(op._2), Some(subscriberCoord._1) ) * opCoords.distance(subscriberCoord._2)
      }
      else bdpToParents
    })
    opToParentBDPs.values.sum
  }
  */
  // not used
  override def makeVCStep(previousVC: Coordinates,
                          stepSize: Double,
                          previousRoundForce: Double,
                          iteration: Int,
                          consecutiveStepAdjustments: Int = 0)
                         (implicit ec: ExecutionContext,
                          operator: Query,
                          dependencyCoordinates: QueryDependenciesWithCoordinates,
                          dependenciesDataRateEstimates: Map[Query, EventBandwidthEstimate]): Future[Coordinates] = ???

}
