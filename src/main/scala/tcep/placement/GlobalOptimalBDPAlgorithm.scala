package tcep.placement
import akka.actor.{ActorContext, ActorRef}
import akka.cluster.{Cluster, Member}
import org.discovery.vivaldi.Coordinates
import tcep.data.Queries._
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.utils.TCEPUtils

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object GlobalOptimalBDPAlgorithm extends SpringRelaxationLike {

  override val name: String = "GlobalOptimalBDP"
  var singleNodePlacement: Option[(Member, Double)] = None

  override def hasInitialPlacementRoutine(): Boolean = true

  override def hasPeriodicUpdate(): Boolean = false

  /**
    * Find optimal node for the operator to place
    *
    * @return HostInfo, containing the address of the host node
    */
  override def findOptimalNode(operator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                              (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster, baseEventRate: Double): Future[HostInfo] =
    applyGlobalOptimalBDPAlgorithm(operator, dependencies).map(_._1)


  /**
    * Find two optimal nodes for the operator placement, one node for hosting the operator and one backup node for high reliability
    *
    * @return HostInfo, containing the address of the host nodes
    */
  override def findOptimalNodes(operator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                               (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster, baseEventRate: Double): Future[(HostInfo, HostInfo)] = {
    for {
      mainNode <- findOptimalNode(operator, dependencies, askerInfo)
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
  def applyGlobalOptimalBDPAlgorithm(operator: Query, dependencies: Dependencies)
                                    (implicit ec: ExecutionContext, cluster: Cluster, baseEventRate: Double): Future[(HostInfo, Double)] = {
    val allParentDependencies = extractOperators(operator, baseEventRate)
    val outputBandwidthEstimates = allParentDependencies.map(e => e._1 -> e._2._4).toMap
    val res = for {
      init <- this.initialize()
      publisherActors <- TCEPUtils.getPublisherActors(cluster)
    } yield {
      cluster.system.scheduler.scheduleOnce(5 seconds)(() => this.singleNodePlacement = None) // reset
      if (this.singleNodePlacement.isDefined) {
        println(s"already placed one operator, current op $operator")
        for { bdpUpdate <- this.updateOperatorToParentBDP(operator, this.singleNodePlacement.get._1, dependencies.parents, outputBandwidthEstimates) } yield
          (HostInfo(this.singleNodePlacement.get._1, operator, this.getPlacementMetrics(operator)), this.singleNodePlacement.get._2)
      } else {
        // get publishers that appear in the query
        val publishersInQuery = cluster.state.members.filter(_.hasRole("Publisher")).flatMap(p => {
          val publisherName = publisherActors.find(_._2.path.address == p.address).getOrElse(
            // get self from list if called on that publisher
            publisherActors.find(a => a._2.path.address.host.isEmpty && a._2.path.address.port.isEmpty).get)._1
          val publisherDummyQuery = allParentDependencies.find(
            d => d._1.isInstanceOf[PublisherDummyQuery] && d._1.asInstanceOf[PublisherDummyQuery].p == publisherName)
          // only add an entry if the publisher is used in the query
          if (publisherDummyQuery.isDefined) Some(publisherDummyQuery.get._1 -> p) else None
        }).toMap
        val subscriber = cluster.state.members.find(_.hasRole("Subscriber"))
        assert(publishersInQuery.nonEmpty && subscriber.nonEmpty, "could not find publisher and client members!")

        println(s"no operator placed yet, publishers: \n $publishersInQuery")

        for {
          allCoordinates <- getCoordinatesOfMembers(cluster.state.members, Some(operator))
          minBDPCandidate = allCoordinates.filter(_._1.hasRole("Candidate"))
                                          .map(c => calculateSingleHostPlacementBDP(c._1, subscriber.get, publishersInQuery, allCoordinates, outputBandwidthEstimates)).minBy(_._2)
          bdpUpdate <- this.updateOperatorToParentBDP(operator, minBDPCandidate._1, dependencies.parents, outputBandwidthEstimates)
        } yield {
          val hostInfo = HostInfo(minBDPCandidate._1, operator, this.getPlacementMetrics(operator))
          placementMetrics.remove(operator) // placement of operator complete, clear entry
          this.singleNodePlacement = Some((minBDPCandidate))
          (hostInfo, minBDPCandidate._2)
        }

      }
    }
    res.flatten

  }

  def calculateSingleHostPlacementBDP(candidate: Member,
                                      subscriber: Member,
                                      publishers: Map[Query, Member],
                                      allCoordinates: Map[Member, Coordinates],
                                      outputBandwidthEstimates: Map[Query, EventBandwidthEstimate]): (Member, Double) = {
    // distance: ms, bandwidthEstimates: Bytes/s
    val bdpsToPublishers = publishers.map(p => allCoordinates(p._2).distance(allCoordinates(candidate)) * outputBandwidthEstimates(p._1) * 0.001)
    val bdpToSubscriber = allCoordinates(subscriber).distance(allCoordinates(candidate)) * outputBandwidthEstimates(ClientDummyQuery()) * 0.001
    candidate -> (bdpsToPublishers.sum + bdpToSubscriber)
  }

  // called during the initial placement in QueryGraph
  override def initialVirtualOperatorPlacement(query: Query, publishers: Map[String, ActorRef])
                                                (implicit ec: ExecutionContext, cluster: Cluster, baseEventRate: Double,
                                                 queryDependencies: mutable.LinkedHashMap[Query, (QueryDependencies, EventRateEstimate, EventSizeEstimate, EventBandwidthEstimate)]
                                                ): Future[Map[Query, Coordinates]] = {
    // calling applyGlobalOptimalBDPAlgorithm calculates the optimal single-host placement while using the root operator's estimated data rate
    // (instead of the stream operator's when using the normal recursive initial deployment)
    // dependencies not needed here, BDP is updated when calling findHost
    for {_ <- applyGlobalOptimalBDPAlgorithm(query, Dependencies(Map(), Map())) } yield {
      // coordinates are not used during deployment with findHost(), see selectHostFromCandidates() below
      assert(this.singleNodePlacement.isDefined, "should have an optimal host after calling initialVirtualOperatorPlacement()")
      Map(query -> Coordinates.origin)
    }
  }

  override def selectHostFromCandidates(coordinates: Coordinates,
                                        memberToCoordinates: Map[Member, Coordinates],
                                        operator: Option[Query])
                                       (implicit ec: ExecutionContext,
                                        cluster: Cluster): Future[(Member, Map[Member, EventRateEstimate])] = {
    assert(singleNodePlacement.isDefined, "findHost must be called after virtual placement; no singleNodePlacement is available yet")
    Future {(singleNodePlacement.get._1, Map())}
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
                          stepSize: EventRateEstimate,
                          previousRoundForce: EventRateEstimate,
                          iteration: Int,
                          consecutiveStepAdjustments: Int = 0)
                         (implicit ec: ExecutionContext,
                          operator: Query,
                          dependencyCoordinates: QueryDependenciesWithCoordinates,
                          dependenciesDataRateEstimates: Map[Query, EventRateEstimate]): Future[Coordinates] = ???

}
