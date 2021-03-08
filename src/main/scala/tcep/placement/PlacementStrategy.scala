package tcep.placement

import java.util.concurrent.TimeUnit

import akka.actor.{ActorContext, ActorRef, Address}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.{Coordinates, DistVivaldiActor}
import org.slf4j.LoggerFactory
import tcep.data.Queries._
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.machinenodes.helper.actors._
import tcep.placement.manets.StarksAlgorithm
import tcep.placement.mop.RizouAlgorithm
import tcep.placement.sbon.PietzuchAlgorithm
import tcep.utils.TCEPUtils.makeMapFuture
import tcep.utils.{SizeEstimator, SpecialStats, TCEPUtils}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Created by raheel on 17/08/2017.
  * Updated by Niels on 26/11/2018
  */
trait PlacementStrategy {

  val iterations = ConfigFactory.load().getInt("constants.placement.relaxation-initial-iterations")
  implicit val resolveTimeout = Timeout(ConfigFactory.load().getInt("constants.placement.placement-request-timeout"), TimeUnit.SECONDS)
  val requestTimeout = Duration(ConfigFactory.load().getInt("constants.default-request-timeout"), TimeUnit.SECONDS)
  val log = LoggerFactory.getLogger(classOf[PlacementStrategy])
  var placementMetrics: mutable.Map[Query, OperatorMetrics] = mutable.Map()
  var publishers: Map[String, ActorRef] = Map()
  var memberBandwidths = Map.empty[(Member, Member), Double] // cache values here to prevent asking taskManager each time (-> msgOverhead)
  var memberCoordinates = Map.empty[Member, Coordinates]
  var memberLoads = mutable.Map.empty[Member, Double] // cache loads, evict entries if an operator is deployed
  private val defaultLoad = ConfigFactory.load().getDouble("constants.default-load")
  private val defaultBandwidth = ConfigFactory.load().getDouble("constants.default-data-rate")
  protected var initialized = false
  private lazy val coordRequestSize: Long = SizeEstimator.estimate(CoordinatesRequest(Address("tcp", "tcep", "speedPublisher", 1)))

  val name: String
  def hasInitialPlacementRoutine(): Boolean
  def hasPeriodicUpdate(): Boolean

  /**
    * Find optimal node for the operator to place
    *
    * @return HostInfo, containing the address of the host node
    */
  def findOptimalNode(operator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                     (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster, baseEventRate: Double): Future[HostInfo]

  /**
    * Find two optimal nodes for the operator placement, one node for hosting the operator and one backup node for high reliability
    *
    * @return HostInfo, containing the address of the host nodes
    */
  def findOptimalNodes(operator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                      (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster, baseEventRate: Double): Future[(HostInfo, HostInfo)]

  def initialize(caller: Option[ActorRef] = None)(implicit ec: ExecutionContext, cluster: Cluster): Future[Boolean] = {
    if(initialized){
      Future { true }
    } else synchronized {
      this.placementMetrics.clear()
      SpecialStats.log(s"${this.toString} $caller", "placementInit", s" placementStrategy was not yet initialized, fetching publisherActor list and bandwidths from local taskManager")
      for {
        publisherMap <- TCEPUtils.getPublisherActors(cluster)
        bandwidthMap <- TCEPUtils.getAllBandwidthsFromLocalTaskManager(cluster)
      } yield {
        SpecialStats.log(s"${this.toString} $caller", "placementInit", s" bw and pub requests complete (${bandwidthMap.size} bws, ${publisherMap.size} pubs")
        this.publishers = publisherMap
        initialized = true
        SpecialStats.log(s"${this.toString} $caller", "placementInit", s" algorithm initialization complete \n publishers: \n ${publishers.keys.mkString("\n")}")
        false
      }

    }
  }
  // used for tests only
  def updateCoordinateMap()(implicit ec: ExecutionContext, cluster: Cluster): Future[Map[Member, Coordinates]] = {
    makeMapFuture(cluster.state.members.filter(m=> m.status == MemberStatus.up && !m.hasRole("VivaldiRef"))
      .map(m => {
        m -> TCEPUtils.getCoordinatesOfNode(cluster, m.address).map { result => memberCoordinates = memberCoordinates.updated(m, result); result }
      }).toMap)
    //log.info(s"member coordinates: \n ${memberCoordinates.map(m => s"\n ${m._1.address} | ${m._2} " )} \n")
  }

  protected def updateOperatorToParentBDP(operator: Query, host: Member, parents: Map[ActorRef, Query],
                                          outputDataRateEstimates: Map[Query, Double])
                                         (implicit ec: ExecutionContext, cluster: Cluster): Future[Map[ActorRef, Double]] = {
    // deep queries cause lookup failures when used as key, use string representation instead
    val outputDataRateEstimatesStr = outputDataRateEstimates.map(e => e._1.toString() -> e._2)
    val bdpToParents: Future[Map[ActorRef, Double]] = for {
      opToParentBDP: Map[ActorRef, EventBandwidthEstimate] <- makeMapFuture(parents.map(p => {
        val bdp = for {
          hostCoords <- getCoordinatesOfNode(host, None) // omit operator since message overhead from this is not placement-related
          parentCoords <- getCoordinatesOfNode(p._1, None)
          bw = outputDataRateEstimatesStr.getOrElse(p._2.toString(),
                                      throw new RuntimeException(s"missing ${p._2} \n among \n ${ outputDataRateEstimates.mkString("\n")}")) * 0.001  // dist in [ms], data rate in [Bytes / s]
        } yield parentCoords.distance(hostCoords) * bw
        p._1 -> bdp
      }))
    } yield {
      val operatorMetrics = placementMetrics.getOrElse(operator, OperatorMetrics())
      operatorMetrics.operatorToParentBDP = opToParentBDP
      placementMetrics += operator -> operatorMetrics
      opToParentBDP
    }

    bdpToParents.onComplete {
      case Success(value) => log.info(s"operator bdp between (${value}) and host $host of ${operator.getClass}")
      case Failure(exception) => log.error(s"failed to update bdp between host $host \nand parents ${parents} \nof ${operator} \n\n dataRateEstimates: \n ${ outputDataRateEstimates.mkString("\n")}", exception)
    }
    bdpToParents
  }

  /**
    * called by any method that incurs communication with other nodes in the process of placing an operator
    * updates an operator's placement msgOverhead (amount of bytes sent as messages to other nodes)
    * accMsgOverhead: accumulated overhead from all directly placement-related communication that has been made for placing this operator
    * @param operator
    */
  protected def updateOperatorMsgOverhead(operator: Option[Query], msgOverhead: Long): Unit = {
    if(operator.isDefined) {
      val operatorMetrics = placementMetrics.getOrElse(operator.get, OperatorMetrics())
      operatorMetrics.accPlacementMsgOverhead += msgOverhead
      placementMetrics.update(operator.get, operatorMetrics)
    }
  }

  protected def getPlacementMetrics(operator: Query): OperatorMetrics = placementMetrics.getOrElse(operator, {
    log.debug(s"could not find placement metrics for operator $operator, returning zero values!")
    OperatorMetrics()
  })

  /**
    * find the n nodes closest to this one
    *
    * @param n       n closest
    * @param candidates map of all neighbour nodes to consider and their vivaldi coordinates
    * @return the nodes closest to this one
    */
  def getNClosestNeighboursByMember(n: Int, candidates: Map[Member, Coordinates] = memberCoordinates)(implicit cluster: Cluster): Seq[(Member, Coordinates)] =
    getNClosestNeighboursToCoordinatesByMember(n, DistVivaldiActor.localPos.coordinates, candidates)

  def getClosestNeighbourToCoordinates(coordinates: Coordinates, candidates: Map[Member, Coordinates] = memberCoordinates)(implicit cluster: Cluster): (Member, Coordinates) =
    getNClosestNeighboursToCoordinatesByMember(1, coordinates, candidates).head

  def getNClosestNeighboursToCoordinatesByMember(n: Int, coordinates: Coordinates, candidates: Map[Member, Coordinates] = memberCoordinates)(implicit cluster: Cluster): Seq[(Member, Coordinates)] =
    getNeighboursSortedByDistanceToCoordinates(candidates, coordinates).take(n)

  def getNeighboursSortedByDistanceToCoordinates(neighbours: Map[Member, Coordinates], coordinates: Coordinates)(implicit cluster: Cluster): Seq[(Member, Coordinates)] =
    if(neighbours.nonEmpty) {
      val sorted = neighbours.map(m => m -> m._2.distance(coordinates)).toSeq.sortWith(_._2 < _._2)
      val keys = sorted.map(_._1)
      keys

    } else {
      log.warn("getNeighboursSortedByDistanceToCoordinates() - received empty coordinate map, returning self")
      Seq(cluster.selfMember -> DistVivaldiActor.localPos.coordinates)
    }

  protected def getBandwidthBetweenCoordinates(c1: Coordinates, c2: Coordinates, nnCandidates: Map[Member, Coordinates], operator: Option[Query] = None)(implicit ec: ExecutionContext, cluster: Cluster): Future[Double] = {
    val source: Member = getClosestNeighbourToCoordinates(c1, nnCandidates)._1
    val target: Member = getClosestNeighbourToCoordinates(c2, nnCandidates.filter(!_._1.equals(source)))._1
    val request = getBandwidthBetweenMembers(source, target, operator)
    request
  }

  /**
    * get the bandwidth between source and target Member; use cached values if available to avoid measurement overhead from iperf
    * @param source
    * @param target
    * @return
    */
  protected def getBandwidthBetweenMembers(source: Member, target: Member, operator: Option[Query])(implicit ec: ExecutionContext, cluster: Cluster): Future[Double] = {
    if(source.equals(target)) Future { 0.0d }
    else if(memberBandwidths.contains((source, target))) {
      Future { memberBandwidths.getOrElse((source, target), defaultBandwidth) }
    } else if(memberBandwidths.contains((target, source))) {
      Future { memberBandwidths.getOrElse((target, source), defaultBandwidth) }
    } else {
      val communicationOverhead = SizeEstimator.estimate(SingleBandwidthRequest(cluster.selfMember)) + SizeEstimator.estimate(SingleBandwidthResponse(0.0d))
      this.updateOperatorMsgOverhead(operator, communicationOverhead)
      val request = for {
        bw <- TCEPUtils.getBandwidthBetweenNodes(cluster, source, target)
      } yield {
        memberBandwidths += (source, target) -> bw
        memberBandwidths += (target, source) -> bw
        bw
      }
      request.onComplete {
        case Success(bw) => SpecialStats.debug(s"$this", s"retrieved bandwidth $bw between $source and $target, caching it locally")
        case Failure(exception) => SpecialStats.debug(s"$this", s"failed to retrieve bandwidth between $source and $target, cause: $exception")
      }
      request
    }
  }

  protected def getAllBandwidths(cluster: Cluster, operator: Option[Query])(implicit ec: ExecutionContext): Future[Map[(Member, Member), Double]] = {
    SpecialStats.log(this.name, "placement", s"sending AllBandwidthsRequest to local taskManager")
    for {
      bandwidths <- TCEPUtils.getAllBandwidthsFromLocalTaskManager(cluster).mapTo[Map[(Member, Member), Double]]
    } yield {
      SpecialStats.log(this.name, "placement", s"received ${bandwidths.size} measurements")
      val communicationOverhead = SizeEstimator.estimate(AllBandwidthsRequest()) + SizeEstimator.estimate(AllBandwidthsResponse(bandwidths))
      this.updateOperatorMsgOverhead(operator, communicationOverhead)
      memberBandwidths ++= bandwidths
      bandwidths
    }
  }

  def getMemberByAddress(address: Address)(implicit cluster: Cluster): Option[Member] = cluster.state.members.find(m => m.address.equals(address))

  /**
    * retrieve all nodes that can host operators, including !publishers!
    *
    * @param cluster cluster reference to retrieve nodes from
    * @return all cluster members (nodes) that are candidates (EmptyApp) or publishers (PublisherApp)
    */
  def findPossibleNodesToDeploy(cluster: Cluster): Set[Member] = cluster.state.members.filter(x =>
    // MemberStatus.Up is still true if node was marked temporarily unreachable!
    x.status == MemberStatus.Up && x.hasRole("Candidate") && !cluster.state.unreachable.contains(x))

  /**
    * retrieves the vivaldi coordinates of a node from its actorRef (or Member)
    * attempts to contact the node 3 times before returning default coordinates (origin)
    * records localMsgOverhead from communication for placement metrics
    * @param node the actorRef of the node
    * @param operator the operator with which the incurred communication overhead will be associated
    * @return its vivaldi coordinates
    */
  def getCoordinatesOfNode(node: ActorRef,  operator: Option[Query])(implicit ec: ExecutionContext, cluster: Cluster): Future[Coordinates] = this.getCoordinatesFromAddress(node.path.address, operator)
  def getCoordinatesOfNode(node: Member, operator: Option[Query])(implicit ec: ExecutionContext, cluster: Cluster): Future[Coordinates] = this.getCoordinatesFromAddress(node.address, operator)
  // blocking, !only use in tests!
  def getCoordinatesOfNodeBlocking(node: Member, operator: Option[Query] = None)(implicit ec: ExecutionContext, cluster: Cluster): Coordinates = Await.result(this.getCoordinatesFromAddress(node.address, operator), requestTimeout)

  protected def getCoordinatesFromAddress(address: Address, operator: Option[Query] = None, attempt: Int = 0)(implicit ec: ExecutionContext, cluster: Cluster): Future[Coordinates] = {

    val maxTries = 3
    val member = getMemberByAddress(address)
    if (member.isDefined && memberCoordinates.contains(member.get)) Future { memberCoordinates(member.get) }
    else {
      this.updateOperatorMsgOverhead(operator, coordRequestSize)
      val request: Future[Coordinates] = TCEPUtils.getCoordinatesOfNode(cluster, address)
      request.recoverWith { // retries up to maxTries times if futures does not complete
        case e: Throwable =>
          if (attempt < maxTries) {
            log.warn(s"failed $attempt times to retrieve coordinates of ${member.get}, retrying... \n cause: ${e.toString}")
            this.getCoordinatesFromAddress(address, operator, attempt + 1)
          } else {
            log.warn(s"failed $attempt times to retrieve coordinates of ${member.get}, returning origin coordinates \n cause: ${e.toString}")
            Future { Coordinates.origin }
          }
      } map { result => {
        this.updateOperatorMsgOverhead(operator, SizeEstimator.estimate(CoordinatesResponse(result)))
        result
      }}
    }
  }

  def getCoordinatesOfMembers(nodes: Set[Member], operator: Option[Query] = None)(implicit ec: ExecutionContext, cluster: Cluster): Future[Map[Member, Coordinates]] = {
    makeMapFuture(nodes.map(node => {
      node -> this.getCoordinatesOfNode(node, operator)
    }).toMap)
  }
  // callers  have to handle futures
  protected def getCoordinatesOfNodes(nodes: Seq[ActorRef], operator: Option[Query] = None)(implicit ec: ExecutionContext, cluster: Cluster): Future[Map[ActorRef, Coordinates]] =
    makeMapFuture(nodes.map(node => node -> this.getCoordinatesOfNode(node, operator)).toMap)

  protected def findMachineLoad(nodes: Seq[Member], operator: Option[Query] = None)(implicit ec: ExecutionContext, cluster: Cluster): Future[Map[Member, Double]] = {
    makeMapFuture(nodes.map(n => n -> this.getLoadOfNode(n, operator).recover {
      case e: Throwable =>
        log.info(s"failed to get load of $n using default load $defaultLoad, cause \n $e")
        defaultLoad
    }).toMap)
  }

  /**
    * retrieves current system load of the node; caches the value if not existing
    * (cache entries are cleared periodically (refreshTask) or if an operator is deployed on that node
    * records localMsgOverhead from communication for placement metrics
    * @param node
    * @return
    */
  def getLoadOfNode(node: Member, operator: Option[Query] = None)(implicit ec: ExecutionContext, cluster: Cluster): Future[Double] = {

    if(memberLoads.contains(node)) Future { memberLoads(node) }
    else {
      val request: Future[Double] = TCEPUtils.getLoadOfMember(cluster, node)
      this.updateOperatorMsgOverhead(operator, SizeEstimator.estimate(LoadRequest()) + SizeEstimator.estimate(LoadResponse(_)))
      request.onComplete {
        case Success(load: Double) =>
          memberLoads += node -> load
        case Failure(exception) =>
      }
      request
    }
  }

}

object PlacementStrategy {

  def getStrategyByName(name: String): PlacementStrategy = {
    name match {
      case PietzuchAlgorithm.name => PietzuchAlgorithm
      case RizouAlgorithm.name => RizouAlgorithm
      case StarksAlgorithm.name => StarksAlgorithm
      case RandomAlgorithm.name => RandomAlgorithm
      case MobilityTolerantAlgorithm.name => MobilityTolerantAlgorithm
      case GlobalOptimalBDPAlgorithm.name => GlobalOptimalBDPAlgorithm
      case other: String => throw new NoSuchElementException(s"need to add algorithm type $other to updateTask!")
    }
  }
}
case class HostInfo(member: Member, operator: Query, var operatorMetrics: OperatorMetrics = OperatorMetrics(), var visitedMembers: List[Member] = List())
// accMsgOverhead is placement messaging overhead for all operators from stream to current operator; root operator has placement overhead of entire query graph
case class OperatorMetrics(var operatorToParentBDP: Map[ActorRef, Double] = Map(), var accPlacementMsgOverhead: Long = 0)
case class QueryDependencies(parents: Option[List[Query]], child: Option[Query])
// child is a list for convenience, we only have one child
case class QueryDependenciesWithCoordinates(parents: Map[Query, Coordinates], child: Map[Query, Coordinates]) {
  def getCoord(q: Query): Option[Coordinates] = {
    val p = parents.get(q)
    val c = child.get(q)
    if(p.isDefined) p
    else if(c.isDefined) c
    else None
  }
}
