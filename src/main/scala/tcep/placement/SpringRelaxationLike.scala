package tcep.placement

import akka.actor.ActorRef
import akka.cluster.{Cluster, Member}
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.Coordinates
import tcep.data.Queries
import tcep.data.Queries._
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.utils.TCEPUtils
import tcep.utils.TCEPUtils.makeMapFuture

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
trait for placement strategies like Relaxation and Rizou that are based on spring relaxation heuristic
  */
trait SpringRelaxationLike extends PlacementStrategy {
  val minimumImprovementThreshold = 0.001d // minimum improvement of network usage (Byte) per round
  lazy val defaultStepSize: Double = ConfigFactory.load().getDouble("constants.placement.relaxation-initial-step-size")

  // place a single operator
  def collectInformationAndExecute(operator: Query, dependencies: Dependencies)
                                  (implicit ec: ExecutionContext, cluster: Cluster, baseEventRate: Double): Future[HostInfo] = {
    val operatorDependenciesAndBandwidths = Queries.extractOperators(operator, baseEventRate)
    val operatorDependencyMap = operatorDependenciesAndBandwidths.mapValues(_._1).toMap
    val outputBandwidthEstimates = operatorDependenciesAndBandwidths.mapValues(_._4).toMap
    if(operator.isInstanceOf[StreamQuery]) {
      assert(dependencies.parents.values.exists(_.isInstanceOf[PublisherDummyQuery]), s"dependencies for streamOperator must contain PublisherDummyQuery: $dependencies")
    }
    log.error(s"dependencies: \n${dependencies.parents.mkString("\n")} \n ${dependencies.subscribers}")

    val res = for {
      init <- this.initialize()
    } yield {
      // start requests in parallel
      val candidateCoordRequest = getCoordinatesOfMembers(findPossibleNodesToDeploy(cluster), Some(operator))
      val dependencyCoordRequest = getCoordinatesOfNodes((dependencies.parents.keys ++ dependencies.subscribers.keys.flatten).toSeq, Some(operator))
      val selfCoordRequest = getCoordinatesOfNode(cluster.selfMember, Some(operator))
      val clientCoordinatesRequest = TCEPUtils.getCoordinatesOfNode(cluster, cluster.state.members.filter(_.hasRole("Subscriber")).head.address) // do not log overhead since this call is due to implementation
      val publisherRequest = TCEPUtils.getPublisherActors(cluster)

      val hostRequest = for {
        candidateCoordinates <- candidateCoordRequest
        dependencyCoordinates: Map[ActorRef, Coordinates] <- dependencyCoordRequest
        selfCoordinates <- selfCoordRequest
        clientCoordinates <- clientCoordinatesRequest
        publishers <- publisherRequest
        publisherCoordinates <- makeMapFuture(publishers.map(p => p._1 -> getCoordinatesOfNode(p._2, None))) // do not log overhead
        // dependencies: Query -> Member, dependencyCoordinates: Member -> Coordinates; we need (Query -> Coordinates)
        operatorCoordinateMap: Map[Query, Coordinates] = (dependencies.parents ++ dependencies.subscribers.flatten { case (maybeRef, query) => if(maybeRef.isDefined) Some(maybeRef.get, query) else None })
          .map(e => e._2 -> dependencyCoordinates.getOrElse(e._1,
            throw new RuntimeException(s"can't find ${e._2} among $dependencies while trying to resolve actor operator to coordinate mapping")))

        dependencyOperatorCoordinates = buildDependencyOperatorCoordinates(operator, operatorDependencyMap, operatorCoordinateMap, publisherCoordinates, clientCoordinates)
        optimalVirtualCoordinates <- calculateVCSingleOperator(operator, dependencyOperatorCoordinates, outputBandwidthEstimates)
        host <- findHost(optimalVirtualCoordinates, candidateCoordinates, operator, dependencies, outputBandwidthEstimates)
      } yield {
        log.info(s"found node to host operator: ${host.toString}")
        val hostCoords = candidateCoordinates.find(e => e._1.equals(host.member))
        if (hostCoords.isDefined) log.info(s"with distance to virtual coords: ${hostCoords.get._2.distance(optimalVirtualCoordinates)}")
        else log.warn("found node, but coordinatesMap does not contain its coords")
        host
      }
      hostRequest
    }
    res.flatten
  }
  /**
    * starting point of the initial placement for an entire query
    * retrieves the coordinates of dependencies and calculates an initial placement of all operators in a query
    * (without actually placing them yet)
    * @param query the operator graph to be placed
    * @return a map from operator to host
    */
  def initialVirtualOperatorPlacement(query: Query, publishers: Map[String, ActorRef])
                                     (implicit ec: ExecutionContext, cluster: Cluster, baseEventRate: Double,
                                      queryDependencies: mutable.LinkedHashMap[Query, (QueryDependencies, EventRateEstimate, EventSizeEstimate, EventBandwidthEstimate)]
                                     ): Future[Map[Query, Coordinates]] = {

    val clientNode = cluster.state.members.filter(m => m.roles.contains("Subscriber"))
    assert(clientNode.size == 1, "there must be exactly one client node with role Subscriber")
    // note that the message overhead is attributed only to the root operator since it is retrieved only once!
    val publisherMembers = publishers.map(p => cluster.state.members.find(_.address == p._2.path.address).getOrElse(cluster.selfMember))
    val publisherCoordRequest = getCoordinatesOfMembers(publisherMembers.toSet, Some(query))
    val virtualOperatorPlacementRequest = for {
      publisherCoordinates <- publisherCoordRequest
      clientCoordinates <- getCoordinatesOfNode(clientNode.head, Some(query))
    } yield {
      log.info("\n" + publisherCoordinates.mkString("\n"))
      log.info("\n" + publishers.mkString("\n"))
      calculateVirtualPlacementWithCoords(query, clientCoordinates, publishers
          .map(e => e._1 -> publisherCoordinates.find(_._1.address == e._2.path.address).getOrElse((cluster.selfMember, publisherCoordinates(cluster.selfMember)))._2))
    }

    virtualOperatorPlacementRequest.onComplete {
      case Success(_) => log.debug(s"$this", s"calculated virtual coordinates for query")
      case Failure(exception) => log.error(s"$this", s"calculation of virtual coordinates failed, cause: \n $exception")
    }
    virtualOperatorPlacementRequest
  }

  /**
    * performs an initial placement of all operators in a query by performing successive (virtual) placement updates for
    * each operator in the virtual coordinate space
    * @param query the query consisting of the operators to be placed
    */
  def calculateVirtualPlacementWithCoords(query: Query, clientCoordinates: Coordinates, publisherCoordinates: Map[String, Coordinates])
                                         (implicit ec: ExecutionContext, cluster: Cluster, baseEventRate: Double,
                                          queryDependencies: mutable.LinkedHashMap[Query, (QueryDependencies, EventRateEstimate, EventSizeEstimate, EventBandwidthEstimate)]
                                         ): Map[Query, Coordinates] = {
    val operatorDependenciesAndBandwidths = Queries.extractOperators(query, baseEventRate)
    val operatorDependencyMap = operatorDependenciesAndBandwidths.mapValues(_._1).toMap
    val outputBandwidthEstimates = operatorDependenciesAndBandwidths.mapValues(_._4).toMap
    //println("client Coordinates: " + clientCoordinates)
    //println(s"publisher Coordinates: \n ${publisherCoordinates.mkString("\n")}")
    // use LinkedHashMap here to guarantee that parent operators are updated before children
    val operators = queryDependencies.toList.map(_._1).reverse // reverse so that parents are updated before their children
    var currentOperatorCoordinates: Map[Query, Coordinates] = operators.map(o => o -> Coordinates.origin).toMap // start at origin
    log.info("\n" + publisherCoordinates.mkString("\n"))
    currentOperatorCoordinates = currentOperatorCoordinates.map(e => {
      e._1 match {
        case d: PublisherDummyQuery => e._1 -> publisherCoordinates(d.p)
        case c: ClientDummyQuery => e._1 -> clientCoordinates
        case _ => e._1 -> e._2
      }
    })
    val onlyOperators = operators.filter(o => !o.isInstanceOf[ClientDummyQuery] && !o.isInstanceOf[PublisherDummyQuery])
    log.info(s"initialised virtual operator coordinates: \n ${currentOperatorCoordinates.toList.map(_.swap).mkString("\n")}")
    for(i <- 0 to iterations) {
      // updates take influence only in next round
      val updateRound = makeMapFuture( // run each operator's current round update in parallel
        onlyOperators.map(operator => {
          val dependencyCoordinates = buildDependencyOperatorCoordinates(operator, operatorDependencyMap, currentOperatorCoordinates, publisherCoordinates, clientCoordinates) // get updated dependency operator coordinates
          operator -> calculateVCSingleOperator(operator, dependencyCoordinates, outputBandwidthEstimates)
        }).toMap)
      val updatedCoordinates = Await.result(updateRound, resolveTimeout.duration) // block here to wait for all operators to finish their update round
      currentOperatorCoordinates = updatedCoordinates
      //log.debug(s"iteration $i: \n${currentOperatorCoordinates.map(e => e._1.toString() -> e._2).mkString("\n")}")
    }
    currentOperatorCoordinates
  }

  /**
    * returns a list of coordinates on which the operator is dependent, i.e. the coordinates of its parents and children.
    * for stream operators the publisher acts as a parent, for the root operator the client node acts as the child
    * @param operator operator for which to obtain dependency coordinates
    * @param operatorDependencyMap map of all operators in the query and their parent/child dependencies
    * @param currentOperatorCoordinates coordinates of currently placed operators, WITHOUT clientDummy and PublisherDummy
    * @return two maps with parent and child operator -> coordinates
    */
  def buildDependencyOperatorCoordinates(operator: Query, operatorDependencyMap: Map[Query, QueryDependencies],
                                         currentOperatorCoordinates: Map[Query, Coordinates],
                                         publisherDummyCoordinates: Map[String, Coordinates], clientDummyCoordinates: Coordinates
                                        ): QueryDependenciesWithCoordinates = {
    log.info(s"operator: $operator \n dependencymap: \n ${operatorDependencyMap.keys.mkString("\n")} \n coordinatemap: \n${currentOperatorCoordinates.mkString("\n")} \n publisherDummyCoords: ${publisherDummyCoordinates.mkString("\n")} \n clientDummyCoords: $clientDummyCoordinates")
    // deep queries cause lookup failures, use string representation instead
    val operatorCoordStrMap = currentOperatorCoordinates.map(e => e._1.toString() -> e._2)
    try {
      operator match {
        case PublisherDummyQuery(p, r) => throw new RuntimeException(
          "buildDependencyOperatorCoordinates() should never be called for a publisher dummy operator")
        case ClientDummyQuery(r) => throw new RuntimeException(
          "buildDependencyOperatorCoordinates() should never be called for a client dummy operator")
        case _ => assert(operatorDependencyMap.contains(operator),
                         s"$operator not among ${ operatorDependencyMap.mkString("\n") }")
          val dependencies = operatorDependencyMap(operator)
          assert(dependencies.child.isDefined, s"$operator has missing child dependency among \n $dependencies")
          assert(dependencies.parents.isDefined, s"$operator has missing parent dependency among \n $dependencies")
          assert(publisherDummyCoordinates.nonEmpty, s"$operator has no publisher coordinates provided")
          dependencies.parents.get.foreach(p => assert(operatorCoordStrMap.contains(p.toString()) || p.isInstanceOf[PublisherDummyQuery],
                 s"parent $p must be in operatorCoordMap: \n${ currentOperatorCoordinates.mkString("\n") }"))
          val parentCoordinates = dependencies.parents.get.map(p => {
            p -> operatorCoordStrMap.getOrElse(p.toString(), publisherDummyCoordinates(p.asInstanceOf[PublisherDummyQuery].p))
          }).toMap
          //
          val childCoordinates = Map(dependencies.child.get -> operatorCoordStrMap.getOrElse(
            dependencies.child.get.toString(), clientDummyCoordinates))
          QueryDependenciesWithCoordinates(parentCoordinates, childCoordinates)
      }
    } catch {
      case e: Throwable =>
        log.error("failed to build dependency coordinates", e)
        throw e
    }
  }

  /**
    * determine the optimal position of a single operator in the virtual vivaldi coordinate space
    * uses a spring-relaxation technique:
    * edges on the operator tree are modeled as springs; the length of a spring corresponds to the latency between the operators on its ends,
    * the spring constant corresponds to the bandwidth on that link. Now the goal is to solve for the minimum energy state in all springs
    * by iteratively moving operators in the latency space in small steps into the direction of the force exerted by the springs connected to them.
    *
    * @param dependencyQueriesWithCoordinates the coordinates of all operator hosts that are connected to this operator (child and parents)
    * @return the optimal vivaldi coordinates for the operator
    */
  def calculateVCSingleOperator(operator: Query,
                                dependencyQueriesWithCoordinates: QueryDependenciesWithCoordinates,
                                dataRateToDependenciesEstimates: Map[Query, Double])
                               (implicit ec: ExecutionContext, cluster: Cluster): Future[Coordinates] = {
    // check if there is any significant distance between the dependency coordinates (> 0.001)
    val minDist = 0.001
    // start coordinates, previous round force or bdp, step size
    val startParameters = getStartParameters(operator, dependencyQueriesWithCoordinates, dataRateToDependenciesEstimates)
    val childCoords = dependencyQueriesWithCoordinates.child.values.toList
    val parentCoords = dependencyQueriesWithCoordinates.parents.values.toList
    val allCoords: List[Coordinates] = startParameters._1 :: childCoords ++ parentCoords
    if(Coordinates.areAllEqual(allCoords, minDist)) {
      Future { startParameters._1 }
    } else {
      makeVCStep(startParameters._1, startParameters._3, startParameters._2)(ec, operator, dependencyQueriesWithCoordinates, dataRateToDependenciesEstimates)
    }
  }

  /**
    *
    * @param operator current operator to be placed; root operator in case of initial placement of complete query
    * @param dependenciesWithCoordinates coordinates of parent and child dependencies; publisher and client in case of initial placement
    * @param dependenciesDataRateEstimates output data rate estimates for dependencies
    * @return Tuple of (startVirtualCoordinates, previousRoundForce (or BDP), startStepSize)
    */
  def getStartParameters(implicit operator: Query,
                                  dependenciesWithCoordinates: QueryDependenciesWithCoordinates,
                                  dependenciesDataRateEstimates: Map[Query, Double]): (Coordinates, Double, Double) = {
    // Relaxation paper said near coordinate origin, but this does not converge well when all coordinates are in a small cluster far off the origin
    //val noise = Coordinates(nextDouble(), nextDouble(), 0.0)
    val startCoords = getWeberL1StartCoordinates(dependenciesWithCoordinates.parents.values ++ dependenciesWithCoordinates.child.values) //.add(noise)
    (startCoords, Double.MaxValue, defaultStepSize)
  }

  /**
    * calculate the start coordinates as the mean along each dimension
    * @param dependencyCoordinates coordinates of dependencies
    */
  def getWeberL1StartCoordinates(dependencyCoordinates: Iterable[Coordinates]): Coordinates = {
    val x = dependencyCoordinates.map(_.x).sum / dependencyCoordinates.size
    val y = dependencyCoordinates.map(_.y).sum / dependencyCoordinates.size
    val h = dependencyCoordinates.map(_.h).sum / dependencyCoordinates.size
    Coordinates(x, y, h)
  }
  /**
    * estimates the BDP (network usage) on all links to (from parents) and from (to child) this operator if it was placed at the given position
    * @return BDP estimate value for the position
    */
  def estimateBDPAtPosition(position: Coordinates)(implicit operator: Query, dependencyCoordinates: QueryDependenciesWithCoordinates,
                                                   dependenciesDataRateEstimates: Map[Query, Double]): Double = {
    val parentBDPs = dependencyCoordinates.parents.map(parent => {
      parent._2.distance(position) * 0.001 * dependenciesDataRateEstimates(parent._1)
    })
    // operator sends data on link to child
    val childBDPs = dependencyCoordinates.child.map(child => child._2.distance(position) * 0.001 * dependenciesDataRateEstimates(operator))
    parentBDPs.sum + childBDPs.sum
  }

  def makeVCStep(previousVC: Coordinates, stepSize: Double, previousRoundForce: Double, iteration: Int = 0, consecutiveStepAdjustments: Int = 0)
                (implicit ec: ExecutionContext, operator: Query, dependencyCoordinates: QueryDependenciesWithCoordinates,
                 dependenciesDataRateEstimates: Map[Query, Double]): Future[Coordinates]

  /**
    * called once the virtual coordinates have been determined to place an operator on a host
    * only used by algorithms that have a separate initial placement logic
    * @param virtualCoordinates
    * @param candidates
    * @param operator
    * @return hostInfo including the hosting Member, its bdp to the parent nodes, and the msgOverhead of the placement
    */
  def findHost(virtualCoordinates: Coordinates, candidates: Map[Member, Coordinates], operator: Query,
               dependencies: Dependencies, outputBandwidthEstimates: Map[Query, Double])
              (implicit ec: ExecutionContext, cluster: Cluster): Future[HostInfo] = {
    if(!hasInitialPlacementRoutine()) throw new IllegalAccessException("only algorithms with a non-sequential initial placement should call findHost() !")
    for {
      candidatesMap <- if(candidates.isEmpty) getCoordinatesOfMembers(findPossibleNodesToDeploy(cluster), Some(operator)) else Future { candidates }
      host <- selectHostFromCandidates(virtualCoordinates, candidatesMap, Some(operator))
      bdpUpdate <- this.updateOperatorToParentBDP(operator, host._1, dependencies.parents, outputBandwidthEstimates)
    } yield {
      val hostInfo = HostInfo(host._1, operator, this.getPlacementMetrics(operator))
      memberLoads.remove(host._1) // get new value with deployed operator on next request
      placementMetrics.remove(operator) // placement of operator complete, clear entry
      log.info(name, s"found host: $host")
      hostInfo
    }
  }

  /**
    * selects a host near the given coordinates for the given operator according to the algorithms placement rule
    * @return the cluster member to host the operator, and optionally a map of candidate members and their cpu loads
    */
  def selectHostFromCandidates(coordinates: Coordinates, memberToCoordinates: Map[Member, Coordinates], operator: Option[Query] = None)(implicit ec: ExecutionContext, cluster: Cluster): Future[(Member, Map[Member, Double])]

}
