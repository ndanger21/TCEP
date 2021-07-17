package tcep.placement.manets

import akka.actor.{ActorContext, ActorRef, Address}
import akka.cluster.{Cluster, Member}
import akka.util.Timeout
import org.discovery.vivaldi.{Coordinates, DistVivaldiActor}
import tcep.data.Queries
import tcep.data.Queries._
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.machinenodes.helper.actors.{Message, StarksTask, StarksTaskReply}
import tcep.placement.{HostInfo, PlacementStrategy}
import tcep.utils.{SizeEstimator, SpecialStats, TCEPUtils}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Created by mac on 10/10/2017.
  */
object StarksAlgorithm extends PlacementStrategy {

  override val name = "MDCEP"
  override def hasInitialPlacementRoutine(): Boolean = false
  override def hasPeriodicUpdate(): Boolean = false // TODO implement
  private val k = 2 // nearest neighbours to try in case task manager for nearest neighbour cannot be found

  def findOptimalNode(operator: Query, rootOperator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                     (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster): Future[HostInfo] = {

    val res: Future[Future[HostInfo]] = for {
      init <- this.initialize()
    } yield {
      val parentAddresses = parentAddressTransform(dependencies)
      val outputBandwidthEstimates = Queries.estimateOutputBandwidths(operator)
      if (askerInfo.visitedMembers.contains(cluster.selfMember)) { // prevent forwarding in circles, i.e. (rare) case when distances are equal
        val hostInfo = HostInfo(cluster.selfMember, operator, askerInfo.operatorMetrics, askerInfo.visitedMembers) // if this node is asked a second time, stop forwarding and deploy on self
        for {
          bdpupdate <- this.updateOperatorToParentBDP(operator, hostInfo.member, parentAddresses, outputBandwidthEstimates)
        } yield {
          this.updateOperatorMsgOverhead(Some(operator), SizeEstimator.estimate(hostInfo)) //add overhead from sending back hostInfo to requester
          hostInfo.operatorMetrics = this.getPlacementMetrics(operator)
          hostInfo
        }

      } else {
        val startTime = System.currentTimeMillis()
        val candidates: Set[Member] = findPossibleNodesToDeploy(cluster).filter(!_.equals(cluster.selfMember)) //
        val candidateCoordRequest = getCoordinatesOfMembers(candidates, Some(operator))
        val parentCoordRequest = getCoordinatesOfNodes(dependencies.parents.keys.toSeq, Some(operator))
        val hostRequest = for {
          candidateCoordinates <- candidateCoordRequest
          parentCoordinates <- parentCoordRequest
          hostInfo <- {
            log.debug(s"the number of available candidates are: ${candidates.size}, candidates: ${candidates.toList}")
            log.debug(s"applyStarksAlgorithm() - dependencyCoords:\n ${parentCoordinates.map(d => d._1.path.address.toString + " -> " + d._2.toString)}")
            placementMetrics += operator -> askerInfo.operatorMetrics // add message overhead from callers (who forwarded to this node); if this is the first caller, these should be zero
            //SpecialStats.debug(s"$this", s"applying starks on ${cluster.selfAddress} to $operator")
            applyStarksAlgorithm(parentCoordinates, candidateCoordinates, askerInfo, operator, askerInfo.visitedMembers, dependencies)
          }
          bdp <- this.updateOperatorToParentBDP(operator, hostInfo.member, parentAddresses, outputBandwidthEstimates)
        } yield {
          // add local overhead to accumulated overhead from callers
          hostInfo.operatorMetrics = this.getPlacementMetrics(operator)
          if (hostInfo.visitedMembers.nonEmpty) hostInfo.operatorMetrics.accPlacementMsgOverhead += SizeEstimator.estimate(hostInfo) // add overhead from sending back hostInfo to requester once this future finishes successfully
          //SpecialStats.log(this.name, "Placement", s"after update: ${hostInfo.operatorMetrics}")
          placementMetrics.remove(operator) // placement of operator complete, clear entry
          hostInfo
        }

        hostRequest.onComplete {
          case Success(host) =>
            val latencyInMillis = System.currentTimeMillis() - startTime
            log.debug(s"$this", s"found host ${host.member}, after $latencyInMillis ms")
            //SpecialStats.log(this.name, "Placement", s"TotalMessageOverhead:${placementMetrics.getOrElse(operator, OperatorMetrics().accPlacementMsgOverhead)}, placement time: $latencyInMillis")
            //SpecialStats.log(this.name, "Placement", s"PlacementTime:${System.currentTimeMillis() - startTime} millis")
          case Failure(exception) =>
            log.error(s"failed to place $operator, cause: \n $exception")
        }
        hostRequest
      }
    }
    res.flatten
  }

  def findOptimalNodes(operator: Query, rootOperator: Query, dependencies: Dependencies, askerInfo: HostInfo)
                      (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster): Future[(HostInfo, HostInfo)] = {
    throw new RuntimeException("Starks algorithm does not support reliability")
  }

  /**
    * Applies Starks's algorithm to find the optimal node for operator deployment
    *
    * @param parentCoordinates   parent nodes on which this operator is dependent (i.e. closer to publishers in the operator tree)
    * @param candidateNodes coordinates of the candidate nodes
    * @param askerInfo HostInfo(member, operator, operatorMetrics) from caller; operatorMetrics contains msgOverhead accumulated from forwarding
    * @param visitedMembers the members to which the operator has already been forwarded
    * @return the address of member where operator will be deployed
    */
  def applyStarksAlgorithm(parentCoordinates: Map[ActorRef, Coordinates], candidateNodes: Map[Member, Coordinates],
                           askerInfo: HostInfo, operator: Query, visitedMembers: List[Member], dependencies: Dependencies)
                          (implicit ec: ExecutionContext, context: ActorContext, cluster: Cluster): Future[HostInfo] = {

    def findDistanceFromParents(coordinates: Coordinates): Double = {
      parentCoordinates.values.foldLeft(0d)((t, c) => t + coordinates.distance(c))
    }

    //val prodsizes = parents.keySet.toSeq.map(p => s"\n$p has size ${SizeEstimator.estimate(p)}" )
    //println(s"DEBUG parent size (bytes) : $prodsizes")
    val mycoords: Coordinates = DistVivaldiActor.localPos.coordinates
    val mydist: Double = findDistanceFromParents(mycoords)
    val neighbourDistances: Seq[((Member, Coordinates), Double)] = candidateNodes
      .map(e => e -> findDistanceFromParents(e._2)).toSeq
      .sortWith(_._2 < _._2)
    val closestNeighbourDist: Double = if(neighbourDistances.nonEmpty) neighbourDistances.head._2 else Double.MaxValue

    log.debug(s"applyStarksAlgorithm(): " +
      s"\n my distance to parents: $mydist " +
      s"\n neighbours sorted by distance to parents:" +
      s" ${neighbourDistances.map(e => s"\n${e._1._1.address.toString} : ${e._2}")}")

    // algorithm description from Starks paper
    // Identify for each operator the so-called relay node, which is the next hop node towards the corresponding data source.
    //• Handle a pinned (= unary) operator: if the local node hosts the parent (or is the publisher), place it locally. Otherwise mark it
    //to be forwarded to the relay node.
    //• Handle an unpinned (= all other) operator: if all parents (= closer to stream operators in graph) of this operator have been
    // forwarded to and hosted on the same relay node, mark it to be forwarded this relay node. Otherwise place the operator locally

    val res: Future[HostInfo] = operator match {
      case s: BinaryQuery => {   // deploying any non-unary operator
        // possible cases:
        // 1. parents on same host -> forward to that host
        // 2. parents not on same host, self is not neighbour closest to both -> find closest neighbour to both (that is neither of the parents)
        // 3. parents not on same host, self is neighbour closest to both -> deploy on self
        // no parent exists for which there exists another parent that has a different address == two parents with different addresses exist
        val allParentsOnSameHost = !parentCoordinates.exists(p => parentCoordinates.exists(other => other._1.path.address.toString != p._1.path.address.toString))

        val relayNodeCandidates = neighbourDistances
          .filter(n => n._2 <= mydist)
          .filter(!_._1._1.hasRole("Publisher"))  // exclude publishers from eligible nodes to avoid bad placement decisions due to latency space inaccuracies
          .sortWith(_._2 < _._2)

        val relayNodeDist = if(relayNodeCandidates.nonEmpty) relayNodeCandidates.head._2 else Double.MaxValue

        log.debug(s"\n distances of all candidates to dependencies: " +
          s"\n ${relayNodeCandidates.map(e => s"\n ${e._1} : ${e._2}")}" +
          s"\n self: ${cluster.selfAddress.host} : $mydist")

        if (allParentsOnSameHost && mydist <= relayNodeDist) { // self is not included in neighbours
          log.debug(s"all parents on same host, deploying operator on self: ${cluster.selfAddress.toString}")
          Future { HostInfo(cluster.selfMember, operator, askerInfo.operatorMetrics) }

        } else if (allParentsOnSameHost && relayNodeDist <= mydist) {
          log.debug(s"all parents on same host, forwarding operator to neighbour: ${neighbourDistances.head._1._1.address.toString}")
          val updatedAskerInfo = HostInfo(cluster.selfMember, operator, askerInfo.operatorMetrics)
          forwardToNeighbour(relayNodeCandidates.take(k).map(_._1._1), updatedAskerInfo, operator, visitedMembers, dependencies)

        } else {  // dependencies not on same host
          val relayNodeCandidatesFiltered = relayNodeCandidates
            .filter(n => !parentCoordinates.keys.toList.map(d => d.path.address).contains(n._1._1.address)) // exclude both parents
            .sortWith(_._2 < _._2)
          // forward to a neighbour that lies between self and dependencies that is not a publisher and not one of the parents
          if (relayNodeCandidatesFiltered.nonEmpty && relayNodeCandidatesFiltered.head._2 <= mydist) {
            log.debug(s"parents not on same host, deploying operator on closest non-parent neighbour")
            val updatedAskerInfo = HostInfo(cluster.selfMember, operator, askerInfo.operatorMetrics)
            forwardToNeighbour(relayNodeCandidatesFiltered.take(k).map(_._1._1), updatedAskerInfo, operator, visitedMembers, dependencies)

          } else {
            log.debug(s"dependencies not on same host, deploying operator on self")
            Future { HostInfo(cluster.selfMember, operator, askerInfo.operatorMetrics) }
          }
        }
      }
      case _ => { // deploying an unary or leaf operator
        // deploy on this host if it is closer to dependency than all candidates
        if (mydist <= closestNeighbourDist) {
          log.debug(s"deploying stream operator on self: ${cluster.selfAddress.toString}, mydist: $mydist, neighbourDist: $closestNeighbourDist")
          Future { HostInfo(cluster.selfMember, operator, askerInfo.operatorMetrics) }

        } else {
          log.debug(s"forwarding operator $operator to neighbour: ${neighbourDistances.head._1._1.address.toString}," +
            s" neighbourDist: $closestNeighbourDist, mydist: $mydist")
          val updatedAskerInfo = HostInfo(cluster.selfMember, operator, askerInfo.operatorMetrics)
          forwardToNeighbour(neighbourDistances.take(k).map(_._1._1), updatedAskerInfo, operator, visitedMembers, dependencies)
        }
      }
    }

    res
  }


  def forwardToNeighbour(orderedCandidates: Seq[Member], askerInfo: HostInfo, operator: Query, visitedMembers: List[Member],
                         dependencies: Dependencies)
                        (implicit ec: ExecutionContext, cluster: Cluster, context: ActorContext): Future[HostInfo] = {
    // if retrieval of taskmanager fails, try other candidates
    def findForwardTaskManager(orderedCandidates: Seq[Member], operator: Query): Future[ActorRef] = {
      val request = TCEPUtils.getTaskManagerOfMember(cluster, orderedCandidates.head) recoverWith {
        case e: Throwable =>
          if (orderedCandidates.tail.nonEmpty) {
            log.warn(s"unable to contact taskManager ${orderedCandidates.head}, trying with ${orderedCandidates.tail}, cause \n $e")
            findForwardTaskManager(orderedCandidates.tail, operator)
          } else {
            log.warn(s"unable to contact taskManager ${orderedCandidates.head}, trying with ${cluster.selfMember}, cause \n $e")
            TCEPUtils.getTaskManagerOfMember(cluster, cluster.selfMember)
          }
      }

      request.onComplete {
        case Success(taskManager) =>
          log.debug(s"${cluster.selfMember} asking taskManager ${taskManager} to host $operator")
          SpecialStats.log(this.name, "placement", s"Starks asking $taskManager to host $operator")
        case scala.util.Failure(exception) =>
          log.error(s"no candidate taskManager among ($orderedCandidates) and self could be contacted")
          SpecialStats.log(this.name, "placement",s"failed Starks request hosting $operator: no task manager could be contacted")
          throw exception
      }
      request
    }

    def sendStarksTask(task: StarksTask, forwardTaskManager: ActorRef): Future[Message] = {
      val hostInfo = TCEPUtils.guaranteedDelivery(context, forwardTaskManager, task, singleTimeout = Timeout(15 seconds)) recoverWith {
        case e: Throwable =>
          SpecialStats.log(this.name, "placement", s"failed to deliver starksTask to $forwardTaskManager retrying... ${e.toString}".toUpperCase());
          sendStarksTask(task, forwardTaskManager)
      }

      hostInfo.onComplete { // logging callback
        case scala.util.Success(hostInfo) =>
          SpecialStats.log(this.name, "placement",s"deploying $operator on ${hostInfo.asInstanceOf[StarksTaskReply].hostInfo}")
          log.debug(s"forwardToNeighbour - received answer, deploying operator on ${hostInfo.asInstanceOf[StarksTaskReply].hostInfo}")
        case scala.util.Failure(exception) =>
          SpecialStats.log(this.name, "placement", s"could not deploy operator $operator, cause: $exception")
          log.error(s"no neighbour among $orderedCandidates or self could host operator $operator", exception)
          throw exception
      }
      hostInfo
    }

    val taskSize = SizeEstimator.estimate(StarksTask(operator, dependencies, askerInfo))
    val updatedAskerInfo = askerInfo.copy()
    updatedAskerInfo.visitedMembers = cluster.selfMember :: visitedMembers
    updatedAskerInfo.operatorMetrics.accPlacementMsgOverhead += taskSize
    val starksTask = StarksTask(operator, dependencies, updatedAskerInfo) // updatedAskerInfo's accMsgOverhead carries the overhead from all previous calls

    val deploymentResult = for {
      forwardTaskManagerRequest <- findForwardTaskManager(orderedCandidates, operator)
      starksTaskResponse <- sendStarksTask(starksTask, forwardTaskManagerRequest).mapTo[StarksTaskReply]
    } yield {
      val replySize = SizeEstimator.estimate(StarksTaskReply(_))
      val host = starksTaskResponse.hostInfo
      host.operatorMetrics.accPlacementMsgOverhead += replySize
      host
    }

    deploymentResult
  }

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

    val start = System.currentTimeMillis()
    log.info("MDCEP starting virtual placement")

    val req = for {
      memberCoords <- TCEPUtils.makeMapFuture(findPossibleNodesToDeploy(cluster).map(e => e.address -> getCoordinatesFromAddress(e.address, Some(rootOperator))).toMap)
    } yield {
      val clientNode = cluster.state.members.find(_.hasRole("Subscriber")).get
      val placement = traverseNetworkTree(clientNode.address, rootOperator, List())(memberCoords, publishers.map(e => e._1 -> e._2.path.address))
      placement.map(e => e._1 -> HostInfo(cluster.state.members.find(_.address == e._2).get, e._1, getPlacementMetrics(e._1)))
    }

    req.onComplete {
      case Failure(exception) => log.error(s"MDCEP - failed to complete virtual placement of ${Queries.getOperators(rootOperator).size} operators: \n${Queries.getOperators(rootOperator).mkString("\n")}", exception)
      case Success(value) => log.info("MDCEP - completed virtual placement after {}ms \n{}", System.currentTimeMillis() - start, value.mkString("\n"))
    }
    req
  }
  // visitedNodes is to break forwarding loops that occur in certain combinations of coordinates and query graphs
  def traverseNetworkTree(currentNode: Address, subQuery: Query, visitedNodes: List[Address])
                         (implicit memberCoords: Map[Address, Coordinates], publishers: Map[String, Address]): Map[Query, Address] = {
    /**
      * @return the relay node (node closest to current one in the direction of the parent's publishers) for each immediate parent operator of the given query
      */
    def getRelayNodes(curSubQuery: Query) : Map[Query, Address] = {
      // get the publishers for the current subtree
      def getParentPublishers(q: Query): Map[String, (Address, Coordinates)] = Queries.getPublishers(q)
        .map(p => p -> memberCoords.find(m => m._1 == publishers(p)).get).toMap

      val currentCoords = memberCoords(currentNode)
      val parentPublishers = getParentPublishers(curSubQuery)

      curSubQuery match {
        case s: StreamQuery =>
          val distancesToPublisher = memberCoords.map(m => m -> m._2.distance(parentPublishers(s.publisherName)._2)).toMap
          val currentDistance = distancesToPublisher.find(_._1._1 == currentNode).get._2
          // find the relay node (next hop), i.e. the node closest to the current node that is closer to the parent)
          val relayNode = if(currentNode == publishers(s.publisherName)) currentNode // RECURSION END
                          else distancesToPublisher.filter(_._2 < currentDistance).map(m => m -> m._1._2.distance(currentCoords)).minBy(_._2)._1._1._1
          Map(s -> relayNode)

        case u: UnaryQuery =>
          val parentRelayNodes = getRelayNodes(u.sq)
          val res = parentRelayNodes.updated(u, parentRelayNodes(u.sq))
          //log.info(s"unary operator ${u.getClass} relay nodes:: \n${res.mkString("\n")}")
          res

        case b: BinaryQuery =>

          val parent1RelayNodes = getRelayNodes(b.sq1)
          val relayNode1 = parent1RelayNodes(b.sq1) //parent1RelayNodes.map(r => r -> getParentPublishers(b.sq1).values.map(p => p._2.distance(memberCoords(r._2))).sum).minBy(_._2)._1
          val parent2RelayNodes = getRelayNodes(b.sq2)
          val relayNode2 = parent2RelayNodes(b.sq2) //parent2RelayNodes.map(r => r -> getParentPublishers(b.sq2).values.map(p => p._2.distance(memberCoords(r._2))).sum).minBy(_._2)._1
          // if parent is itself a binary operator, choose the relay node with minimum summed distance to both parent's publishers
          val binaryOperatorRelayNode: Address =
            // both parents have same relay node -> choose it
            if(relayNode1 == relayNode2) relayNode1
            // different relay nodes -> use the node closest to both relay nodes if both parents are streams
            else if(b.sq1.isInstanceOf[LeafQuery] && b.sq2.isInstanceOf[LeafQuery]) {
              memberCoords.filter(m => m._1 != relayNode1 && m._1 != relayNode2)
                          .map(m => m._1 -> (m._2.distance(memberCoords(relayNode1)) + m._2.distance(memberCoords(relayNode2)))).toList.sortBy(_._2).map(_._1)
                          .headOption.getOrElse(currentNode)
            // use the parent relay node with minimum distance to current node
            } else Seq(relayNode1, relayNode2).map(r => r -> memberCoords(r).distance(currentCoords)).minBy(_._2)._1

          val res = (parent1RelayNodes ++ parent2RelayNodes)
            .updated(b.sq1, relayNode1)
            .updated(b.sq2, relayNode2)
            .updated(b, binaryOperatorRelayNode)
          //log.info(s"binary operator ${b.getClass} relay nodes: \n${res.mkString("\n")}")
          res

        case s: SequenceQuery =>
          // same as StreamQuery, but use summed distances to the two publishers instead
          val distancesToPublishers = memberCoords.map(m => m -> (m._2.distance(parentPublishers(s.s1.publisherName)._2) + m._2.distance(parentPublishers(s.s2.publisherName)._2)))
          val currentDistance = currentCoords.distance(parentPublishers(s.s1.publisherName)._2) + currentCoords.distance(parentPublishers(s.s2.publisherName)._2)
          val relayNode = distancesToPublishers.filter(_._2 <= currentDistance).map(m => m -> m._1._2.distance(currentCoords)).minBy(_._2)._1._1._1
          Map(s -> relayNode)

        case _ => throw new IllegalArgumentException(s"unknown operator $subQuery")
      }
    }

    // get 1 relay node for each parent
    val curRelayNodes: Map[Query, Address] = getRelayNodes(subQuery)
    //log.debug(s"relayNodes for ${subQuery}; are (${curRelayNodes.size} of ${Queries.getOperators(subQuery).size}) :\n${curRelayNodes.mkString("\n")}")
    try {
      subQuery match {
        case s: StreamQuery =>
          if (currentNode == curRelayNodes(s))
            Map(s -> currentNode)
          else
            traverseNetworkTree(curRelayNodes(s), s, currentNode :: visitedNodes)

        case u: UnaryQuery => // always place on same node as parent
          val parentPlacements = traverseNetworkTree(currentNode, u.sq, List())
          parentPlacements.updated(u, parentPlacements(u.sq))

        case b: BinaryQuery =>
          // 1. parents are forwarded to same host -> forward self to that host
          if (curRelayNodes(b.sq1) == curRelayNodes(b.sq2)) {
            // parent relay node is current node -> deploy
            if(curRelayNodes(b.sq1) == currentNode || visitedNodes.contains(currentNode)) { // stop recursion
              val parent1Placement = traverseNetworkTree(currentNode, b.sq1, List())
              val parent2Placement = traverseNetworkTree(currentNode, b.sq2, List())
              (parent1Placement ++ parent2Placement).updated(b, currentNode)
            } else
              traverseNetworkTree(curRelayNodes(b.sq1), b, currentNode :: visitedNodes)
          } else {
            val relayNode1 = curRelayNodes(b.sq1)
            val relayNode2 = curRelayNodes(b.sq2)
            val minDistSumToRelayNodes = memberCoords.filter(m => m._1 != relayNode1 && m._1 != relayNode2)
              .map(m => m -> (m._2.distance(memberCoords(relayNode1)) + m._2.distance(memberCoords(relayNode2)))).minBy(_._2)
            // 2. parents not forwarded to same host, self is not neighbour closest to both -> find closest neighbour to both (that is neither of the parent relay nodes)
            if (minDistSumToRelayNodes._1._1 != currentNode && !visitedNodes.contains(currentNode)) {
              traverseNetworkTree(minDistSumToRelayNodes._1._1, b, currentNode :: visitedNodes)
              // 3. parents forwarded to different hosts, currentNode is closest neighbour to both -> deploy on self
            } else {
              val parent1Placement = traverseNetworkTree(curRelayNodes(b.sq1), b.sq1, List())
              val parent2Placement = traverseNetworkTree(curRelayNodes(b.sq2), b.sq2, List())
              (parent1Placement ++ parent2Placement).updated(b, currentNode)
            }
          }

        case s: SequenceQuery =>
          if (currentNode == curRelayNodes(s))
            Map(s -> currentNode)
          else
            traverseNetworkTree(curRelayNodes(s), s, currentNode :: visitedNodes)

        case _ => throw new IllegalArgumentException(s"unknown operator $subQuery")
      }
    } catch {
      case e: Throwable =>
        log.error(s"failed to traverse tree, current relay nodes are (${curRelayNodes.size} of ${Queries.getOperators(subQuery).size}) for subquery \n$subQuery", e)
        throw e
    }
  }
}