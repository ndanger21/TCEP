package tcep.placement.manets

import akka.actor.{ActorContext, ActorRef}
import akka.cluster.{Cluster, Member}
import akka.util.Timeout
import org.discovery.vivaldi.{Coordinates, DistVivaldiActor}
import tcep.data.Queries
import tcep.data.Queries.{BinaryQuery, Query, QueryDependencyMap}
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
      val outputBandwidthEstimates = Queries.estimateOutputBandwidths(operator)
      if (askerInfo.visitedMembers.contains(cluster.selfMember)) { // prevent forwarding in circles, i.e. (rare) case when distances are equal
        val hostInfo = HostInfo(cluster.selfMember, operator, askerInfo.operatorMetrics, askerInfo.visitedMembers) // if this node is asked a second time, stop forwarding and deploy on self
        for {
          bdpupdate <- this.updateOperatorToParentBDP(operator, hostInfo.member, dependencies.parents, outputBandwidthEstimates)
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
            log.info(s"the number of available candidates are: ${candidates.size}, candidates: ${candidates.toList}")
            log.info(s"applyStarksAlgorithm() - dependencyCoords:\n ${parentCoordinates.map(d => d._1.path.address.toString + " -> " + d._2.toString)}")
            placementMetrics += operator -> askerInfo.operatorMetrics // add message overhead from callers (who forwarded to this node); if this is the first caller, these should be zero
            //SpecialStats.debug(s"$this", s"applying starks on ${cluster.selfAddress} to $operator")
            applyStarksAlgorithm(parentCoordinates, candidateCoordinates, askerInfo, operator, askerInfo.visitedMembers, dependencies)
          }
          bdp <- this.updateOperatorToParentBDP(operator, hostInfo.member, dependencies.parents, outputBandwidthEstimates)
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
            log.info(s"$this", s"found host ${host.member}, after $latencyInMillis ms")
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

    log.info(s"applyStarksAlgorithm(): " +
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

        log.info(s"\n distances of all candidates to dependencies: " +
          s"\n ${relayNodeCandidates.map(e => s"\n ${e._1} : ${e._2}")}" +
          s"\n self: ${cluster.selfAddress.host} : $mydist")

        if (allParentsOnSameHost && mydist <= relayNodeDist) { // self is not included in neighbours
          log.info(s"all parents on same host, deploying operator on self: ${cluster.selfAddress.toString}")
          Future { HostInfo(cluster.selfMember, operator, askerInfo.operatorMetrics) }

        } else if (allParentsOnSameHost && relayNodeDist <= mydist) {
          log.info(s"all parents on same host, forwarding operator to neighbour: ${neighbourDistances.head._1._1.address.toString}")
          val updatedAskerInfo = HostInfo(cluster.selfMember, operator, askerInfo.operatorMetrics)
          forwardToNeighbour(relayNodeCandidates.take(k).map(_._1._1), updatedAskerInfo, operator, visitedMembers, dependencies)

        } else {  // dependencies not on same host
          val relayNodeCandidatesFiltered = relayNodeCandidates
            .filter(n => !parentCoordinates.keys.toList.map(d => d.path.address).contains(n._1._1.address)) // exclude both parents
            .sortWith(_._2 < _._2)
          // forward to a neighbour that lies between self and dependencies that is not a publisher and not one of the parents
          if (relayNodeCandidatesFiltered.nonEmpty && relayNodeCandidatesFiltered.head._2 <= mydist) {
            log.info(s"parents not on same host, deploying operator on closest non-parent neighbour")
            val updatedAskerInfo = HostInfo(cluster.selfMember, operator, askerInfo.operatorMetrics)
            forwardToNeighbour(relayNodeCandidatesFiltered.take(k).map(_._1._1), updatedAskerInfo, operator, visitedMembers, dependencies)

          } else {
            log.info(s"dependencies not on same host, deploying operator on self")
            Future { HostInfo(cluster.selfMember, operator, askerInfo.operatorMetrics) }
          }
        }
      }
      case _ => { // deploying an unary or leaf operator
        // deploy on this host if it is closer to dependency than all candidates
        if (mydist <= closestNeighbourDist) {
          log.info(s"deploying stream operator on self: ${cluster.selfAddress.toString}, mydist: $mydist, neighbourDist: $closestNeighbourDist")
          Future { HostInfo(cluster.selfMember, operator, askerInfo.operatorMetrics) }

        } else {
          log.info(s"forwarding operator $operator to neighbour: ${neighbourDistances.head._1._1.address.toString}," +
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
          log.info(s"${cluster.selfMember} asking taskManager ${taskManager} to host $operator")
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
          log.info(s"forwardToNeighbour - received answer, deploying operator on ${hostInfo.asInstanceOf[StarksTaskReply].hostInfo}")
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

}