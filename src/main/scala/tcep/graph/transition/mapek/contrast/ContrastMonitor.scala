package tcep.graph.transition.mapek.contrast

import java.util.concurrent.atomic.AtomicInteger
import akka.actor.{ActorRef, Address, Cancellable}
import akka.cluster.ClusterEvent.{MemberJoined, MemberLeft}
import akka.cluster.{Member, MemberStatus}
import akka.event.LoggingAdapter
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.Coordinates
import tcep.graph.transition.MAPEK._
import tcep.graph.transition.mapek.contrast.ContrastMAPEK.{GetOperatorTreeDepth, MonitoringDataUpdate}
import tcep.graph.transition.mapek.contrast.FmNames._
import tcep.graph.transition.{ChangeInNetwork, MAPEK, MonitorComponent}
import tcep.machinenodes.consumers.Consumer.{AllRecords, GetAllRecords}
import tcep.machinenodes.helper.actors.{GetNetworkHopsMap, NetworkHopsMap}
import tcep.utils.TCEPUtils.{getTaskManagerOfMember, makeMapFuture}
import tcep.utils.{SpecialStats, TCEPUtils}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Failure, Success}

/**
  * Created by Niels on 18.02.2018.
  *
  * M of the MAPE-K loop
  * responsible for retrieving and maintaining the data represented by CFM context attributes and features
  *
  * @param mapek reference to the running MAPEK instance
  * @param consumer container object for all metrics for which a monitor exists; contained records are set and updated by monitors
  */
class ContrastMonitor(mapek: MAPEK, consumer: ActorRef, fixedSimulationProperties: Map[Symbol, Int]) extends MonitorComponent {

  var updateNetworkDataScheduler: Cancellable = _
  var updateMonitorDataScheduler: Cancellable = _
  var checkOperatorHostStateScheduler: Cancellable = _
  var nodeCountChangerate: AtomicInteger = new AtomicInteger(0)
  var nodeChanges: List[Long] = List()
  var previousLatencies: Map[Long, Double] = Map()
  var previousLoads: Map[Long, Double] = Map()
  var previousArrivalsPerSecond: Map[Long, Double] = Map()
  var jitter: Double = 0.0d
  var avgVivaldiDistance: Double = 0.0d
  var vivaldiDistanceStdDev: Double = 0.0d
  var maxPubToClientLatency: Double = 0.0d
  var publisherMobility: Boolean = false
  var networkHopsMap: Map[Address, Map[Address, Int]] = Map()
  var vivDistances: Map[(Member, Member), Option[Double]] = Map()
  val isMininetSim: Boolean = ConfigFactory.load().getBoolean("constants.mininet-simulation")
  val isLocalSwarm: Boolean = ConfigFactory.load().getBoolean("constants.isLocalSwarm")

  def nodeCount: Int = cluster.state.members.count(m => m.status == MemberStatus.Up && m.hasRole("Candidate"))

  override def preStart(): Unit = {
    super.preStart()
    log.info("starting MAPEK Monitor")
    updateMonitorDataScheduler = this.context.system.scheduler.schedule(5 seconds, mapek.samplingInterval)(f = updateContextData())
    log.info(s"registered updateMonitorData scheduler: $updateMonitorDataScheduler")
    // update network state every half minute
    updateNetworkDataScheduler = this.context.system.scheduler.schedule(5 seconds, 15 seconds)(f = updateNetworkData())
    log.info(s"registered updateNetworkData scheduler: $updateNetworkDataScheduler")
    checkOperatorHostStateScheduler = this.context.system.scheduler.schedule(5 seconds, 15 seconds)(f = checkOperatorHostState())
    log.info(s"registered checkOperatorHostState scheduler: $checkOperatorHostStateScheduler")
    this.updateNetworkHops()
  }

  override def postStop() = {
    super.postStop()
    updateNetworkDataScheduler.cancel()
    updateMonitorDataScheduler.cancel()
    checkOperatorHostStateScheduler.cancel()
  }

  override def receive: Receive = {

    case MemberJoined(member) =>
      log.info(s"new member joined $member")
      nodeChanges = System.currentTimeMillis() :: nodeChanges
      updateNetworkData()

    case MemberLeft(member) =>
      nodeChanges = System.currentTimeMillis() :: nodeChanges
      updateNetworkData()

    case r: AddRequirement =>
      mapek.knowledge ! r
      mapek.analyzer ! r

    case r: RemoveRequirement => mapek.knowledge ! r

    case m => log.info(s"ignoring unknown message $m")
  }

  /**
    * periodically called to capture the latest state of the CFM context attributes and features as well as QoS metrics
    * stores this data in the ContrastKnowledge
    */
  def updateContextData(): Unit = {
    //val startTime = System.currentTimeMillis()

    for {
      deploymentComplete <- (mapek.knowledge ? IsDeploymentComplete).mapTo[Boolean]
      if deploymentComplete
      //_ <- Future { log.info(s"deploymentComplete: $deploymentComplete")}
      cAllRecords <- (consumer ? GetAllRecords).mapTo[AllRecords]
      //_ <- Future { log.info(s"allRecords defined: ${cAllRecords.allDefined}")}
      if cAllRecords.allDefined
      operatorTreeDepth <- (mapek.knowledge ? GetOperatorTreeDepth).mapTo[Int]
      //_ <- Future { log.info(s"operator tree depth: $operatorTreeDepth")}
      opCount <- (mapek.knowledge ? GetOperatorCount).mapTo[Int]
      currentLatency <- (mapek.knowledge ? GetAverageLatency(mapek.samplingInterval.toMillis)).mapTo[Double]
    } yield {

     // val currentLatency: Long = cAllRecords.recordLatency.lastMeasurement.get.toMillis // use avg latency of all events in last sampling interval
      val currentMsgHops: Int = cAllRecords.recordMessageHops.lastMeasurement.get
      val currentSystemLoad: Double = cAllRecords.recordAverageLoad.lastLoadMeasurement.get
      val currentArrivalRate: Int = cAllRecords.recordFrequency.lastMeasurement.get
      val currentPublishingRate: Double = cAllRecords.recordPublishingRate.lastRateMeasurement.get
      val eventMsgOverhead: Long = cAllRecords.recordMessageOverhead.lastEventOverheadMeasurement.get
      val networkUsage: Double = cAllRecords.recordNetworkUsage.lastUsageMeasurement.get

      previousArrivalsPerSecond += (System.currentTimeMillis() -> currentArrivalRate.toDouble / mapek.samplingInterval.toSeconds)
      previousArrivalsPerSecond = previousArrivalsPerSecond.filter(e => e._1 - System.currentTimeMillis() <= 60000)
      val avgArrivalRateLastMinute: Double = previousArrivalsPerSecond.values.sum / previousArrivalsPerSecond.size

      def calculateLoadMetrics(load: Double): Map[Symbol, Double] = {
        previousLoads += (System.currentTimeMillis() -> load)
        // keep only values of the last 120 seconds
        previousLoads = previousLoads.filter(e => System.currentTimeMillis() - e._1 <= 120000)

        val loadVariance: Double = ContrastMonitor.variance(previousLoads.values.toList)
        val loadStdDev: Double = ContrastMonitor.stdDev(previousLoads.values.toList)
        val loadGini: Double = ContrastMonitor.gini(previousLoads.values.toList)
        Map('variance -> loadVariance, 'stdDev -> loadStdDev, 'gini -> loadGini)
      }

      /**
        * retrieves the amount of link changes due to mobility in the last 20 and 60 seconds
        * file linkChanges.csv is updated by the simulation_*.py or mobility.py (wifi simulation) files running the mobility simulation
        * @return the amount of mobility-caused link changes of publisher nodes (from one mininet switch to the next)
        */
      def getLinkChanges: Int = {
        if(!isMininetSim) 0
        else {
          try {
            val bufferedSource = Source.fromFile("handovers.csv")
            var res = Map[Long, String]()
            //log.info(s"updateContextData() - getting link changes from file")

            for (line <- bufferedSource.getLines) {
              val cols = line.split(",").map(_.trim)
              res += (cols(0).toLong -> s"${cols(1)}: ${cols(2)} -> ${cols(3)}")
            }
            val sorted = res.toList.sorted
            //log.debug(s"getLinkChanges() - file read: ${res.toList.sorted}")
            if (res.nonEmpty) {

              //log.info(s"updateContextData() - time since last entry: ${System.currentTimeMillis() - sorted.last._1}ms")
              val last20 = res.filter(e => System.currentTimeMillis() - e._1 <= 20 * 1000)
              val last60 = res.filter(e => System.currentTimeMillis() - e._1 <= 60 * 1000)
              if (last60.size > 0) {
                publisherMobility = true
              } else {
                publisherMobility = false
              }
              //log.info(s"updateContextData() - number of link changes in the last 20s: ${last20.size}")
              //log.info(s"updateContextData() - number of link changes in the last 60s: ${last60.size}, mobility: ${publisherMobility}")
              bufferedSource.close()
              last60.size
            } else {
              bufferedSource.close()
              0
            }
          } catch {
            case e: Throwable =>
              log.error("could not get link changes from file", e)
              0
          }
        }
      }

      /**
        * iteratively calculates the jitter based on the difference between current and previous latency measurement
        *
        * @param currentLatency latency to use for the update
        * @return the jitter value updated by the currentLatency
        */
      def calculateJitterIteratively(currentLatency: Double): Double = {
        // keep only values of the last $interval seconds
        previousLatencies = previousLatencies.filter(e => System.currentTimeMillis() - e._1 <= 1.5d * mapek.samplingInterval.toMillis)

        if (previousLatencies.nonEmpty) {
          val prevLatencyKey = previousLatencies.keys.toList.sortWith(_ > _).head
          val prevLatency = previousLatencies(prevLatencyKey)
          // RFC 3550 https://tools.ietf.org/rfcmarkup?rfc=3550&draft=&url=#section-6.4.1
          //J(i) = J(i-1) + ( |D(i-1,i)| - J(i-1) ) / 16
          // division by 16 to dampen outliers
          jitter = jitter + ((math.abs(prevLatency - currentLatency) - jitter) / 16)
          previousLatencies += (System.currentTimeMillis() -> currentLatency)
          jitter
        } else {
          previousLatencies += (System.currentTimeMillis() -> currentLatency)
          jitter = 0.0d
          jitter
        }
      }

      // remove all node changes that are older than 2 minutes
      nodeChanges = nodeChanges.filter(t => System.currentTimeMillis() - t <= 2 * 60 * 1000)

      val loadMetrics = calculateLoadMetrics(currentSystemLoad)
      val contextDataUpdate: Map[String, AnyVal] = ContrastMonitor.createContextDataUpdate(
        vivDistances, operatorTreeDepth, opCount, currentLatency, currentMsgHops, currentSystemLoad, currentPublishingRate,
        avgArrivalRateLastMinute, eventMsgOverhead, networkUsage, getLinkChanges, calculateJitterIteratively(currentLatency), loadMetrics,
        nodeCount, nodeChanges.size, publisherMobility, maxPubToClientLatency, calculateAverageNodesInNHops(1), calculateAverageNodesInNHops(2),
        calculateAverageNodesInNHopsGini(1), calculateAverageNodesInNHopsGini(2),
        calculateAverageHopsBetweenNodes, fixedSimulationProperties)(log)
      // share data with knowledge to make it available to simulation csv writer and analyzer
      log.info(s"contextData update: \n${contextDataUpdate.mkString("\n")}")
      mapek.knowledge ! MonitoringDataUpdate(contextDataUpdate)
      //log.info(s"updateContextData() - complete after ${System.currentTimeMillis() - startTime}ms")
    }
  }

  /**
    * called periodically to update the network state (available nodes, vivaldi distances between all nodes)
    */
  def updateNetworkData(): Unit = {
    try {
      if (true /*mapek.knowledge.deploymentComplete*/ ) {

        val allNodes: Set[Member] = cluster.state.members.filter(m => m.status == MemberStatus.Up && !m.hasRole("Consumer"))

        for {
          vivCoordinates: Map[Member, Coordinates] <- makeMapFuture(allNodes.map(node => node -> TCEPUtils.getCoordinatesOfNode(cluster, node.address)).seq.toMap)
        } yield {
          //log.debug("updateNetworkState() - vivCoords retrieval successful")
          // calculate the distances between all vivaldi coordinate pairs
          //log.debug(s"\n coords: \n ${vivCoordinates.map(e => s"\n ${e._1} -> ${e._2}")} \n size: ${vivCoordinates.size}")
          var vivDistances: Map[(Member, Member), Option[Double]] = Map()
          for {
            node <- allNodes
            other <- allNodes.withFilter(n => !n.equals(node))
          } yield {
            //log.debug(s"updateNetworkState() - node: ${node.address.host.get}, other: ${other.address.host.get}")
            if (vivCoordinates.contains(node) && vivCoordinates.contains(other))
              vivDistances += ((node, other) -> Some(vivCoordinates(node).distance(vivCoordinates(other))))
            else vivDistances += ((node, other) -> None)
          }
          this.vivDistances = vivDistances
          //log.debug(s"updateNetworkState() - viv distances: \n ${vivDistances.map(e => s"\n${e._1._1.address} -> ${e._1._2.address} : ${e._2}")}")

          this.maxPubToClientLatency = vivDistances.filter(pair =>
            pair._1._1.hasRole("Publisher") && pair._1._2.hasRole("Subscriber") || pair._1._1.hasRole("Subscriber") && pair._1._2.hasRole("Publisher"))
            .minBy(entry => entry._2.getOrElse(Double.MaxValue))._2.get
          this.avgVivaldiDistance = vivDistances.values.flatten.sum / vivDistances.values.flatten.size
          this.vivaldiDistanceStdDev = ContrastMonitor.stdDev(vivDistances.values.flatten.toList)
          this.updateNetworkHops()
          /*log.info(s"updateNetworkState() - update complete" +
            s"\n maximum vivaldi distance between client and publishers: ${ContrastMonitor.scale(maxPubToClientLatency, 2)}" +
            s"\n avg vivaldi distance: $avgVivaldiDistance, stddev: $vivaldiDistanceStdDev")
           */
        }
      }
    } catch {
      case e: Throwable => log.error(e, s"updateNetworkState() - error while updating network state")
    }
  }

  /**
    * check if any operator hosts are down; executed periodically
    */
  def checkOperatorHostState(): Unit = {
    for {
      deploymentComplete <- (mapek.knowledge ? IsDeploymentComplete).mapTo[Boolean]
      if deploymentComplete
      operators <- (mapek.knowledge ? GetOperators).mapTo[List[ActorRef]]
    } yield {

      // check if an operator of the graph has an unreachable host (and no backup)
      // if yes, notify analyzer to trigger transition and/or placement algorithm execution
      val unreachableNodes: Set[Address] = cluster.state.unreachable.map(m => m.address)
      operators.foreach { op =>
        val host: Address = op.path.address
        if(unreachableNodes.contains(host)) {
          log.info(s"host $host of operator $op is unreachable, notifying analyzer of Network Change")
          mapek.analyzer ! ChangeInNetwork
        }
      }
    }
  }

  def calculateAverageHopsBetweenNodes: Double = {
    //log.info(s"localSwarm: $isLocalSwarm mininet $isMininetSim \n networkHopsMap ${networkHopsMap.mkString("\n")} \n VivaldiMap ${vivDistances.mkString("\n")}")
    // distributed docker swarm, or mininet with known link latency
    if(isLocalSwarm) 1
    else {
      val networkHops = if (!isMininetSim) networkHopsMap.flatMap(_._2.values).toList
                        else vivDistances.filter(_._2.isDefined).map(e => estimateNetworkHopsInMininet(e._2.get)).toList
      networkHops.sum.toDouble / networkHops.size
    }
  }

  def estimateNetworkHopsInMininet(latency: Double) = math.round(latency / fixedSimulationProperties.getOrElse('baseLatency, 30)).toInt

  /**
    * calculates the average amount of nodes that can be reached in N hops
    * (hops on the processing node network; number of hops inferred from the average link latency)
    * @param hops N
    * @return the average number of nodes reachable in N hops
    */
  def calculateAverageNodesInNHops(hops: Int): Double = {
    val nodesInNHops: Map[Address, Int] = nodesWithinNHops(hops)
    nodesInNHops.values.sum.toDouble / nodesInNHops.size
  }

  /**
    * Calculates the gini coefficient of the number of nodes that can be reached from each node in n network hops.
    * This is meant to gauge the topology of the network; it indicates how centralized it is and how deep branches are
    * i.e. a simple star topology with only one node (the center) being reachable for all nodes except the center
    * will lead to a high value, whereas a full mesh would lead to a value of zero
    * @param hops network hops to reach other nodes in
    * @return gini coefficient of the amount of nodes reachable in n hops from each node
    */
  def calculateAverageNodesInNHopsGini(hops: Int): Double = {
    val connectionDegreesNHops = nodesWithinNHops(hops)
    val nHopGiniCoefficient = ContrastMonitor.gini(connectionDegreesNHops.values.toList)
    nHopGiniCoefficient
  }

  def nodesWithinNHops(n: Int) = {
    if(isMininetSim) nodesWithinNHopsMininet(n)
    else if(isLocalSwarm) nodesWithinNHopsLocalSwarm(n)
    else nodesWithinNHopsTraceroute(n)
  }
  /**
    * estimates the amount of nodes reachable in n hops within a mininet simulation with a known base link latency
    * @param n number of network hops (approximated by using the base link latency)
    * @return the number of nodes that can be reached from each node in n hops
    */
  def nodesWithinNHopsMininet(n: Int): Map[Address, Int] = {
    val allNodes = cluster.state.members.filter(m => m.status == MemberStatus.Up && !m.hasRole("Consumer"))
    val networkHops = vivDistances.filter(_._2.isDefined).map(e => e._1 -> estimateNetworkHopsInMininet(e._2.get))
    allNodes.map(node => node.address -> networkHops.filter(_._1._1.equals(node)).count(_._2 <= n)).toMap
  }
  // full mesh in local docker swarm -> entire cluster is always reachable
  def nodesWithinNHopsLocalSwarm(n: Int): Map[Address, Int] = {
    val allNodes = cluster.state.members.filter(m => m.status == MemberStatus.Up && !m.hasRole("Consumer"))
    allNodes.map(node => node.address -> allNodes.size).toMap
  }
  def nodesWithinNHopsTraceroute(n: Int) : Map[Address, Int] = this.networkHopsMap.map(addr => addr._1 -> addr._2.count(_._2 <= n))

  /**
    * update the networkHopsMap containing the network hops between all address pairs; hops are measured using traceroute
    */
  def updateNetworkHops(addresses: List[Member] = cluster.state.members.filter(m => m.status == MemberStatus.up && m != cluster.selfMember && !m.hasRole("Consumer")).toList) = {
    // traceroute does not work in a local docker swarm and mininet
    if(!isMininetSim && !isLocalSwarm) {
      implicit val timeout: Timeout = 10 seconds
      val requests = TCEPUtils.makeMapFuture(addresses.map(node => node -> {
        for {
          taskManager <- getTaskManagerOfMember(cluster, node)
          hopsMap <- (taskManager ? GetNetworkHopsMap).mapTo[NetworkHopsMap]
        } yield {
          hopsMap.hopsMap
        }
      }).toMap)
      requests.onComplete {
        case Success(hopsMap) =>
          this.networkHopsMap = hopsMap.map(e => e._1.address -> e._2)
        //log.debug(s"updateNetworkHops - update successful: \n ${hopsMap.map(e => e._1 -> e._2.mkString("\n")).mkString("\n")}")
        case Failure(exception) => log.error(exception, "failed to update network hops map for all addresses")
      }
    }
  }
}

object ContrastMonitor {

  /**
    * rounds a double to the specified number of digits
    * @param value the value
    * @param digits the number of digits to round to
    * @return the rounded value
    */
  def scale(value: Double, digits: Int): Double = BigDecimal(value).setScale(digits, BigDecimal.RoundingMode.HALF_UP).toDouble

  /**
    * calculates the variance of the passed list of values
    * @param values list of values
    * @return the variance
    */
  def variance(values: List[AnyVal]): Double = {
    val vals = values.map {
      case i: Int => i.toDouble
      case v: Double => v
    }
    val n = vals.size
    val avg = vals.sum / n
    val variance = vals.map(v => math.pow(v - avg, 2)).sum / n
    variance
  }

  /**
    * standard deviation of the passed values
    * @param values the values
    * @return the standard deviation
    */
  def stdDev(values: List[AnyVal]): Double = math.sqrt(variance(values))

  /**
    * gini coefficient of the passed list of values
    * "relative mean difference", measure of inequality in a collection of values: 0 - total equality, 1 - maximum inequality
    * @param values the values
    * @return the gini coefficient
    */
  def gini(values: List[AnyVal]): Double = {

    val vals = values.map {
      case i: Int => i.toDouble
      case v: Double => v
    }
    if(vals.sum > 0) {
      val n = values.size
      val diffs: Double = vals.map(v =>
        vals.map(other => math.abs(v - other))).flatten.sum
      val denom = 2 * n * vals.sum
      diffs / denom
    } else {
      0.0d
    }
  }

  def createContextDataUpdate(vivDistances: Map[(Member, Member), Option[Double]],
                              operatorTreeDepth: Int,
                              opCount: Int,
                              currentLatency: Double,
                              currentMsgHops: Int,
                              currentSystemLoad: Double,
                              currentPublishingRate: Double,
                              avgArrivalRateLastMinute: Double,
                              msgOverhead: Long,
                              networkUsage: Double,
                              linkChanges: Int,
                              jitter: Double,
                              loadMetrics: Map[Symbol, Double],
                              nodeCount: Int,
                              nodeChanges: Int,
                              mobility: Boolean,
                              maxPubToClientLatency: Double,
                              avgNodesIn1Hops: Double,
                              avgNodesIn2Hops: Double,
                              avgNodesIn1HopGini: Double,
                              avgNodesIn2HopGini: Double,
                              avgHopsBetweenNodes: Double,
                              fixedSimulationProperties: Map[Symbol, Int])(implicit log: LoggingAdapter): Map[String, AnyVal] = {

    val avgDistance: Double = vivDistances.values.flatten.sum / vivDistances.values.flatten.size
    val distanceStdDev = stdDev(vivDistances.values.flatten.toList)
    var contextDataUpdate: Map[String, AnyVal] = Map()
    contextDataUpdate += (LATENCY -> scale(currentLatency, 0))
    contextDataUpdate += (MSG_HOPS -> currentMsgHops)
    contextDataUpdate += (AVG_LOAD -> scale(currentSystemLoad, LOAD_DIGITS))
    contextDataUpdate += (OVERHEAD -> msgOverhead)
    contextDataUpdate += (NETWORK_USAGE -> scale(networkUsage, NETWORK_USAGE_DIGITS))
    // above only for measurement files, not for context config
    contextDataUpdate += (LOAD_VARIANCE -> scale(loadMetrics('variance), LOAD_DIGITS))
    contextDataUpdate += (AVG_EVENT_ARRIVAL_RATE -> scale(avgArrivalRateLastMinute, EVENT_FREQUENCY_DIGITS))
    contextDataUpdate += (EVENT_PUBLISHING_RATE -> scale(currentPublishingRate, EVENT_FREQUENCY_DIGITS))
    contextDataUpdate += (NODECOUNT -> nodeCount)
    contextDataUpdate += (NODECOUNT_CHANGERATE -> nodeChanges)
    contextDataUpdate += (NODE_TO_OP_RATIO -> scale(nodeCount.toDouble / opCount, NODE_TO_OP_RATIO_DIGITS))
    contextDataUpdate += (OPERATOR_TREE_DEPTH -> operatorTreeDepth)
    contextDataUpdate += (JITTER -> scale(jitter, JITTER_DIGITS))
    contextDataUpdate += (AVG_VIV_DISTANCE -> scale(avgDistance, AVG_VIV_DISTANCE_DIGITS))
    contextDataUpdate += (VIV_DISTANCE_STDDEV -> scale(distanceStdDev, AVG_VIV_DISTANCE_DIGITS))
    contextDataUpdate += (LINK_CHANGES -> linkChanges)
    contextDataUpdate += (MOBILITY -> mobility)
    contextDataUpdate += (MAX_PUB_TO_CLIENT_PING -> scale(maxPubToClientLatency, LATENCY_DIGITS))
    //contextDataUpdate += (BASE_LATENCY -> fixedSimulationProperties.getOrElse('baseLatency, 30))
    contextDataUpdate += (MAX_TOPOLOGY_HOPS_PUB_TO_CLIENT -> fixedSimulationProperties.getOrElse('maxPubToClientHops, 0))
    contextDataUpdate += (GINI_CONNECTION_DEGREE_1_HOP -> scale(avgNodesIn1HopGini, GINI_DIGITS))
    contextDataUpdate += (GINI_CONNECTION_DEGREE_2_HOPS -> scale(avgNodesIn2HopGini, GINI_DIGITS))
    contextDataUpdate += (AVG_NODES_IN_1_HOP -> scale(avgNodesIn1Hops, GINI_DIGITS))
    contextDataUpdate += (AVG_NODES_IN_2_HOPS -> scale(avgNodesIn2Hops, GINI_DIGITS))
    contextDataUpdate += (AVG_HOPS_BETWEEN_NODES -> scale(avgHopsBetweenNodes, GINI_DIGITS))
    contextDataUpdate
  }

}

