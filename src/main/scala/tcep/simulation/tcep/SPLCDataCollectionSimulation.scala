
package tcep.simulation.tcep

import java.io.{File, PrintStream}

import akka.actor.{ActorContext, ActorRef}
import akka.cluster.Cluster
import akka.pattern.ask
import akka.util.Timeout
import tcep.data.Queries.Query
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.transition.MAPEK.{GetPlacementStrategyName, GetTransitionStatus, IsDeploymentComplete}
import tcep.graph.transition.mapek.contrast.ContrastMAPEK.{GetCFM, GetContextData}
import tcep.graph.transition.mapek.contrast.FmNames._
import tcep.graph.transition.mapek.contrast.{CFM, RequirementChecker}
import tcep.placement.PlacementStrategy

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by Niels on 17.03.2018.
  */
/**
  * Simulation for collection of additional measurement data for SPLConqueror along with the usual logfiles
  * @param name name of the simulation log files
  * @param directory directory to store the logfiles
  * @param query query to simulate
  * @param publishers map of publisher name to their actorRef
  * @param allRecords reference to monitors' measurements
  * @param startingPlacementAlgorithm placement algorithm to start with
  * @param fixedSimulationProperties simulation properties that do not change during the simulation, supplied to SimulationRunner
  */
class SPLCDataCollectionSimulation(
                                  name: String, directory: Option[File], query: Query, transitionConfig: TransitionConfig, publishers: Map[String, ActorRef],
                                  consumer: ActorRef, startingPlacementAlgorithm: Option[PlacementStrategy], allRecords: AllRecords,
                                  fixedSimulationProperties: Map[Symbol, Int] = Map(), mapekType: String = "CONTRAST"
                                  )(implicit cluster: Cluster, context: ActorContext, baseEventRate: Double)
  extends Simulation(name, directory, query, transitionConfig, publishers, consumer, startingPlacementAlgorithm, allRecords, fixedSimulationProperties, mapekType) {

  implicit val timeout = Timeout(5 seconds)
  val splcOut: PrintStream = directory map { directory => new PrintStream(new File(directory, s"$name-$ldt" + "_splc.csv"))
  } getOrElse java.lang.System.out

  def splcHeader: String = (cfmFeatures ++ fixedCFMAttributes ++ variableCFMAttributes ++ metricElements).mkString(";")
  def logHeader: String =
        s"Time \t Algorithm" +
        s"\t latency \t estLatency" +
        s"\t hops \t estHops" +
        s"\t cpuUsage \t estLoad" +
        s"\t eventPublishRate \t eventArrivalRate " +
        s"\t MessageOverhead \t NetworkUsage " +
        s"\t TransitionStatus" +
        s"\t linkChanges"
        s"\t otherAlgorithmEstimates"

  override def startSimulationLog(algoName: String, startTime: FiniteDuration, interval: FiniteDuration, totalTime: FiniteDuration, callback: () => Any): Any = {

    var time = 0L
    val cfm: CFM = Await.result((queryGraph.mapek.knowledge ? GetCFM).mapTo[CFM], timeout.duration)
    val predictionHelper = new RequirementChecker(cfm.getFM, contextConfig = null)
    // SPLC measurement file header; status of all features is needed for SPLC
    splcOut.append(splcHeader)
    splcOut.println()
    out.append(logHeader)
    out.println()
    implicit val ec = context.system.dispatchers.lookup("blocking-io-dispatcher")
    simulation = context.system.scheduler.schedule(startTime, interval)(createCSVEntry())
    if(totalTime.toSeconds > 0)
      context.system.scheduler.scheduleOnce(totalTime)(stopSimulation())

    log.info("simulation logging setup complete")

    def createCSVEntry(): Unit = {
      for {
        deploymentComplete <- (queryGraph.mapek.knowledge ? IsDeploymentComplete).mapTo[Boolean]
        contextData <- (queryGraph.mapek.knowledge ? GetContextData).mapTo[Map[String, AnyVal]]
        if deploymentComplete && allRecords.allDefined && contextData.nonEmpty
        transitionStatus <- (queryGraph.mapek.knowledge ? GetTransitionStatus).mapTo[Int]
        strategyName <- (queryGraph.mapek.knowledge ? GetPlacementStrategyName).mapTo[String]
      } yield {

        //log.debug("called createCSVEntry()")
        val activePlacementAlgorithm = s"fs${strategyName}"
        val contextConfig = cfm.getCurrentContextConfig(contextData)
        val perfEstimates: Map[String, Map[Symbol, Double]] = predictionHelper.estimatePerformancePerAlgorithm(contextConfig)
        //log.debug("createCSVEntry() prediction helper created and performance estimated")
        val estimatedLatency = perfEstimates(activePlacementAlgorithm)('latency)
        val estimatedLoad = perfEstimates(activePlacementAlgorithm)('load)
        val estimatedHops = perfEstimates(activePlacementAlgorithm)('hops)
        //log.debug(s"createCSVEntry() - perfEstimates: ${perfEstimates} \n $activePlacementAlgorithm")
        //log.debug(s"metric values for active placement algorithm: $activePlacementAlgorithm: \n" +
          //"predicted            | actual" +
          //s"\nLatency:     $estimatedLatency | ${contextData(LATENCY)}" +
          //s"\nLoad:        $estimatedLoad | ${contextData(AVG_LOAD)}" +
          //s"\nHops:        $estimatedHops | ${contextData(MSG_HOPS)}")

        out.append(
          s"$time" +
            s"\t$strategyName" +
            s"\t${contextData(LATENCY)}" +
            s"\t$estimatedLatency" +
            s"\t${contextData(MSG_HOPS)}" +
            s"\t$estimatedHops" +
            s"\t${contextData(AVG_LOAD)}" +
            s"\t$estimatedLoad" +
            s"\t${contextData(EVENT_PUBLISHING_RATE)}" +
            s"\t${contextData(AVG_EVENT_ARRIVAL_RATE)}" +
            s"\t${contextData(OVERHEAD)}" +
            s"\t${contextData(NETWORK_USAGE)}" +
            s"\t$transitionStatus" +
            s"\t${contextData(LINK_CHANGES)}\t" +
            perfEstimates.filter(_._1 != activePlacementAlgorithm).flatMap(a => a._2.map(m => s"${m._2}")).mkString("\t")
        )
        out.println()

        val placementBooleans = allPlacementAlgorithms.map(pa => if(activePlacementAlgorithm == pa) 1 else 0).mkString(";") + ";"
        val measurementLine: String = {
          // root, system, mechanisms, placement algorithm are always active
          "1;1;1;1;" +
            // status of placement algorithm features
            placementBooleans +
            // context, network situation, fixedProperties and variableProperties - always active
            "1;1;1;1;" +
            fixedCFMAttributes.map(e => contextData.getOrElse(e, Double.NaN)).mkString(";") + ";" +
            variableCFMAttributes.map(e => contextData.getOrElse(e, Double.NaN)).mkString(";") + ";" +
            metricElements.map(e => contextData.getOrElse(e, Double.NaN)).mkString(";")
        }

        splcOut.append(measurementLine)
        splcOut.println()
        time += interval.toSeconds
      }
    }
  }

  override def stopSimulation(): Unit = {
    super.stopSimulation()
    splcOut.close()
  }
}
