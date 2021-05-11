
package tcep.simulation.tcep

import java.io.{File, PrintStream}
import akka.actor.{ActorContext, ActorRef}
import akka.cluster.Cluster
import akka.dispatch.MessageDispatcher
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import tcep.data.Queries.Query
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.transition.MAPEK.{GetPlacementStrategyName, GetTransitionStatus, IsDeploymentComplete}
import tcep.graph.transition.mapek.contrast.ContrastMAPEK.{GetCFM, GetContextData}
import tcep.graph.transition.mapek.contrast.FmNames._
import tcep.graph.transition.mapek.contrast.{CFM, RequirementChecker}
import tcep.graph.transition.mapek.learnon.LearnOnMAPEK
import tcep.graph.transition.mapek.learnon.LearnOnMessages.GetLearnOnLogData
import tcep.graph.transition.mapek.learnon.LightweightMessages.GetLightweightLogData
import tcep.graph.transition.mapek.learnon.ModelRLMessages.GetModelRLLogData
import tcep.machinenodes.consumers.Consumer.{AllRecords, GetAllRecords}
import tcep.placement.PlacementStrategy

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
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
                                  name: String, query: Query, transitionConfig: TransitionConfig, publishers: Map[String, ActorRef],
                                  consumer: ActorRef, startingPlacementAlgorithm: Option[PlacementStrategy],
                                  mapekType: String = "CONTRAST"
                                  )(implicit cluster: Cluster, context: ActorContext, baseEventRate: Double, directory: Option[File],
                                    fixedSimulationProperties: Map[Symbol, Int] = Map(), pimPaths: (String, String) = ("", ""))
  extends Simulation(name, query, transitionConfig, publishers, consumer, startingPlacementAlgorithm, mapekType) {

  implicit val timeout = Timeout(5 seconds)
  val splcOut: PrintStream = directory map { directory => new PrintStream(new File(directory, s"$name-$ldt" + "_splc.csv"))
  } getOrElse java.lang.System.out

  val learningModel = ConfigFactory.load().getString("constants.mapek.learning-model").toLowerCase
  var cfm: Option[CFM] = None
  var predictionHelper: Option[RequirementChecker] = None

  def splcHeader: String = (cfmFeatures ++ fixedCFMAttributes ++ variableCFMAttributes ++ metricElements).mkString(";")
  /*def logHeader: String = s"Time \t Algorithm" +
        s"\t latency \t estLatency" +
        s"\t hops \t estHops" +
        s"\t cpuUsage \t estLoad" +
        s"\t eventPublishRate \t eventArrivalRate " +
        s"\t MessageOverhead \t NetworkUsage " +
        s"\t TransitionStatus" +
    s"\t linkChanges"*/
  /*def logHeader: String = ConfigFactory.load().getString("constants.mapek.learning-model")*/
  def logHeader: String = {
    if (this.learningModel.equals("lightweight")) {
      s"Time \t Algorithm" +
        s"\t latency" +
        s"\t cpuUsage" +
        s"\t eventPublishRate \t eventArrivalRate " +
        s"\t TransitionStatus" +
        s"\t AlgorithmFitnesses \t AlgorithmSelectionProbabilities "
    } else if (this.learningModel.equals("learnon")) {
      s"Time \t Algorithm" +
        s"\t latency \t estLatency" +
        s"\t cpuUsage \t estLoad" +
        s"\t eventPublishRate \t eventArrivalRate " +
        s"\t TransitionStatus \t latencyPredictions " +
        s"\t loadPredictions"
    } else {
      s"Time \t Algorithm" +
        s"\t latency" +
        s"\t cpuUsage" +
        s"\t eventPublishRate \t eventArrivalRate " +
        s"\t TransitionStatus" +
        s"\t CurrentQVals \t QTableUpdateTime "
    }
  }

  override def startSimulationLog(algoName: String, startTime: FiniteDuration, interval: FiniteDuration, totalTime: FiniteDuration, callback: () => Any): Any = {

    var time = 0L
    log.info(s"starting splcData simulation log, asking ${queryGraph.mapek.knowledge} for cfm")
    for { res <- (queryGraph.mapek.knowledge ? GetCFM).mapTo[CFM] } yield {
      log.info("received cfm from knowledge")
      cfm = Some(res)
      predictionHelper = Some(new RequirementChecker(cfm.get.getFM, contextConfig = null))

      // SPLC measurement file header; status of all features is needed for SPLC
      splcOut.append(splcHeader)
      splcOut.println()
      out.append(logHeader)
      out.println()
      simulation = context.system.scheduler.schedule(startTime, interval)(createCSVEntry())
      if (totalTime.toSeconds > 0)
        context.system.scheduler.scheduleOnce(totalTime)(stopSimulation())

      log.info("simulation logging setup complete")

      def createCSVEntry(): Unit = {
        //log.info("called createCSVEntry()")
        try {
          for {
            cAllRecords <- (consumer ? GetAllRecords).mapTo[AllRecords]
            //_ <- Future { log.info(s"all defined: ${cAllRecords.allDefined} cAllRecords: $cAllRecords")}
            if recordsArrived(cAllRecords) && cAllRecords.allDefined
            deploymentComplete <- (queryGraph.mapek.knowledge ? IsDeploymentComplete).mapTo[Boolean]
            //_ <- Future { log.info(s"deployment complete: $deploymentComplete")}
            contextData <- (queryGraph.mapek.knowledge ? GetContextData).mapTo[Map[String, AnyVal]]
            //_ <- Future { log.info(s"contextData: ${contextData.mkString("\n")}")}
            if deploymentComplete && contextData.nonEmpty
            transitionStatus <- (queryGraph.mapek.knowledge ? GetTransitionStatus).mapTo[Int]
            //_ <- Future { log.info(s"transitionStatus: $transitionStatus")}
            strategyName <- (queryGraph.mapek.knowledge ? GetPlacementStrategyName).mapTo[String]
          } yield {
            val activePlacementAlgorithm = s"fs${strategyName}"
            //log.info(s"requests complete, active placement: $activePlacementAlgorithm")
            //CONTRAST
            //val contextConfig = cfm.getCurrentContextConfig(contextData)
            //log.info("loaded current context config")
            /*predictionHelper = new RequirementChecker(cfm.getFM, contextConfig = null, perfModels = uPerfModels)
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
          */

            if (this.learningModel.equals("learnon")) {
              // map per metric with a list of hypothesis tuples of the form (hypothesisPrediction, hypothesisWeight, hypothesisId)
              for {learnOnLogData <- (queryGraph.mapek.asInstanceOf[LearnOnMAPEK].learningModel ? GetLearnOnLogData(cfm.get, contextData, strategyName)).mapTo[mutable.HashMap[String, List[List[Double]]]]} yield {
                //log.info(s"logData received: ${learnOnLogData.mkString("\n")}")
                val estimatedLatency = learnOnLogData.get("latency").get.head.head
                val estimatedLoad = learnOnLogData.get("load").get.head.head
                out.append(
                  s"$time" +
                    s"\t$strategyName" +
                    s"\t${contextData(LATENCY)}" +
                    s"\t$estimatedLatency" +
                    s"\t${contextData(AVG_LOAD)}" +
                    s"\t$estimatedLoad" +
                    s"\t${contextData(EVENT_PUBLISHING_RATE)}" +
                    s"\t${contextData(AVG_EVENT_ARRIVAL_RATE)}" +
                    s"\t$transitionStatus" +
                    s"\t${learnOnLogData.get("latency").get}" +
                    s"\t${learnOnLogData.get("load").get}\t"
                )
                //log.info("LearnOn data added!")
                out.println()
              }

            } else if (this.learningModel.equals("lightweight")) {
              for {learnOnLogData <- (queryGraph.mapek.asInstanceOf[LearnOnMAPEK].learningModel ? GetLightweightLogData).mapTo[(List[Any], List[Any])]} yield {
                out.append(
                  s"$time" +
                    s"\t$strategyName" +
                    s"\t${contextData(LATENCY)}" +
                    s"\t${contextData(AVG_LOAD)}" +
                    s"\t${contextData(EVENT_PUBLISHING_RATE)}" +
                    s"\t${contextData(AVG_EVENT_ARRIVAL_RATE)}" +
                    s"\t$transitionStatus" +
                    s"\t${learnOnLogData._1.toString()}" +
                    s"\t${learnOnLogData._2.toString()}\t"
                )
                log.info("Baseline data added!")
                out.println()
              }
            } else if (this.learningModel.equals("rl")) {
              for {learnOnLogData <- (queryGraph.mapek.asInstanceOf[LearnOnMAPEK].learningModel ? GetModelRLLogData).mapTo[ListBuffer[Double]]} yield {
                log.info(s"learnOn RL log data: \n $learnOnLogData")
                out.append(
                  s"$time" +
                    s"\t$strategyName" +
                    s"\t${contextData(LATENCY)}" +
                    s"\t${contextData(AVG_LOAD)}" +
                    s"\t${contextData(EVENT_PUBLISHING_RATE)}" +
                    s"\t${contextData(AVG_EVENT_ARRIVAL_RATE)}" +
                    s"\t$transitionStatus" +
                    s"\t${learnOnLogData.slice(0, learnOnLogData.size - 1)}" +
                    s"\t${learnOnLogData.last}"
                )
                log.info("RL data added!")
                out.println()
              }
            } else {
              out.append(
                s"$time" +
                  s"\t$strategyName" +
                  s"\t${contextData(LATENCY)}" +
                  s"\t-" +
                  s"\t${contextData(AVG_LOAD)}" +
                  s"\t-" +
                  s"\t${contextData(EVENT_PUBLISHING_RATE)}" +
                  s"\t${contextData(AVG_EVENT_ARRIVAL_RATE)}" +
                  s"\t$transitionStatus" +
                  s"\t-1" +
                  s"\t-1\t"
              )
              log.info("Failed to get LogData!")
              out.println()
            }


            val placementBooleans = allPlacementAlgorithms.map(pa => if (activePlacementAlgorithm == pa) 1 else 0).mkString(";") + ";"
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
        } catch {
          case e: Throwable =>
            log.error(s"failed to create csv entry", e)
            throw e
        }
      }
    }
  }

  override def stopSimulation(): Unit = {
    super.stopSimulation()
    splcOut.close()
  }
}
