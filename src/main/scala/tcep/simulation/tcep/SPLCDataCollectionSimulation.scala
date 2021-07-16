
package tcep.simulation.tcep

import akka.actor.{ActorContext, ActorRef, Address}
import akka.cluster.Cluster
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import tcep.data.Queries
import tcep.data.Queries.Query
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.qos.OperatorQosMonitor.Samples
import tcep.graph.transition.MAPEK._
import tcep.graph.transition.mapek.ExchangeablePerformanceModelMAPEK
import tcep.graph.transition.mapek.ExchangeablePerformanceModelMAPEK.GetContextSample
import tcep.graph.transition.mapek.contrast.CFM
import tcep.graph.transition.mapek.contrast.ContrastMAPEK.{GetCFM, GetContextData}
import tcep.graph.transition.mapek.contrast.FmNames._
import tcep.graph.transition.mapek.learnon.LearnOnMAPEK
import tcep.graph.transition.mapek.learnon.LearnOnMessages.GetLearnOnLogData
import tcep.graph.transition.mapek.learnon.LightweightMessages.GetLightweightLogData
import tcep.graph.transition.mapek.learnon.ModelRLMessages.GetModelRLLogData
import tcep.machinenodes.consumers.Consumer.{AllRecords, GetAllRecords}
import tcep.prediction.PredictionHelper.{EndToEndLatencyAndThroughputPrediction, OfflineAndOnlinePredictions, Throughput}
import tcep.prediction.QueryPerformancePredictor.GetPredictionForPlacement
import tcep.utils.TCEPUtils

import java.io.{File, PrintStream}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
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
                                  consumer: ActorRef, startingPlacementAlgorithm: Option[String],
                                  mapekType: String = "CONTRAST"
                                  )(implicit cluster: Cluster, context: ActorContext, publisherEventRates: Map[String, Throughput], directory: Option[File],
                                    fixedSimulationProperties: Map[Symbol, Int] = Map(), pimPaths: (String, String) = ("", ""))
  extends Simulation(name, query, transitionConfig, publishers, consumer, startingPlacementAlgorithm, mapekType) {

  implicit val timeout = Timeout(5 seconds)
  val splcOut: PrintStream = directory map { directory => new PrintStream(new File(directory, s"$name-$ldt" + "_splc.csv"))
  } getOrElse java.lang.System.out

  val learningModel = if(mapekType == "LearnOn") ConfigFactory.load().getString("constants.mapek.learning-model").toLowerCase else ""
  var cfm: Option[CFM] = None
  var querySize: Option[Int] = None

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
        //s"\t eventPublishRate
        s"\t eventArrivalRate " +
        s"\t TransitionStatus" +
        s"\t AlgorithmFitnesses \t AlgorithmSelectionProbabilities "
    } else if (this.learningModel.equals("learnon")) {
      s"Time \t Algorithm" +
        s"\t latency \t estLatency" +
        s"\t cpuUsage \t estLoad" +
        s"\t eventPublishRate \t eventArrivalRate " +
        s"\t TransitionStatus \t latencyPredictions " +
        s"\t loadPredictions"
    } else if(this.learningModel.equals("rl")) {
      s"Time \t Algorithm" +
        s"\t latency" +
        s"\t cpuUsage" +
        s"\t eventPublishRate \t eventArrivalRate " +
        s"\t TransitionStatus" +
        s"\t CurrentQVals \t QTableUpdateTime "
    } else if(this.mapekType == "ExchangeablePerformanceModel") {
      s"Time\tAlgorithm" +
        s"\tlatency" +
        s"\tcpuUsage" +
        s"\teventArrivalRate" +
        s"\tTransitionStatus" +
        s"\tQueryEndToEndLatencyPrediction" +
        s"\tQueryThroughputPrediction"
    } else {
      s"Time \t Algorithm" +
        s"\t latency" +
        s"\t cpuUsage" +
        //s"\t eventPublishRate
        s"\t eventArrivalRate " +
        s"\t TransitionStatus"
    }
  }

  override def startSimulationLog(algoName: String, startTime: FiniteDuration, interval: FiniteDuration, totalTime: FiniteDuration, callback: () => Any): Any = {

    var time = 0L
    var exchangeableTime = 0L
    log.info(s"starting splcData simulation log, asking ${queryGraph.mapek.knowledge} for cfm")
    val cfmInit = if(this.learningModel.equals("learnon"))
      for { res <- (queryGraph.mapek.knowledge ? GetCFM).mapTo[CFM] } yield {
        log.info("received cfm from knowledge")
        cfm = Some(res)
      } else Future {}
    cfmInit map { _ =>
      // SPLC measurement file header; status of all features is needed for SPLC
      splcOut.append(splcHeader)
      splcOut.println()
      out.append(logHeader)
      out.println()
      simulation = context.system.scheduler.scheduleAtFixedRate(startTime, interval)(() => createCSVEntry())
      if (totalTime.toSeconds > 0)
        context.system.scheduler.scheduleOnce(totalTime)(stopSimulation())

      log.info("simulation logging setup complete")

      def addSPLCMeasurementLine(contextData: Map[String, AnyVal], activePlacementAlgorithm: String): Unit = {
        val placementBooleans = allPlacementAlgorithms.map(pa => if (activePlacementAlgorithm == pa) 1 else 0).mkString(";") + ";"
        // root, system, mechanisms, placement algorithm are always active
        val measurementLine = "1;1;1;1;" +
          // status of placement algorithm features
          placementBooleans +
          // context, network situation, fixedProperties and variableProperties - always active
          "1;1;1;1;" +
          fixedCFMAttributes.map(e => contextData.getOrElse(e, Double.NaN)).mkString(";") + ";" +
          variableCFMAttributes.map(e => contextData.getOrElse(e, Double.NaN)).mkString(";") + ";" +
          metricElements.map(e => contextData.getOrElse(e, Double.NaN)).mkString(";")


        splcOut.append(measurementLine)
        splcOut.println()
      }

      def createCSVEntry(): Unit = {
        //log.info("called createCSVEntry()")
        try {
          for {
            cAllRecords <- (consumer ? GetAllRecords).mapTo[AllRecords]
            //_ <- Future { log.info(s"all defined: ${cAllRecords.allDefined} cAllRecords: $cAllRecords")}
            if recordsArrived(cAllRecords) && cAllRecords.allDefined
            deploymentComplete <- (queryGraph.mapek.knowledge ? IsDeploymentComplete).mapTo[Boolean]
            //_ <- Future { log.info(s"deployment complete: $deploymentComplete")}
            avgLatency <- (queryGraph.mapek.knowledge ? GetAverageLatency(interval.toMillis)).mapTo[Double]
            //_ <- Future { log.info(s"contextData: ${contextData.mkString("\n")}")}
            if deploymentComplete //&& contextData.nonEmpty
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
              for {
                contextData <- (queryGraph.mapek.knowledge ? GetContextData).mapTo[Map[String, AnyVal]]
                learnOnLogData <- (queryGraph.mapek.asInstanceOf[LearnOnMAPEK].learningModel ? GetLearnOnLogData(cfm.get, contextData, strategyName)).mapTo[mutable.HashMap[String, List[List[Double]]]]} yield {
                //log.info(s"logData received: ${learnOnLogData.mkString("\n")}")
                val estimatedLatency = learnOnLogData.get("latency").get.head.head
                val estimatedLoad = learnOnLogData.get("load").get.head.head
                out.append(
                  s"$time" +
                    s"\t$strategyName" +
                    s"\t${avgLatency}" +
                    s"\t$estimatedLatency" +
                    s"\t${cAllRecords.recordAverageLoad.lastLoadMeasurement.getOrElse(0)}" +
                    s"\t$estimatedLoad" +
                    s"\t${cAllRecords.recordPublishingRate.lastRateMeasurement.getOrElse(0)}" +
                    s"\t${cAllRecords.recordFrequency.lastMeasurement.getOrElse(0)}" +
                    s"\t$transitionStatus" +
                    s"\t${learnOnLogData.get("latency").get}" +
                    s"\t${learnOnLogData.get("load").get}\t"
                )
                addSPLCMeasurementLine(contextData, activePlacementAlgorithm)
                //log.info("LearnOn data added!")
                out.println()
              }

            } else if (this.learningModel.equals("lightweight")) {
              log.info("requesting LightweightLogData")
              for {learnOnLogData <- (queryGraph.mapek.asInstanceOf[LearnOnMAPEK].learningModel ? GetLightweightLogData).mapTo[(List[Any], List[Any])]} yield {
                out.append(
                  s"$time" +
                    s"\t$strategyName" +
                    s"\t${avgLatency}" +
                    s"\t${cAllRecords.recordAverageLoad.lastLoadMeasurement.getOrElse(0)}" +
                    //s"\t${cAllRecords.recordPublishingRate.lastRateMeasurement.getOrElse(0)}" +
                    s"\t${cAllRecords.recordFrequency.lastMeasurement.getOrElse(0)}" +
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
                    s"\t${avgLatency}" +
                    s"\t${cAllRecords.recordAverageLoad.lastLoadMeasurement.getOrElse(0)}" +
                    s"\t${cAllRecords.recordPublishingRate.lastRateMeasurement.getOrElse(0)}" +
                    s"\t${cAllRecords.recordFrequency.lastMeasurement.getOrElse(0)}" +
                    s"\t$transitionStatus" +
                    s"\t${learnOnLogData.slice(0, learnOnLogData.size - 1)}" +
                    s"\t${learnOnLogData.last}"
                )
                log.info("RL data added!")
                out.println()
              }
            } else if(this.mapekType == "ExchangeablePerformanceModel") {

              val start = System.nanoTime()
              val rootOperator: Query = queryGraph.mapek.asInstanceOf[ExchangeablePerformanceModelMAPEK].getQueryRoot
              val qSize = if(querySize.isDefined) querySize.get else {
                querySize = Some(Queries.getOperators(rootOperator).size)
                querySize.get
              }
              val queryPerformancePredictor = cluster.system.actorSelection(queryGraph.mapek.planner.path.child("queryPerformancePredictor"))
              for {
                publisherEventRates: Map[String, Throughput] <- TCEPUtils.getPublisherEventRates()
                currentPlacement: Option[Map[Query, ActorRef]] <- (queryGraph.mapek.knowledge ? GetOperators).mapTo[CurrentOperators].map(e => Some(e.placement))
                contextSample <- (queryGraph.mapek.knowledge ? GetContextSample).mapTo[Map[Query, (Samples, List[OfflineAndOnlinePredictions])]]
                _ <- Future {
                  val missing = contextSample.keySet.diff(currentPlacement.getOrElse(Map[Query, Address]()).keySet).toString()
                  log.debug("logging sim data, context samples from {} operators and placements from {} operators received ({} ops in query),missing placements from {}", contextSample.size.toString, currentPlacement.getOrElse(Map.empty).size.toString, qSize.toString, missing)
                }
                currentPlacementStr <- (queryGraph.mapek.knowledge ? GetPlacementStrategyName).mapTo[String]
                if contextSample.size >= qSize // only start logging after all operators have sent samples
                queryPred <- (queryPerformancePredictor ? GetPredictionForPlacement(rootOperator, currentPlacement, currentPlacement.get.map(e => e._1 -> e._2.path.address), publisherEventRates, Some(contextSample), currentPlacementStr)).mapTo[EndToEndLatencyAndThroughputPrediction]
              } yield {
                val end = System.nanoTime()
                out.append(
                  s"$exchangeableTime" +
                    s"\t$strategyName" +
                    s"\t${avgLatency}" +
                    s"\t${cAllRecords.recordAverageLoad.lastLoadMeasurement.getOrElse(0)}" +
                    s"\t${cAllRecords.recordFrequency.lastMeasurement.getOrElse(0)}" +
                    s"\t$transitionStatus" +
                    s"\t${queryPred.endToEndLatency.amount.toNanos.toDouble / 1e6}" +
                    s"\t${queryPred.throughput.amount}"
                )
                exchangeableTime += interval.toSeconds
                out.println()
              }

            } else {
              out.append(
                s"$time" +
                  s"\t$strategyName" +
                  s"\t${avgLatency}" +
                  s"\t${cAllRecords.recordAverageLoad.lastLoadMeasurement.getOrElse(0)}" +
                  //s"\t${cAllRecords.recordPublishingRate.lastRateMeasurement.getOrElse(0)}" +
                  s"\t${cAllRecords.recordFrequency.lastMeasurement.getOrElse(0)}" +
                  s"\t$transitionStatus"
              )
              out.println()
            }

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
