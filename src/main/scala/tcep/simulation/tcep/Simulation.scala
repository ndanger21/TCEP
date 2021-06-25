package tcep.simulation.tcep

import akka.actor.{ActorContext, ActorRef, Cancellable}
import akka.cluster.Cluster
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import tcep.data.Queries.{Query, Requirement}
import tcep.graph.QueryGraph
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.qos.MonitorFactory
import tcep.graph.transition.MAPEK._
import tcep.graph.transition.TransitionStats
import tcep.graph.transition.mapek.lightweight.LightweightKnowledge.GetLogData
import tcep.machinenodes.GraphCreatedCallback
import tcep.machinenodes.consumers.Consumer.{AllRecords, GetAllRecords, GetMonitorFactories, SetQosMonitors}
import tcep.prediction.PredictionHelper.Throughput

import java.io.{File, PrintStream}
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class Simulation(name: String, query: Query, transitionConfig: TransitionConfig,
                 publishers: Map[String, ActorRef], consumer: ActorRef, startingPlacementStrategy: Option[String],
                 mapekType: String = ConfigFactory.load().getString("constants.mapek.type"))
                (implicit context: ActorContext, cluster: Cluster, publisherEventRates: Map[String, Throughput], directory: Option[File], pimPaths: (String, String),
                 fixedSimulationProperties: Map[Symbol, Int] = Map()) {

  lazy val blockingIoDispatcher: ExecutionContext = cluster.system.dispatchers.lookup("blocking-io-dispatcher")
  implicit val ec: ExecutionContext = blockingIoDispatcher
  val ldt = LocalDateTime.now
  val out = directory map { directory => new PrintStream(new File(directory, s"$name-$ldt.csv"))
  } getOrElse java.lang.System.out
  protected val log = LoggerFactory.getLogger(getClass)
  protected var queryGraph: QueryGraph = _
  protected var simulation: Cancellable = _
  protected var guiUpdater: Cancellable = _
  protected var callback: () => Any = _
  implicit private val timeout = Timeout(5 seconds)
  private val LIGHTWEIGHT = "lightweight"
  val baseEventRate: Double = if(publisherEventRates.nonEmpty) publisherEventRates.values.map(_.getEventsPerSec).sum / publisherEventRates.size else 0
  /**
    *
    * @param startTime Start Time of Simulation (in Seconds)
    * @param interval  Interval for recording data in CSV (in Seconds)
    * @param totalTime Total Time for the simulation (in Seconds)
    * @return
    */
  def startSimulation(queryStr: String, startTime: FiniteDuration, interval: FiniteDuration, totalTime: FiniteDuration)(callback: () => Any): Future[QueryGraph] = {
    this.callback = callback
    log.info("Executing query. Fetching monitors...")
    for {
      monitors <- (this.consumer ? GetMonitorFactories).mapTo[Array[MonitorFactory]]
      _ = {
        log.info(s"Monitors are: $monitors")
        queryGraph = new QueryGraph(query, transitionConfig, publishers, startingPlacementStrategy, Some(GraphCreatedCallback()), consumer, mapekType)
      }
      graph <- queryGraph.createAndStart()
    } yield {
      guiUpdater = context.system.scheduler.schedule(0 seconds, 60 seconds)(GUIConnector.sendMembers(cluster))
      startSimulationLog(queryStr, startTime, interval, totalTime, callback)
      queryGraph
    }
  }



  def startSimulationLog(queryStr: String, startTime: FiniteDuration, interval: FiniteDuration, totalTime: FiniteDuration, callback: () => Any): Any = {

    log.info("entered StartSimulationLog")
    var time = 0L
    val transitionExecutionMode = ConfigFactory.load().getInt("constants.transition-execution-mode")
    var executionMode: String = "Sequential"
    var header: Boolean = false
    //val eventRate: Int = microsecondsPerEvent / 1000000

    def createCSVEntry(): Unit = synchronized {
      try {
        def getFitnessData() = {
          if (mapekType == LIGHTWEIGHT)
            (queryGraph.mapek.knowledge ? GetLogData)
          else Future {
            (List(), List())
          }
        }

        for {
          cAllRecords <- (consumer ? GetAllRecords).mapTo[AllRecords]
          if recordsArrived(cAllRecords) && cAllRecords.allDefined
          status <- (queryGraph.mapek.knowledge ? GetTransitionStatus).mapTo[Int]
          avgLatency <- (queryGraph.mapek.knowledge ? GetAverageLatency(interval.toMillis)).mapTo[Double]
          placementStrategy <- (queryGraph.mapek.knowledge ? GetPlacementStrategyName).mapTo[String]
          currentRequirement <- (queryGraph.mapek.knowledge ? GetRequirements).mapTo[List[Requirement]]
          lastTransitionStats <- (queryGraph.mapek.knowledge ? GetLastTransitionStats).mapTo[(TransitionStats, Long)]
          //placementComplete <- (queryGraph.mapek.knowledge ? IsDeploymentComplete).mapTo[Boolean]
          //if placementComplete && status == 0
          //fitnessContainer <- (queryGraph.mapek.knowledge ? GetFitnessContainer).mapTo[FitnessContainer]
          fitness_data <- getFitnessData().mapTo[(List[Any], List[Any])]
        } yield {
          if (transitionExecutionMode == 1)
            executionMode = "Exponential"
          if (time == 0 && !header) {
            var headerLine = s"Placement \t CurrentTime \t Query \t Publisher Event Rate \t Time \t latency \t hops \t cpuUsage \t Output Event Rate " +
              s"\t EventMsgOverhead \t PlacementMsgOverhead \t NetworkUsage " +
              s"\t TransitionStatus \t LastTransitionDuration \t LastTransitionCommunicationOverhead \t LastTransitionPlacementOverhead \t LastTransitionCombinedOverhead " +
              s"\t requirement \t mapek"
            if (mapekType == LIGHTWEIGHT) headerLine += s"\t AlgorithmFitness \t AlgorithmSelectionProbabilities "
            out.append(headerLine)
            out.println()
            header = true
          }
          log.info(s"write csv ${cAllRecords.recordLatency.lastMeasurement}")
          var outstring = ""
          //fitnessContainer.updateFitness()
          //val fitness_data = fitnessContainer.getLogData()
          outstring = s"$placementStrategy" +
            s"\t ${LocalDateTime.ofInstant(Instant.now, ZoneOffset.UTC).getHour}:${LocalDateTime.ofInstant(Instant.now, ZoneOffset.UTC).getMinute}:${LocalDateTime.ofInstant(Instant.now, ZoneOffset.UTC).getSecond} " +
            s"\t $queryStr" +
            s"\t $baseEventRate" +
            s"\t $time \t ${BigDecimal(avgLatency).setScale(2, BigDecimal.RoundingMode.HALF_UP)} " +
            s"\t ${cAllRecords.recordMessageHops.lastMeasurement.get.toString} " +
            s"\t ${BigDecimal(cAllRecords.recordAverageLoad.lastLoadMeasurement.get).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString} " +
            s"\t ${cAllRecords.recordFrequency.lastMeasurement.get.toString} " +
            s"\t ${cAllRecords.recordMessageOverhead.lastEventOverheadMeasurement.get / 1000.0} " +
            s"\t ${cAllRecords.recordMessageOverhead.lastPlacementOverheadMeasurement.get / 1000.0} " +
            s"\t ${BigDecimal(cAllRecords.recordNetworkUsage.lastUsageMeasurement.get).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString} " +
            s"\t $status " +
            s"\t ${lastTransitionStats._2} " +
            s"\t ${lastTransitionStats._1.transitionOverheadBytes / 1000.0} " + // kB
            s"\t ${lastTransitionStats._1.placementOverheadBytes / 1000.0} " +
            s"\t ${(lastTransitionStats._1.placementOverheadBytes + lastTransitionStats._1.transitionOverheadBytes) / 1000.0} " +
            s"\t ${currentRequirement.map(_.name).mkString(" ")}" +
            s"\t $mapekType"
          if (mapekType == LIGHTWEIGHT) {
            outstring +=
              s"\t ${fitness_data._1.toString()} " +
                s"\t ${fitness_data._2.toString()}"
          }
          out.append(outstring)
          out.println()
         time += interval.toSeconds
        }
      } catch {
        case e: Throwable =>
          log.error("exception while creating csv entry", e)
      }
    }
    simulation = context.system.scheduler.schedule(startTime, interval)(createCSVEntry())
    // If the total time is set to 0, run the simulation infinitely until stopped
    if (totalTime.toSeconds != 0) {
      context.system.scheduler.scheduleOnce(totalTime)(stopSimulation())
      context.system.scheduler.scheduleOnce(totalTime)(GUIConnector.sendMembers(cluster))
    }

  }

  def recordsArrived(cAllRecords: AllRecords): Boolean = {
    if (!cAllRecords.allDefined) {
      log.info(s"Data not available yet! ${cAllRecords.getValues}")
      log.info(s"Resetting!")
      consumer ! SetQosMonitors
      false
    } else {
      true
    }
  }

  def stopSimulation(): Unit = {
    simulation.cancel()
    guiUpdater.cancel()
    out.close()
    queryGraph.stop()
    if (callback != null) {
      //wait for all actors to stop
      context.system.scheduler.scheduleOnce(FiniteDuration.apply(1, TimeUnit.SECONDS))(() => callback.apply())
    }
  }
}

