package tcep.graph.transition.mapek.contrast

import akka.actor.Cancellable
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import org.cardygan.config.Config
import tcep.data.Queries.Requirement
import tcep.graph.transition.MAPEK._
import tcep.graph.transition.mapek.contrast.ContrastMAPEK.{GetCFM, GetContextData, RunPlanner}
import tcep.graph.transition.{AnalyzerComponent, ChangeInNetwork, MAPEK}

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Analyzer of the MAPE-K cycle
  * collects current context data and QoS requirements from ContrastKnowledge, generates the current context configuration
  * (CFM features and attributes instantiated to resemble the current context), and sends it to the ContrastPlanner.
  * This happens as a reaction to new QoS requirements being added to the query, or in a regular interval
  * @author Niels
  * @param mapek reference to the running MAPEK instance
  */
class ContrastAnalyzer(mapek: MAPEK, delay: FiniteDuration = 1 minute, interval: FiniteDuration = 1 minute) extends AnalyzerComponent {

  var planningScheduler: Cancellable = _
  val transitionCooldown = ConfigFactory.load().getInt("constants.mapek.transition-cooldown") * 1000
  val transitionsEnabled = ConfigFactory.load().getBoolean("constants.mapek.transitions-enabled")

  override def preStart(): Unit = {
    super.preStart()
    log.info(s"starting MAPEK analyzer with dispatcher ${context.dispatcher}")
    // note: transitions have a "cooldown" time (set in application.conf),
    // starting when this component starts up; i.e. the planner will not run before that period is over,
    // and not for that period after a transition is completed
    planningScheduler = this.context.system.scheduler.schedule(delay, interval, this.self, GenerateAndSendContextConfig)
  }

  override def postStop(): Unit = {
    super.postStop()
    planningScheduler.cancel()
  }

  override def receive: Receive = {

    case AddRequirement(newRequirements) =>
      log.info(s"New requirement(s) added: $newRequirements, generating context config")
      self ! GenerateAndSendContextConfig

    case ChangeInNetwork =>
      log.info("operator host down, sending context config to Planner")
      self ! GenerateAndSendContextConfig

    // execute planner periodically
    case GenerateAndSendContextConfig =>
      // disabled for base data collection
      if(transitionsEnabled) {
        log.info("received GenerateAndSendContextConfig message")
        try {
          //implicit val ec = blockingIoDispatcher
          for {
            transitionStatus <- (mapek.knowledge ? GetTransitionStatus).mapTo[Int]
            deploymentComplete <- (mapek.knowledge ? IsDeploymentComplete).mapTo[Boolean]
            lastTransitionEnd <- (mapek.knowledge ? GetLastTransitionEnd).mapTo[Long]
            _ <- Future { log.info(s"deploymentComplete: $deploymentComplete transitionStatus: $transitionStatus transitionCooldownComplete: ${System.currentTimeMillis() - lastTransitionEnd >= transitionCooldown}") }
            if deploymentComplete && transitionStatus == 0 && System.currentTimeMillis() - lastTransitionEnd >= transitionCooldown
            currentLatency <- (mapek.knowledge ? GetAverageLatency(mapek.samplingInterval.toMillis)).mapTo[Double]
            cfm <- (mapek.knowledge ? GetCFM).mapTo[CFM]
            contextConfig <- getCurrentContextConfig(cfm)
            requirements <- (mapek.knowledge ? GetRequirements).mapTo[List[Requirement]]
          } yield {
            val qosRequirements: Set[Requirement] = requirements.toSet
            log.info("sending context config to Planner ")
            mapek.planner ! RunPlanner(cfm, contextConfig, currentLatency, qosRequirements)
          }
        } catch {
          case e: Throwable => log.error(e, "failed to collect data and send RunPlanner")
        }
      } else log.info("not generating and sending context config since transitions are disabled in application.conf")
  }

  def getCurrentContextConfig(cfm: CFM): Future[Config] = for {
    contextData <- (mapek.knowledge ? GetContextData).mapTo[Map[String, AnyVal]]
    if contextData.nonEmpty
  } yield cfm.getCurrentContextConfig(contextData)

  case object GenerateAndSendContextConfig
}

