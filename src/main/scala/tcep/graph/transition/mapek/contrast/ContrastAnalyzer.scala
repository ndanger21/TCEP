package tcep.graph.transition.mapek.contrast

import akka.actor.Cancellable
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import org.cardygan.config.Config
import tcep.data.Queries.Requirement
import tcep.graph.transition.MAPEK._
import tcep.graph.transition.mapek.contrast.ContrastMAPEK.{GetCFM, GetContextData, RunPlanner}
import tcep.graph.transition.{AnalyzerComponent, ChangeInNetwork}

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
class ContrastAnalyzer(mapek: ContrastMAPEK) extends AnalyzerComponent {

  var planningScheduler: Cancellable = _
  private val transitionCooldown = ConfigFactory.load().getInt("constants.mapek.transition-cooldown") * 1000

  override def preStart(): Unit = {
    super.preStart()
    log.info("starting MAPEK analyzer")
    // note: transitions have a "cooldown" time (set in application.conf),
    // starting when this component starts up; i.e. the planner will not run before that period is over,
    // and not for that period after a transition is completed
    planningScheduler = this.context.system.scheduler.schedule(1 minute, 1 minute, this.self, GenerateAndSendContextConfig)
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

    // execute planner periodically every 60s
    case GenerateAndSendContextConfig =>
      // disabled for base data collection
      val transitionsEnabled = ConfigFactory.load().getBoolean("constants.mapek.transitions-enabled")
      if(transitionsEnabled) {
        log.info("received GenerateAndSendContextConfig message")
        for {

          transitionStatus <- (mapek.knowledge ? GetTransitionStatus).mapTo[Int]
          deploymentComplete <- (mapek.knowledge ? IsDeploymentComplete).mapTo[Boolean]
          lastTransitionEnd <- (mapek.knowledge ? GetLastTransitionEnd).mapTo[Long]
          if deploymentComplete && transitionStatus == 0 && System.currentTimeMillis() - lastTransitionEnd >= transitionCooldown
          currentLatency <- (mapek.knowledge ? GetAverageLatency).mapTo[Double]
          contextData <- (mapek.knowledge ? GetContextData).mapTo[Map[String, AnyVal]]
          logg: Boolean <- {
            //log.debug(s"received RunPlanner message, conditions: \n transitionStatus: $transitionStatus \n deploymentComplete ${deploymentComplete} \n contextData ${contextData.mkString("\n")}")
            Future { true }
          }
          if contextData.nonEmpty
          cfm <- (mapek.knowledge ? GetCFM).mapTo[CFM]
          requirements <- (mapek.knowledge ? GetRequirements).mapTo[List[Requirement]]
        } yield {

          val contextConfig: Config = cfm.getCurrentContextConfig(contextData)
          val qosRequirements: Set[Requirement] = requirements.toSet
          log.info("sending context config to Planner ")
          mapek.planner ! RunPlanner(cfm, contextConfig, currentLatency, qosRequirements)
        }
      } else log.info("not generating and sending context config since transitions are disabled in application.conf")
  }

  case object GenerateAndSendContextConfig
}

