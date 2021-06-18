package tcep.machinenodes

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import org.discovery.vivaldi.DistVivaldiActor
import tcep.config.ConfigurationParser
import tcep.machinenodes.helper.actors.TaskManagerActor

/**
  * Just creates a `TaskManagerActor` which could receive tasks from PlacementAlgorithms
  * Created by raheel
  * on 09/08/2017.
  */
object EmptyApp extends ConfigurationParser with App {

  val actorSystem: ActorSystem = ActorSystem(config.getString("clustering.cluster.name"), config)
  DistVivaldiActor.createVivIfNotExists(actorSystem)
  val taskManager = actorSystem.actorOf(Props(classOf[TaskManagerActor]), "TaskManager")
  logger.info(s"booting up EmptyApp with taskManager actor $taskManager")
  override def getRole: String = "Candidate"
  override def getArgs: Array[String] = args
}