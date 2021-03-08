package contrast.testextensions

import akka.actor.ActorRef
import tcep.graph.transition.mapek.contrast.{ContrastMAPEK, ContrastMonitor}

/**
  * Created by Niels on 30.03.2018.
  */
class TestMonitorComponent(mapek: ContrastMAPEK, consumer: ActorRef) extends ContrastMonitor(mapek: ContrastMAPEK, consumer, Map()) {

  override def nodeCount: Int = 42

  override def receive: Receive = {
    case TestUpdateContextData => updateContextData()
  }

  override def postStop() = {

  }
}

