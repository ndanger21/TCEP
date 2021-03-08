package tcep.publishers

import tcep.data.Events.Event
import tcep.graph.nodes.traits.Node._
import tcep.graph.transition.StartExecution

case class TestPublisher() extends Publisher {

  override def receive: Receive = {
    case Subscribe(s, op) =>
      super.receive(Subscribe(s, op))
    case e: Event =>
      if(subscribers.isEmpty)
        println(s"no subscribers on $self to send $e to".toUpperCase())
      subscribers.keys.foreach(_ ! e)
    case StartExecution(algorithm) => super.receive(StartExecution(algorithm))
    case message =>
      subscribers.keys.foreach(_ ! message)
  }

}
