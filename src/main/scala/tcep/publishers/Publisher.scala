package tcep.publishers

import akka.actor.{ActorLogging, ActorRef}
import akka.cluster.ClusterEvent._
import tcep.data.Queries.{PublisherDummyQuery, Query}
import tcep.graph.nodes.traits.Node._
import tcep.graph.transition.{AcknowledgeStart, StartExecution, TransitionRequest}
import tcep.machinenodes.helper.actors.PlacementMessage
import tcep.placement.vivaldi.VivaldiCoordinates
import tcep.publishers.Publisher._

import scala.collection.mutable

/**
  * Publisher Actor
  **/
trait Publisher extends VivaldiCoordinates with ActorLogging {

  val subscribers: mutable.Map[ActorRef, Query] = mutable.Map[ActorRef, Query]()

  override def receive: Receive = super.receive orElse {
    case Subscribe(sub, operator) =>
      transitionLog(s"${sub} subscribed to publisher $self, current subscribers: \n ${subscribers.mkString("\n")}")
      subscribers += sub -> operator
      sender() ! AcknowledgeSubscription(PublisherDummyQuery(self.path.name))

    case UnSubscribe() =>
      subscribers -= sender()
      transitionLog(s"${sender().path.name} unsubscribed, remaining subscribers: $subscribers")

    case _: MemberEvent => // ignore
    case StartExecution(algorithm) => sender() ! AcknowledgeStart()
    case _: TransitionRequest => log.error(s"FAULTY SUBSCRIPTION: \n Publisher ${self} received TransitionRequest from $sender()")
  }

}

/**
  * List of Akka Messages which is being used by Publisher actor.
  **/
object Publisher {
  case class AcknowledgeSubscription(acknowledgingParent: Query) extends PlacementMessage
  case class StartStreams() extends PlacementMessage
}
