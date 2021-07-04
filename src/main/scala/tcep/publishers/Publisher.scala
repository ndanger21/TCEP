package tcep.publishers

import akka.actor.{ActorLogging, ActorRef}
import akka.cluster.ClusterEvent._
import com.typesafe.config.ConfigFactory
import tcep.data.Queries.{PublisherDummyQuery, Query}
import tcep.graph.nodes.traits.Node._
import tcep.graph.transition.{AcknowledgeStart, StartExecution, TransitionRequest}
import tcep.machinenodes.helper.actors.PlacementMessage
import tcep.placement.vivaldi.VivaldiCoordinates
import tcep.prediction.PredictionHelper.Throughput
import tcep.publishers.Publisher._
import tcep.publishers.RegularPublisher.{GetEventsPerSecond, LogEventsSentTick}

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

/**
  * Publisher Actor
  **/
trait Publisher extends VivaldiCoordinates with ActorLogging {
  val publisherName: String = self.path.name
  val subscribers: mutable.Map[ActorRef, Query] = mutable.Map[ActorRef, Query]()
  val id: AtomicInteger = new AtomicInteger(0)
  var emitEventTask: ScheduledFuture[_] = _
  var sched = Executors.newSingleThreadScheduledExecutor()
  var eventRateOut: Throughput
  val eventSizeOut: Long
  val samplingInterval: FiniteDuration = FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.MILLISECONDS)
  var lastEventCount = 0
  var startedPublishing = false

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
    case LogEventsSentTick =>
      eventRateOut = Throughput(id.get() - lastEventCount, samplingInterval)
      lastEventCount = id.get()
    //SpecialStats.log(self.toString(), "eventsSent", s"total: ${id.get()}")

    case GetEventsPerSecond => sender() ! eventRateOut
  }

}

/**
  * List of Akka Messages which is being used by Publisher actor.
  **/
object Publisher {
  case class AcknowledgeSubscription(acknowledgingParent: Query) extends PlacementMessage
  case class StartStreams() extends PlacementMessage
}
