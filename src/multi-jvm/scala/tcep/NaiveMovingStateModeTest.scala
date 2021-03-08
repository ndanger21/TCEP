package tcep

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Props, RootActorPath}
import akka.testkit.TestProbe
import akka.util.Timeout
import org.discovery.vivaldi.Coordinates
import tcep.data.Events
import tcep.data.Events.{Event1, Event2}
import tcep.data.Queries.{ClientDummyQuery, SlidingTime}
import tcep.dsl.Dsl._
import tcep.graph.nodes.traits.Node.Subscribe
import tcep.graph.nodes.traits.{TransitionConfig, TransitionExecutionModes, TransitionModeNames}
import tcep.graph.transition.MAPEK.{GetOperators, GetPlacementStrategyName, GetTransitionStatus}
import tcep.graph.transition.{TransitionRequest, TransitionStats}
import tcep.graph.{EventCallback, QueryGraph}
import tcep.machinenodes.consumers.Consumer.{SetQosMonitors, SetStatus}
import tcep.machinenodes.helper.actors.{ACK, GetEventPause, GetMaxEventInterval, GetTimeSinceLastEvent}
import tcep.placement.{GlobalOptimalBDPAlgorithm, MobilityTolerantAlgorithm}
import tcep.publishers.Publisher.AcknowledgeSubscription

import scala.concurrent.Await
import scala.concurrent.duration._

class NaiveMovingStateModeMultiJvmClient extends NaiveMovingStateModeSpec

class NaiveMovingStateModeMultiJvmPublisher1 extends NaiveMovingStateModeSpec

class NaiveMovingStateModeMultiJvmPublisher2 extends NaiveMovingStateModeSpec

abstract class NaiveMovingStateModeSpec extends MultiJVMTestSetup(3) {

  import TCEPMultiNodeConfig._

  private val windowSize = 1
  private val t = FiniteDuration(1, TimeUnit.SECONDS)
  var graph: QueryGraph = _
  var rootOperator, endActor, clientWrapperActor: ActorRef = _
  var streamProbe1, streamProbe2: TestProbe = _
  lazy val stream1 = stream[Int](pNames(0))
  lazy val stream2 = stream[Int](pNames(1))
  lazy val query = stream1.join(stream2, SlidingTime(windowSize), SlidingTime(windowSize))

  case class PrintEventCallback() extends EventCallback {
    override def apply(event: Events.Event): Unit = {
      println("received EVENT: " + event)
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    setCoordinates(Coordinates(0, 0, 0), Coordinates(50, 100, 0), Coordinates(-50, 100, 0), Coordinates(1000, 1000, 0), Coordinates(1000, 1000, 0))
  }

  "NaiveMovingStateMode" should {
    "deploy the test graph operators to the publishers and the client" in within(10.second) {

      testConductor.enter("test start")
      runOn(client) {
        clientWrapperActor = system.actorOf(Props(new Actor {

          override def preStart(): Unit = {
            super.preStart()
            graph = new QueryGraph(query, TransitionConfig(TransitionModeNames.NaiveMovingState, TransitionExecutionModes.CONCURRENT_MODE), publishers, Some(MobilityTolerantAlgorithm), None, consumer = clientProbe.ref)
            rootOperator = graph.createAndStart(None)
            endActor = graph.clientNode
          }

          override def receive: Receive = {
            case GetOperators => graph.mapek.knowledge.forward(GetOperators)
            case _ =>
          }
        }), "SimulationSetup")

        clientWrapperActor ! GetOperators
        expectMsgPF[Set[ActorRef]](remaining, s"stream operators must be deployed to publisher hosts, join to client host within ${remaining.toMillis}ms") {
          case ops: List[ActorRef] =>
            log.info("\n========= \n OPERATORS \n========== \n" + ops.mkString("\n"))
            assert(ops.size == 3, "there must be 3 operators")
            assert(ops.head.path.address != ops.tail.head.path.address && ops.head.path.address != ops.last.path.address, "all operators must be on different hosts")
            assert(ops.exists(op => op.path.address.host.isEmpty && op.path.address.port.isEmpty && op.path.name.contains("Join")),
              s"join operator must be on client host")
            assert(ops.exists(op => op.path.address.port.getOrElse(-1) == node(publisher1).address.port.get && op.path.toString.contains(pNames(0).substring(2)) && op.path.name.contains("Stream")),
              "stream operator 1 must be on publisher1")
            assert(ops.exists(op => op.path.address.port.getOrElse(-1) == node(publisher2).address.port.get && op.path.toString.contains(pNames(1).substring(2)) && op.path.name.contains("Stream")),
              "stream operator 2 must be on publisher2")
            ops.toSet
        }
      }
      testConductor.enter("initial placement to client and publishers complete")
    }

    "receive events on stream1 after initial placement" in within(2.second) {
      runOn(publisher1) {
        streamProbe1 = TestProbe("stream1Probe")
        Await.result(for {streamActor1 <- cluster.system.actorSelection(RootActorPath(node(publisher1).address) / "user" / "Stream*").resolveOne()(remaining)} yield {
          streamProbe1.send(streamActor1, Subscribe(streamProbe1.ref, ClientDummyQuery()))
          streamProbe1.expectMsg(AcknowledgeSubscription(stream1))
          streamProbe1.expectMsgClass(classOf[Event1])
        }, remaining)
      }
      testConductor.enter("events should arrive at stream2 before transition request")
    }
    "receive events on stream2 after initial placement" in within(2.second) {
      runOn(publisher2) {
        streamProbe2 = TestProbe("stream2Probe")
        Await.result(for {streamActor2 <- cluster.system.actorSelection(RootActorPath(node(publisher2).address) / "user" / "Stream*").resolveOne()(remaining)} yield {
          streamProbe2.send(streamActor2, Subscribe(streamProbe2.ref, ClientDummyQuery()))
          streamProbe2.expectMsg(AcknowledgeSubscription(stream2))
          streamProbe2.expectMsgClass(classOf[Event1])
        }, remaining)
      }
      testConductor.enter("events should arrive at stream2 before transition request")
    }

    "receive events on join and client after initial placement" in within(2.second) {
      runOn(client) {
        awaitAssert(clientProbe.expectMsg(SetQosMonitors))
        val joinProbe = TestProbe("joinProbe")
        joinProbe.send(rootOperator, Subscribe(joinProbe.ref, ClientDummyQuery()))
        joinProbe.expectMsg(AcknowledgeSubscription(query))
        joinProbe.expectMsgClass(classOf[Event2])
        clientProbe.expectMsgClass(classOf[Event2])
      }
      testConductor.enter("events arrive at client and join before transition request")
    }

    "receive ACK for TransitionRequest on clientNode" in within(t) {
      runOn(client) {
        endActor ! TransitionRequest(GlobalOptimalBDPAlgorithm, self, TransitionStats())
        expectMsg(ACK())
      }
      testConductor.enter("TransitionRequest ACK'd")
    }

    "change to transition status to ongoing on mapek knowledge" in within(t) {
      runOn(client) {
        val p = TestProbe()
        awaitAssert(() => {
          p.send(graph.mapek.knowledge, GetTransitionStatus)
          p.expectMsg(1)
        }, remaining)
        awaitAssert(() => {
          clientProbe.expectMsg(SetStatus(1))
        }, remaining)
      }
      testConductor.enter("transition status changed")
    }

    "pause event processing on stream operator and wait until remaining events on intermediate operators are processed and have arrived at the client" in within(10.second) {
      runOn(client) {
        // expect no further new events for some time
        val p = TestProbe()
        implicit val timeout: Timeout = Timeout(remaining)
        val res = p.awaitAssert {
          val joinActor = Await.result(cluster.system.actorSelection(RootActorPath(node(client).address) / "user" / "Join*").resolveOne(), remaining)
          p.send(joinActor, GetMaxEventInterval)
          val maxEventInterval: Long = p.expectMsgClass(classOf[Long])
          p.send(joinActor, GetTimeSinceLastEvent)
          val timeSinceLast: Long  = p.expectMsgClass(classOf[Long])
          println("MAX EVENT INTERVAL STREAM1: " + maxEventInterval + " time since last: " + timeSinceLast + " is larger: " + (timeSinceLast > maxEventInterval))
          //assert(timeSinceLast > maxEventInterval, "there must be a pause longer than the longest event interval since transition start")
          p.send(joinActor, GetEventPause)
          p.expectMsg(true)
        }
        println("client: " + res)
      }
      runOn(publisher1) {
        // stream should stop sending events after receiving transitionRequest
        val p = TestProbe()
        implicit val timeout: Timeout = Timeout(remaining)
        awaitAssert {
          val streamActor1 = Await.result(cluster.system.actorSelection(RootActorPath(node(publisher1).address) / "user" / "Stream*").resolveOne(), remaining)
          /*
          p.send(streamActor1, GetMaxEventInterval)
          val maxEventInterval: Long = expectMsgClass(classOf[Long])
          p.send(streamActor1, GetTimeSinceLastEvent)
          val timeSinceLast: Long = expectMsgClass(classOf[Long])
          log.warning("MAX EVENT INTERVAL STREAM1: " + maxEventInterval + " time since last: " + timeSinceLast + " is larger: " + (timeSinceLast > maxEventInterval))
          //assert(timeSinceLast > maxEventInterval, "there must be a pause longer than the longest event interval since transition start")
          */
          p.send(streamActor1, GetEventPause)
          p.expectMsg(true)
        }
      }
      runOn(publisher2) {
        // stream should stop sending events after receiving transitionRequest
        val p = TestProbe()
        implicit val timeout: Timeout = Timeout(remaining)
        awaitAssert {
          val streamActor2 = Await.result(cluster.system.actorSelection(RootActorPath(node(publisher2).address) / "user" / "Stream*").resolveOne(), remaining)
          /*
          p.send(streamActor2, GetMaxEventInterval)
          val maxEventInterval: Long = expectMsgClass(classOf[Long])
          p.send(streamActor2, GetTimeSinceLastEvent)
          val timeSinceLast: Long = expectMsgClass(classOf[Long])
          log.warning("MAX EVENT INTERVAL STREAM1: " + maxEventInterval + " time since last: " + timeSinceLast + " is larger: " + (timeSinceLast > maxEventInterval))
          //assert(timeSinceLast > maxEventInterval, "there must be a pause longer than the longest event interval since transition start")
          */
          p.send(streamActor2, GetEventPause)
          p.expectMsg(true)
        }
      }

      testConductor.enter("sending new events on stream operators stopped and remaining events arrived at client")
    }

    "change transition status to complete" in within(10.second) {
      runOn(client) {
        // wait until transition is complete
        clientProbe.awaitAssert(() => {
          clientProbe.expectMsg(SetStatus(0))
        }, remaining)

        val p = TestProbe()
        p.awaitAssert(() => {
          p.send(graph.mapek.knowledge, GetTransitionStatus)
          p.expectMsg(0)
        }, remaining)
      }
      testConductor.enter("change transition status to complete")
    }

    "change active placement algorithm to GlobalOptimalBDP" in within(t) {
      runOn(client) {
        awaitAssert(() => {
          graph.mapek.knowledge ! GetPlacementStrategyName
          expectMsg(GlobalOptimalBDPAlgorithm.name)
        }, remaining)
      }
      testConductor.enter("update active placement algorithm in knowledge")
    }

    "receive new events on client after transition is done" in within(t) {
      runOn(client) {
        clientProbe.expectMsgClass(classOf[Event2])
      }
      testConductor.enter("receiving new events")
    }

    "send new operators to mapek knowledge and all should have the same host" in within(t) {
      runOn(client) {
        awaitAssert(() => {
        clientWrapperActor ! GetOperators
        expectMsgPF(remaining) {
          case l: List[ActorRef] =>
            val ops = l.toVector
            println("OPERATORS after transition: \n" + ops.mkString("\n"))
            assert(!ops.contains(rootOperator), s"old join operator ${rootOperator} must not be contained in operator list on knowledge")
            assert(ops.size == 3, "mapek knowledge actor must know 3 operators")
            assert(ops(0).path.address == ops(1).path.address && ops(1).path.address == ops(2).path.address,
              "mapek knowledge actor must know the 3 new operators (which must all be on the same host)")
        }
        }, remaining)
      }
      testConductor.enter("update operator hosts on knowledge")
    }

    //TODO currently we just record arriving events and send the events within the window to the successor, which re-sends them to itself; try transfering esper state directly instead
    "transfer events from the join window to the new operator" in within(1.second) {
      runOn(client) {

      }
      testConductor.enter("test end")
    }

  }
}
