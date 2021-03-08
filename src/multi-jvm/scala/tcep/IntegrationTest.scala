package tcep

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, Props, RootActorPath}
import akka.testkit.TestProbe
import tcep.data.Queries.{Conjunction11, Filter2, Query, Stream1}
import tcep.dsl.Dsl.{Seconds => _, TimespanHelper => _}
import tcep.simulation.tcep.MobilityData

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.runtime.universe.typeOf

// need one concrete test class per node
class IntegrationMultiJvmNode1 extends IntegrationMultiNodeTestSpec
class IntegrationMultiJvmNode2 extends IntegrationMultiNodeTestSpec
class IntegrationMultiJvmClient extends IntegrationMultiNodeTestSpec
class IntegrationMultiJvmPublisher1 extends IntegrationMultiNodeTestSpec
class IntegrationMultiJvmPublisher2 extends IntegrationMultiNodeTestSpec

abstract class IntegrationMultiNodeTestSpec extends MultiJVMTestSetup {

  import TCEPMultiNodeConfig._

  "Integration test MFGS transition" must {
    "be able to send a query between hosts (Serialization and Deserialization)" in within(FiniteDuration(5, TimeUnit.SECONDS)) {
      runOn(client) {
        //val query = stream[Int]("A").and(stream[MobilityData]("B")).where((_, _) => true)
        val query = Filter2(Conjunction11[Int, MobilityData](Stream1[Int]("A", Set()), Stream1[MobilityData]("B", Set()), Set()), _ => true, Set())
        val probe = TestProbe("X")

        val replier = Await.result(system.actorSelection(RootActorPath(node(publisher1).address) / "user" / "replier").resolveOne()(remaining), remaining)
        probe.send(replier, query)
        probe.expectMsgPF() {
          case received: Filter2[_, _] =>
            assert(received.types == Vector(typeOf[Int].toString, typeOf[MobilityData].toString))
            assert(received.sq == query.sq)
            println(s"result is: $received with types ${received.types}")
            println(received.cond)
        }
      }

      runOn(publisher1) {
        val r = system.actorOf(Props(new Actor {
          override def receive: Receive = {
            case q: Query => println(s"received $q")
              sender() ! q
          }
        }), "replier")
      }
      testConductor.enter("serialization test end")
    }
    /*
        "successfully deploy the operator graph to the candidates and transit from MDCEP to Relaxation" in within(FiniteDuration(5, TimeUnit.SECONDS)) {
          runOn(client) {

            val dir = "logs"
            val directory = if (new File(dir).isDirectory) Some(new File(dir)) else {
              log.info("Invalid directory path")
              None
            }
            val simRef: ActorRef = system.actorOf(Props(new SimulationSetup(directory, 2, transitionMode = TransitionConfig(), durationInMinutes = Some(2), startingPlacementAlgorithm = "Relaxation", overridePublisherPorts = Some(Set(2501, 2502)))), "SimulationSetup")
            system.scheduler.scheduleOnce(90 seconds)(() => simRef ! PoisonPill)
            val start = System.currentTimeMillis()
            while (System.currentTimeMillis() - start <= 90000) {}

            }
            val start = System.currentTimeMillis()
            while (System.currentTimeMillis() - start <= 150000) {}

      }
    }*/
    }
/*
  "Integration test SMS transition" must {
    "successfully deploy the operator graph to the candidates and transit from Relaxation to Starks" in {
      testConductor.enter("test integration test start")

      runOn(client) {

        val dir = "logs"
        val directory = if (new File(dir).isDirectory) Some(new File(dir)) else {
          log.info("Invalid directory path")
          None
        }
        val publisherNames = Some(Vector("P:localhost:2501", "P:localhost:2502")) // need to override publisher names because all nodes have hostname (localhost) during test
        val simRef: ActorRef = system.actorOf(Props(new SimulationSetup(directory, Mode.TEST_SMS, Some(2), Some("Relaxation"), publisherNames)), "SimulationSetup")
      }

      val start = System.currentTimeMillis()
      while (System.currentTimeMillis() - start <= 150000) {}
      testConductor.enter("integration test end")
    }
  }

  "Integration test SPLC data collection" must {
    "successfully deploy the operator graph to the candidates" in {
      testConductor.enter("test integration test start")

      runOn(client) {

        val dir = "logs"
        val directory = if (new File(dir).isDirectory) Some(new File(dir)) else {
          log.info("Invalid directory path")
          None
        }
        val publisherNames = Some(Vector("P:localhost:2501", "P:localhost:2502")) // need to override publisher names because all nodes have hostname (localhost) during test
        val simRef: ActorRef = system.actorOf(Props(new SimulationSetup(directory, Mode.SPLC_DATACOLLECTION, Some(2), Some("Rizou"), publisherNames)), "SimulationSetup")

      }

      val start = System.currentTimeMillis()
      while (System.currentTimeMillis() - start <= 150000) {}
      testConductor.enter("integration test end")
    }
  }
  */
}

