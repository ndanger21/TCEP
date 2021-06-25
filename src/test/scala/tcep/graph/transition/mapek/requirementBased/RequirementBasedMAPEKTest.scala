
package tcep.graph.transition.mapek.requirementBased

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import tcep.data.Queries
import tcep.data.Queries.{Requirement, Stream1}
import tcep.data.Structures.MachineLoad
import tcep.dsl.Dsl._
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.transition.MAPEK.{AddRequirement, RemoveRequirement, SetClient}
import tcep.graph.transition.TransitionRequest
import tcep.placement.GlobalOptimalBDPAlgorithm
import tcep.placement.benchmarking.BenchmarkingNode
import tcep.placement.manets.StarksAlgorithm
import tcep.placement.sbon.PietzuchAlgorithm

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration

class RequirementBasedMAPEKTest extends TestKit(ActorSystem())  with WordSpecLike with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    super.afterAll()
    system.terminate()
  }

  val latencyRequirement = latency < timespan(500.milliseconds) otherwise None
  val messageHopsRequirement = hops < 3 otherwise None
  val loadRequirement = load < MachineLoad(1.0) otherwise None
  implicit val t = FiniteDuration(5, TimeUnit.SECONDS)

  class WrapperActor(transitionConfig: TransitionConfig, initialRequirements: Set[Requirement]) extends Actor {
    var mapek: RequirementBasedMAPEK = _
    var subscribers = ListBuffer[ActorRef]()
    override def preStart(): Unit = {
      super.preStart()
      val query = Stream1[Int]("A", initialRequirements)
      mapek = new RequirementBasedMAPEK(context, query, transitionConfig, BenchmarkingNode.selectBestPlacementAlgorithm(List(), Queries.pullRequirements(query, List()).toList))
      mapek.knowledge ! SetClient(testActor)
    }

    override def receive: Receive = {

      case message => mapek.monitor.forward(message)
    }
  }

  "RequirementBasedMAPEK" must {
    "propagate msgHops requirement addition, return Starks algorithm" in within(t) {
      val w = system.actorOf(Props( new WrapperActor(TransitionConfig(), Set(latencyRequirement))))

      Thread.sleep(100)
      w ! RemoveRequirement(Seq(latencyRequirement))
      w ! AddRequirement(Seq(messageHopsRequirement, loadRequirement))
      expectMsgPF(remaining) {
        case t: TransitionRequest => assert(t.placementStrategyName === StarksAlgorithm.name)
      }
    }
  }

  "RequirementBasedMAPEK" must {
    "select GlobalOptimalBDP after latency requirement is removed and latency + msgHops requirement are added (Relaxation->GlobalOptimalBDP)" in within(t) {
      val w = system.actorOf(Props( new WrapperActor(TransitionConfig(), Set(latencyRequirement))))
      Thread.sleep(100)
      w ! RemoveRequirement(Seq(latencyRequirement))
      w ! AddRequirement(Seq(latencyRequirement, messageHopsRequirement))
      expectMsgPF(remaining) {
        case t: TransitionRequest => assert(t.placementStrategyName === GlobalOptimalBDPAlgorithm.name)
      }
    }
  }


  "RequirementBasedMAPEK" must {
    "select Relaxation after latency and msgHops requirement is removed and latency requirement is added (GlobalOptimalBDP->Relaxation)" in within((t)) {
      val w = system.actorOf(Props( new WrapperActor(TransitionConfig(), Set(latencyRequirement, messageHopsRequirement))))
      Thread.sleep(100)
      w ! RemoveRequirement(Seq(latencyRequirement, messageHopsRequirement))
      w ! AddRequirement(Seq(latencyRequirement, loadRequirement))
      expectMsgPF(remaining) {
        case t: TransitionRequest => assert(t.placementStrategyName === PietzuchAlgorithm.name)
      }
    }
  }
}
