package tcep

import akka.cluster.Member
import org.discovery.vivaldi.Coordinates
import org.scalatest.mockito.MockitoSugar
import tcep.data.Queries
import tcep.data.Queries._
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.placement.GlobalOptimalBDPAlgorithm

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

// need one concrete test class per node
class GlobalOptimalBDPMultiJvmNode1 extends      GlobalOptimalBDPMultiNodeTestSpec
class GlobalOptimalBDPMultiJvmNode2 extends      GlobalOptimalBDPMultiNodeTestSpec
class GlobalOptimalBDPMultiJvmClient extends     GlobalOptimalBDPMultiNodeTestSpec
class GlobalOptimalBDPMultiJvmPublisher1 extends GlobalOptimalBDPMultiNodeTestSpec
class GlobalOptimalBDPMultiJvmPublisher2 extends GlobalOptimalBDPMultiNodeTestSpec

abstract class GlobalOptimalBDPMultiNodeTestSpec extends MultiJVMTestSetup with MockitoSugar {

  import TCEPMultiNodeConfig._

  private def initUUT(): GlobalOptimalBDPAlgorithm.type = {
    val uut = GlobalOptimalBDPAlgorithm
    Await.result(uut.initialize(), FiniteDuration(20, TimeUnit.SECONDS))
    uut
  }

  "GlobalOptimalBDPAlgorithm" must {
    "select the node with minimum BDP sum to publishers and subscriber" in within(5 seconds){
      testConductor.enter("initialization complete")
      runOn(client) {
        val uut = initUUT()
        val clientC = new Coordinates(0, -10, 0)
        val pub1 = new Coordinates(-40, 30, 0) // pnames(0)
        val pub2 = new Coordinates(40, 30, 0) // pnames(1)
        val host1 = new Coordinates(0, 0, 0)
        val host2 = new Coordinates(40, -10, 0)
        setCoordinatesForPlacement(uut, clientC, pub1, pub2, host1, host2)
        val s1 = Stream1[Int](pNames(0), Set())
        val s2 = Stream1[Int](pNames(1), Set())
        val and = Conjunction11[Int, Int](s1, s2, Set())
        val f = Filter2[Int, Int](and, _ => true, Set())

        val candidates: Map[Member, Coordinates] = Await.result(uut.getCoordinatesOfMembers(cluster.state.members), uut.requestTimeout)
        val p1Member = cluster.state.members.find(_.address == node(publisher1).address).get
        val p2Member = cluster.state.members.find(_.address == node(publisher2).address).get

        val s1Placement = s1 -> Await.result(uut.applyGlobalOptimalBDPAlgorithm(s1, f, Dependencies(Map(publishers(pNames(0)) -> PublisherDummyQuery(pNames(0))), Map(None -> and))), remaining)
        val s2Placement = s2 -> Await.result(uut.applyGlobalOptimalBDPAlgorithm(s2, f, Dependencies(Map(publishers(pNames(1)) -> PublisherDummyQuery(pNames(1))), Map(None -> and))), remaining)
        // use publisher actors here even if they are not the real parents; irrelevant here
        val andPlacement = and -> Await.result(uut.applyGlobalOptimalBDPAlgorithm(and, f, Dependencies(Map(publishers(pNames(0)) -> s1), Map(None -> f))), remaining)
        val fPlacement = f -> Await.result(uut.applyGlobalOptimalBDPAlgorithm(f, f, Dependencies(Map(publishers(pNames(0)) -> and), Map(Some(clientProbe.ref) -> ClientDummyQuery()))), remaining)
        val placement = Map(s1Placement, s2Placement, andPlacement, fPlacement)
        //println(s"\n placement bdp: \n ${placement.map(e => s"${e._1} -> ${e._2._1.member.address} @ ${e._2._2}").mkString("\n")}")

        assert(placement.forall(_._2._1.member.address == placement.head._2._1.member.address), "all operators should have the same host")
        assert(placement.forall(_._2._2 == placement.head._2._2), "all operators should have the same BDP")
        val dataRateEstimates = Queries.estimateOutputBandwidths(f)
        val allBDPs = candidates.map(c => c._1 -> {
          val p1BDP = dataRateEstimates(PublisherDummyQuery(s1.publisherName)) * 0.001 * candidates(p1Member).distance(c._2)
          val p2BDP = dataRateEstimates(PublisherDummyQuery(s2.publisherName)) * 0.001 * candidates(p2Member).distance(c._2)
          val cBDP = dataRateEstimates(f) * 0.001 * candidates(cluster.state.members.find(_.hasRole("Subscriber")).get).distance(c._2)
          p1BDP + p2BDP + cBDP
        })

        //println(s"allBDPS: \n ${allBDPs.mkString("\n")}")
        assert(placement.head._2._1.member == allBDPs.minBy(_._2)._1, "BDP of placement must be on the member with minimum possible BDP of all possible placements")
        assert(math.abs(placement.head._2._2 - allBDPs.minBy(_._2)._2) < 1e-3, "BDP of placement must be minimum possible BDP of all possible placements")


        // manually reset placement
        uut.singleNodePlacement = None
        Await.result((uut.getVirtualOperatorPlacementCoords(f, publishers)(ec, cluster, mutable.LinkedHashMap())), remaining)
        assert(uut.singleNodePlacement.isDefined, "singleNodePlacement must be defined after calling initialVirtualOperatorPlacement")
        assert(uut.singleNodePlacement.get.host == placement.head._2._1.member, "initialVirtualOperatorPlacement should return the same host as recursive deployment")
      }
      testConductor.enter("test minimal BDP complete")
    }

    "return a valid placement upon calling initialVirtualPlacement" in within(5 seconds) {
      runOn(client) {
        val uut = initUUT()
        val clientC = new Coordinates(0, -10, 0)
        val pub1 = new Coordinates(-40, 30, 0) // pnames(0)
        val pub2 = new Coordinates(40, 30, 0) // pnames(1)
        val host1 = new Coordinates(0, 0, 0)
        val host2 = new Coordinates(40, -10, 0)
        setCoordinatesForPlacement(uut, clientC, pub1, pub2, host1, host2)
        val s1 = Stream1[Int](pNames(0), Set())
        val s2 = Stream1[Int](pNames(1), Set())
        val and = Conjunction11[Int, Int](s1, s2, Set())
        val f = Filter2[Int, Int](and, _ => true, Set())
        implicit val dependencyMap = Queries.extractOperatorsAndThroughputEstimates(f)
        val placement = Await.result(GlobalOptimalBDPAlgorithm.initialVirtualOperatorPlacement(f, publishers), remaining)
        assert(placement.nonEmpty)
        assert(uut.singleNodePlacement.isDefined)
        assert(placement.size == 4)
        assert(placement.forall(_._2.member == placement.head._2.member), "all operators must be placed on same node")
      }
      testConductor.enter("test end")
    }
  }

}