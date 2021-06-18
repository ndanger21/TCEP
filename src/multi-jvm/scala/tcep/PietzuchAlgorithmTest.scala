package tcep.placement.sbon

import java.util.concurrent.TimeUnit

import akka.cluster.Member
import org.discovery.vivaldi.Coordinates
import tcep.data.Queries
import tcep.data.Queries._
import tcep.dsl.Dsl
import tcep.dsl.Dsl.{Seconds => _, TimespanHelper => _, _}
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.placement.QueryDependenciesWithCoordinates
import tcep.{MultiJVMTestSetup, TCEPMultiNodeConfig}

import scala.concurrent.Await
import scala.concurrent.duration._


// need one concrete test class per node
class PietzuchMultiJvmNode1 extends PietzuchMultiNodeTestSpec
class PietzuchMultiJvmNode2 extends PietzuchMultiNodeTestSpec
class PietzuchMultiJvmClient extends PietzuchMultiNodeTestSpec
class PietzuchMultiJvmPublisher1 extends PietzuchMultiNodeTestSpec
class PietzuchMultiJvmPublisher2 extends PietzuchMultiNodeTestSpec

abstract class PietzuchMultiNodeTestSpec extends MultiJVMTestSetup {

  import TCEPMultiNodeConfig._

  private def initUUT(): PietzuchAlgorithm.type = {
    val uut = PietzuchAlgorithm
    Await.result(uut.initialize(), FiniteDuration(20, TimeUnit.SECONDS))
    testConductor.enter("initialization complete")
    uut
  }

  "PlacementStrategy getNClosestNeighbours" must {
    "return the two closest neighbours" in within(new FiniteDuration(60, TimeUnit.SECONDS)) {

      val uut = initUUT()
      val clientCoords = new Coordinates(20, 0, 0)
      val publisher1 = new Coordinates(30, 0, 0)
      val publisher2 = new Coordinates(3000, 0, 0)
      val host1 = new Coordinates(40, 0, 0)
      val host2 = new Coordinates(10, 0, 0)
      setCoordinatesForPlacement(uut, clientCoords, publisher1, publisher2, host1, host2)

      runOn(client) {
        val res = uut.getNClosestNeighboursToCoordinatesByMember(2, new Coordinates(50, 0, 0))
        res should have size 2
        res.head._2.x should be(40.0d)
        res.last._2.x should be(30.0d)
      }
      testConductor.enter("test1-complete")
    }
  }

  "PlacementStrategy - getCoordinatesOfNode" must {
    "return the coordinates of a different cluster member" in within(new FiniteDuration(30, TimeUnit.SECONDS)) {

      val uut = initUUT()
      val clientCoords = new Coordinates(20, 0, 0)
      val p1 = new Coordinates(30, 0, 0)
      val p2 = new Coordinates(3000, 0, 0)
      val host1 = new Coordinates(40, 0, 0)
      val host2 = new Coordinates(10, 0, 0)
      setCoordinatesForPlacement(uut, clientCoords, p1, p2, host1, host2)

      runOn(node1) {
        val members = cluster.state.members
        val cres: Coordinates = uut.getCoordinatesOfNodeBlocking( members.find(m => m.hasRole("Subscriber")).get)
        cres should not be Coordinates.origin
        assert(cres.equals(new Coordinates(20, 0, 0)))
        val publisherCoords = Await.result(uut.getCoordinatesOfMembers(members.filter(_.hasRole("Publisher"))), remaining)
        assert(publisherCoords.find(_._1.address == node(publisher1).address).get._2 == Coordinates(30, 0, 0))
        assert(publisherCoords.find(_._1.address == node(publisher2).address).get._2 == Coordinates(3000, 0, 0))
      }
      testConductor.enter("test2-complete")
    }
  }

  "PlacementStrategy - getLoadOfNode" must {
    "return the load percentage of the machine (between 0 and 8)" in within(new FiniteDuration(15, TimeUnit.SECONDS)) {

      val uut = initUUT()

      runOn(node1) {
        val members = getMembersWithRole(cluster, "Candidate")
        val res: Double = Await.result(uut.getLoadOfNode(members.find(m => !m.equals(cluster.selfMember)).get), uut.resolveTimeout.duration)
        assert(res <= 18.0d && res >= 0.0d)
      }
      testConductor.enter("test-complete")
    }
  }

  "PietzuchAlgorithm - calculateVCSingleOperator() with one parent" must {
    "return virtual coordinates that are closer to the client node than the single parent node" in {
      val uut = initUUT()
      runOn(client) {
        val op = stream[Int](pNames(0))
        val dependencyCoords = QueryDependenciesWithCoordinates(Map(PublisherDummyQuery(pNames(0)) -> Coordinates(50, 0, 0)), Map(ClientDummyQuery() -> Coordinates(50, 50, 0)))
        val dataRateEstimates = Queries.estimateOutputBandwidths(op)
        val virtualCoords = Await.result(uut.calculateVCSingleOperator(op, dependencyCoords, dataRateEstimates), uut.requestTimeout)
        assert(virtualCoords.distance(Coordinates(50, 25, 0)) <= 6.0d, "virtual coordinates should be in the middle of parent and client")
      }
      testConductor.enter("test-calculateVCSingleOperator-1-complete")
    }
  }

  "PietzuchAlgorithm - calculateVCSingleOperator() with two parents" must {
    "return virtual coordinates that are closer to the client node than the parent node since join operator has high bandwidth usage" in within(new FiniteDuration(15, TimeUnit.SECONDS)) {

      val uut = initUUT()
      val clientC = Coordinates(0, -50, 0)
      val publisher1 = Coordinates(-50, 50, 0)
      val publisher2 = Coordinates(50, 50, 0)
      val host1 = Coordinates(0, 50, 0)
      val host2 = Coordinates(50, 0, 0)
      setCoordinatesForPlacement(uut, clientC, publisher1, publisher2, host1, host2)

      runOn(client) {
        // p1       p2
        //
        //    (0,0)
        //
        //    client
        val s1 = stream[Int](pNames(0))
        val s2 = stream[Int](pNames(1))
        val w = Dsl.slidingWindow(Dsl.Seconds(3))
        val op = s1.join(s2, w, w) // we assume streams are already deployed on publishers
        val dependencyCoords = QueryDependenciesWithCoordinates(Map(s1 -> publisher1, s2 -> publisher2), Map(ClientDummyQuery() -> clientC))
        val dataRateEstimates = Queries.estimateOutputBandwidths(op)
        val virtualCoords = Await.result(uut.calculateVCSingleOperator(op, dependencyCoords, dataRateEstimates), uut.requestTimeout)
        //println(virtualCoords)
        assert(virtualCoords.x > -1 && virtualCoords.y > -50 &&
          virtualCoords.x < 1 && virtualCoords.y < -10,
          "virtual coords should lie somewhere on the line between (0,0,0) and (0,-50,0)")
        val distToPub = virtualCoords.distance(publisher1)
        val distToClient = virtualCoords.distance(clientC)
        assert(distToPub > distToClient && distToPub >= publisher1.distance(clientC) * 0.65,
          "virtual coords must lie further from parents (two forces) than to (single force) client due to high bandwidth of join operator")
      }
      testConductor.enter("test-calculateVCSingleOperator()-2-complete")
    }
  }

  "PietzuchAlgorithm - selectHostFromCandidates" must {
    "return the candidate Member that has minimal load among the 3 nodes closest to the virtual coords" in {
      val uut = initUUT()
      val clientC = new Coordinates(50, 50, 0)
      val publisher1 = new Coordinates(25, 25, 0)
      val publisher2 = new Coordinates(25, 25, 0)
      val host1 = new Coordinates(50, 0, 0)
      val host2 = new Coordinates(0, 50, 0)
      setCoordinatesForPlacement(uut, clientC, publisher1, publisher2, host1, host2)

      runOn(client) {
        val op = stream[Int](pNames(0))
        val candidates = uut.memberCoordinates.filter(!_._1.hasRole("Publisher"))
        val vc = new Coordinates(50, 50, 0)
        val result = Await.result(uut.selectHostFromCandidates(vc, candidates), uut.requestTimeout)
        assert(getMembersWithRole(cluster, "Candidate").contains(result._1) && !result._1.equals(getMembersWithRole(cluster, "Publisher").head),
          "must place operator among the three closest non-publisher nodes")
        assert(!result._2.values.toSeq.sortWith(_ < _).take(uut.k).exists(l => l < result._2(result._1)), "result must be the candidate with minimum load")
      }
    }
    testConductor.enter("test physical placement complete")
  }
  //TODO move to Queries test
  "PietzuchAlgorithm - extractOperators()" must {
    "return a map of all operators and their assorted parent and child operators that exist in a query" in {
      runOn(client) {
        val query =
          stream[Boolean](pNames(0))
            .and(stream[Boolean](pNames(1)))
            .and(stream[Boolean](pNames(0)).where(_ == true))

        val result = Queries.extractOperators(query)
        assert(result.nonEmpty)
        assert(result.size == 10, "result must contain all 6 operators, 3 publisher dummy operators, and 1 ClientDummyOperator")
        assert(result.count(o => o._1.isInstanceOf[Stream1[_]]) == 3, "result must contain 3 stream operators")
        assert(result.count(o => o._1.isInstanceOf[Filter1[_]]) == 1, "result must contain one filter operator")
        assert(result.count(o => o._1.isInstanceOf[Conjunction21[_, _, _]]) == 1,
          "result must contain one conjunction21 operator")
        assert(result.count(o => o._1.isInstanceOf[Conjunction11[_, _]]) == 1,
          "result must contain one conjunction11 operator")
        assert(result.filter(o => o._1.isInstanceOf[Stream1[_]]).forall(o =>
          o._2._1.parents.isDefined && o._2._1.parents.get.nonEmpty && o._2._1.parents.get.head.isInstanceOf[PublisherDummyQuery]),
          "stream operators must have dummy operator parents")
        assert(result.exists(o => o._1.isInstanceOf[Conjunction21[_, _, _]] && o._2._1.child.isDefined && o._2._1.child.get.isInstanceOf[ClientDummyQuery]),
          "conjunction21 operator is the root of the query and has no children")
        assert(result.exists(o => o._1.isInstanceOf[Conjunction21[_, _, _]] &&
          o._2._1.parents.get.exists(p => p.isInstanceOf[Filter1[_]]) &&
          o._2._1.parents.get.exists(p => p.isInstanceOf[Conjunction11[_, _]])),
          "conjunction21 operator has filter and conjunction11 as parents")
        assert(result.exists(o => o._1.isInstanceOf[ClientDummyQuery] && o._2._1.parents.isDefined && o._2._1.parents.get.head == query && o._2._1.child.isEmpty))
      }
      testConductor.enter("test extractOperators complete")
    }
  }

  "PietzuchAlgorithm - calculateVirtualPlacementWithCoords() with one operator" must {
    "return the mapping of operator to coordinates for the given query" in {
      val uut = initUUT()
      val clientC = new Coordinates(0, 0, 0)
      val publisher1 = new Coordinates(-100, 0, 0)
      val publisher2 = new Coordinates(-100, 0, 0)
      val host1 = new Coordinates(-75, 0, 0)
      val host2 = new Coordinates(-50, 0, 0)
      setCoordinatesForPlacement(uut, clientC, publisher1, publisher2, host1, host2)

      runOn(client) {
        val query = stream[Int](pNames(0))
        implicit val dependencyData = Queries.extractOperators(query)
        val result = uut.calculateVirtualPlacementWithCoords(query, clientC, Map(pNames(0) -> publisher1))
        assert(result(query).distance(new Coordinates(-50, 0, 0)) <= 2.0, "single operator should be placed between client and publisher")
      }
      testConductor.enter("test calculateVirtualPlacementWithCoords() with one operator complete")
    }
  }

  "PietzuchAlgorithm - calculateVirtualPlacementWithCoords()" must {
    "return the mapping of operator to coordinates for the given query" in {
      val uut = initUUT()
      val clientC = new Coordinates(0, 0, 0)
      val publisher1 = new Coordinates(-100, 0, 0)
      val publisher2 = new Coordinates(-100, 0, 0)
      val host1 = new Coordinates(-66.666, 0, 0)
      val host2 = new Coordinates(-33.333, 0, 0)
      setCoordinatesForPlacement(uut, clientC, publisher1, publisher2, host1, host2)

      runOn(client) {
        val s = stream[Int](pNames(0))
        val f1 = Filter1[Int](s, _ => true, Set())
        val f2 = Filter1[Int](f1, _ => true, Set())
        implicit val dependencyData = Queries.extractOperators(f2)
        val result = uut.calculateVirtualPlacementWithCoords(f2, clientC, Map(pNames(0) -> publisher1))
        val eps = 8.0
        assert(result(s).distance(new Coordinates(-75, 0, 0)) <= eps,"stream operator should be near publisher")
        assert(result(f1).distance(new Coordinates(-50, 0, 0)) <= eps, "filter 1 should be between publisher and client")
        assert(result(f2).distance(new Coordinates(-25, 0, 0)) <= eps, "filter 2 should be near client")
        //println("DEBUG \n " + result.map("\n" + _))
      }
      testConductor.enter("test calculateVirtualPlacementWithCoords() complete")
    }
  }


  "PietzuchAlgorithm - virtual and then physical placement" must {
    "place operators on the section [pub2 - host1 - host2] of the line [pub1 - pub2 - host1 - host2 - clientC]" in {

      val uut = initUUT()
      uut.k = 1
      val clientC = new Coordinates(100, 0, 0)
      val pub1 = new Coordinates(0, 0, 0)
      val pub2 = new Coordinates(25, 0, 0)
      val host1 = new Coordinates(50, 0, 0)
      val host2 = new Coordinates(75, 0, 0)
      setCoordinatesForPlacement(uut, clientC, pub1, pub2, host1, host2)

      runOn(publisher1) {
        val s = stream[Int](publishers.head._1)
        val f1 = Filter1[Int](s, _ => true, Set())
        val f2 = Filter1[Int](f1, _ => true, Set())
        implicit val dependencyData = Queries.extractOperators(f2)
        val dataRateEstimates = Queries.estimateOutputBandwidths(f2)
        val candidates = Await.result(uut.getCoordinatesOfMembers(uut.findPossibleNodesToDeploy(cluster)), uut.requestTimeout)
        val virtualCoordinates: Map[Query, Coordinates] = Await.result(uut.initialVirtualOperatorPlacement(f2, publishers), uut.requestTimeout)
        val physicalPlacements: Map[Query, Member] = virtualCoordinates.map(o => o._1 -> Await.result(uut.findHost(o._2, candidates, o._1, Dependencies(Map(), Map()), dataRateEstimates), uut.requestTimeout).member)
        assert(physicalPlacements.values.toSet.map((m: Member) => m.address).subsetOf(
          getMembersWithRole(cluster, "Candidate").map(c => c.address)),
          "all operators must be hosted on candidates")

        //val candidateCoordinates = Await.result(uut.getCoordinatesOfMembers(uut.findPossibleNodesToDeploy(cluster)), uut.requestTimeout)
        //println(candidateCoordinates.mkString("\n") + "\n")
        //println("\n" + physicalPlacements.map(e => e._1 -> s"${e._2} ${e._2.getRoles} @ ${candidateCoordinates(e._2)}").mkString("\n"))
        assert(physicalPlacements(s).address == node(publisher2).address, "stream operator should be on publisher2")
        assert(physicalPlacements(f1).address == node(node1).address, "filter1 operator should be on host1")
        assert(physicalPlacements(f2).address == node(node2).address, "filter2 operator should be on host2")
      }
      testConductor.enter("test initialVirtualOperatorPlacement and then physical placement complete")
    }
  }

  "PietzuchAlgorithm - BDP test" must {
    "produce a placement that has minimal BDP" in {

      val uut = initUUT()
      val clientC = new Coordinates(50, 50, 0)
      val publisher1 = new Coordinates(0, 0, 0)
      val publisher2 = new Coordinates(0, 0, 0)
      val host1 = new Coordinates(50, 0, 0)
      val host2 = new Coordinates(25, 25, 0)
      setCoordinatesForPlacement(uut, clientC, publisher1, publisher2, host1, host2)

      runOn(client) {

        val s = stream[Int](pNames(0))
        val f1 = Filter1[Int](s, _ => true, Set())
        //val f2 = Filter1[Int](f1, _ => true, Set())
        val candidates = Await.result(uut.getCoordinatesOfMembers(uut.findPossibleNodesToDeploy(cluster)), uut.requestTimeout)
        // find all possible mappings of the entire query to the candidate nodes (hosts + client)
        val h1 = candidates.head
        val h2 = candidates.last
        val perm: List[List[(Member, Coordinates)]] = candidates.toList.permutations.toList
        val permWithDuplicates = List(candidates.head, candidates.head) :: List(candidates.last, candidates.last) :: perm
        //println(s"\nDEBUG permutations: ${permWithDuplicates.map("\n" + _)}")

        val allPossibleMappings: List[Map[Query, (Member, Coordinates)]] = List(
          Map(s -> h1, f1 -> h1),
          Map(s -> h1, f1 -> h2),
          Map(s -> h2, f1 -> h1),
          Map(s -> h2, f1 -> h2)
        )
        val BDPs = allPossibleMappings.map(m => m -> getTestBDP(m, clientC, publisher1))
        val minBDP = BDPs.minBy(m => m._2)._2
        //println(s"\nDEBUG BDPs: ${BDPs.map(e => s"\n ${e._1.map(t => s"${t._1} -> ${t._2._2}")}  | ${e._2}")}")
        //println(s"\nDEBUG minimum BDP: $minBDP")

        uut.k = 1 // disable load-based host choice for repeatable tests
        implicit val dependencyData = Queries.extractOperators(f1)
        val dataRateEstimates = Queries.estimateOutputBandwidths(f1)
        val virtualCoords: Map[Query, Coordinates] = uut.calculateVirtualPlacementWithCoords(f1, clientC, Map(pNames(0) -> publisher1))
        // dependencies empty since they're only needed for statistics update
        val physicalPlacement: Map[Query, (Member, Coordinates)] = virtualCoords.map(vc => vc._1 -> Await.result(
          uut.findHost(vc._2, candidates, vc._1, Dependencies(Map(), Map()), dataRateEstimates), uut.requestTimeout))
          .map(e => e._1 -> (e._2.member, uut.getCoordinatesOfNodeBlocking(e._2.member)))
        val placementBDP = getTestBDP(physicalPlacement, clientC, publisher1)
        assert(placementBDP == minBDP, "BDP of placement must be minimum possible BDP of all possible placements")
      }

      testConductor.enter("test minimal BDP complete")
    }
  }
}
