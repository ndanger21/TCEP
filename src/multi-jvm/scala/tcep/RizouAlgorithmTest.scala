package tcep

import akka.cluster.Member
import org.discovery.vivaldi.Coordinates
import tcep.data.Queries
import tcep.data.Queries._
import tcep.dsl.Dsl.{Seconds => _, TimespanHelper => _, _}
import tcep.placement.mop.RizouAlgorithm
import tcep.placement.{QueryDependencies, QueryDependenciesWithCoordinates}

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

// need one concrete test class per node
class RizouMultiJvmNode1 extends RizouMultiNodeTestSpec
class RizouMultiJvmNode2 extends RizouMultiNodeTestSpec
class RizouMultiJvmClient extends RizouMultiNodeTestSpec
class RizouMultiJvmPublisher1 extends RizouMultiNodeTestSpec
class RizouMultiJvmPublisher2 extends RizouMultiNodeTestSpec

abstract class RizouMultiNodeTestSpec extends MultiJVMTestSetup {

  import TCEPMultiNodeConfig._

  val bdpAccuracy = 0.1
  val coordAccuracy = 4.0

  private def initUUT(): RizouAlgorithm.type = {
    val uut = RizouAlgorithm
    Await.result(uut.initialize(), FiniteDuration(20, TimeUnit.SECONDS))
    testConductor.enter("initialization complete")
    uut
  }

  "RizouAlgorithm - calculateVCSingleOperator() with two parents" must {
    "return virtual coordinates that are closer to the client node than the parent nodes" in within(new FiniteDuration(15, TimeUnit.SECONDS)) {
      val uut = initUUT()
      val clientC = new Coordinates(50, 50, 0)
      val publisher1 = Coordinates(-50, 0, 0)
      val publisher2 = Coordinates(0, -50, 0)
      val host1 = new Coordinates(0, 50, 0)
      val host2 = new Coordinates(50, 0, 0)
      setCoordinatesForPlacement(uut, clientC, publisher1, publisher2, host1, host2)

      runOn(client) {
        val s1 = stream[Int](pNames(0))
        val s2 = stream[Int](pNames(1))
        val op = s1.and(s2) // we assume streams are already deployed on publishers
        val dependencyCoords = QueryDependenciesWithCoordinates(Map(s1 -> publisher1, s2 -> publisher2), Map(ClientDummyQuery() -> clientC))
        val dataRateEstimates = Queries.estimateOutputBandwidths(op)
        val virtualCoords = Await.result(uut.calculateVCSingleOperator(op, dependencyCoords, dataRateEstimates), uut.requestTimeout)
        assert(virtualCoords.x < 0 && virtualCoords.y < 0 &&
                 virtualCoords.x > -25 && virtualCoords.y > -25,
               "virtual coords should lie somewhere on the line between (0,0,0) and (-50,-50,0)")
        val distToPub1 = virtualCoords.distance(publisher1)
        val distToPub2 = virtualCoords.distance(publisher2)
        val distToClient = virtualCoords.distance(clientC)
        assert(distToPub1 < distToClient && distToPub2 < distToClient, "virtual coords must lie closer to publishers than to client")
        assert(distToPub1 <= publisher1.distance(clientC) * 0.45,
               "virtual coords must lie closer to parents (two forces) than to (single force) client")
      }
      testConductor.enter("test-calculateVCSingleOperator()-2-complete")
    }
  }

  "RizouAlgorithm - selectHostFromCandidates" must {
    "return the candidate Member that has minimal load among the 3 nodes closest to the virtual coords" in {
      val uut = initUUT()
      val clientC = new Coordinates(50, 50, 0)
      val publisher1C = new Coordinates(25, 25, 0)
      val publisher2 = new Coordinates(25, 25, 0)
      val host1 = new Coordinates(50, 0, 0)
      val host2 = new Coordinates(0, 50, 0)
      setCoordinatesForPlacement(uut, clientC, publisher1C, publisher2, host1, host2)

      runOn(client) {
        val op = stream[Int](pNames(0))
        val candidates = uut.memberCoordinates.filter(!_._1.hasRole("Publisher")).toMap
        val vc = new Coordinates(50, 50, 0)
        val result = Await.result(uut.selectHostFromCandidates(vc, candidates, Some(op)), uut.requestTimeout)
        val candidateLoads = result._2
        assert(getMembersWithRole(cluster, "Candidate").contains(result._1),"must place operator on a candidate host")
        assert(!candidateLoads.values.toSeq.sortWith(_ < _).take(uut.k).exists(l => l < candidateLoads(result._1)), "result must be the candidate with minimum load")
      }
    }
    testConductor.enter("test physical placement complete")
  }

  "RizouAlgorithm - calculateVirtualPlacementWithCoords() - one operator" must {
    "return the coordinate with minimal bdp for this single operator" in {
      val uut = initUUT()
      val clientC = new Coordinates(0, 0, 0)
      val publisher1 = new Coordinates(-100, 0, 0)
      val publisher2 = new Coordinates(-100, 0, 0)
      val host1 = new Coordinates(-75, 0, 0)
      val host2 = new Coordinates(-50, 0, 0)
      setCoordinatesForPlacement(uut, clientC, publisher1, publisher2, host1, host2)

      runOn(client) {
        val query = stream[Int](pNames(0))
        implicit val dependencyData = Queries.extractOperatorsAndThroughputEstimates(query)
        val result = uut.calculateVirtualPlacementWithCoords(query, clientC, Map(pNames(0) -> publisher1))
        val deps = QueryDependenciesWithCoordinates(Map(PublisherDummyQuery(pNames(0)) -> publisher1), Map(ClientDummyQuery() -> clientC))
        val resultBDP = calculateBDPSum(query, deps, result(query))
        val approxBDP = findApproximateMinBDPCoord(query, deps)
        assert(math.abs(resultBDP - approxBDP._2) <= bdpAccuracy)
        assert(result(query).distance(new Coordinates(-50, 0, 0)) <= coordAccuracy, "single operator should be placed between client and publisher")
      }
      testConductor.enter("test calculateVirtualPlacementWithCoords() with one operator complete")
    }
  }

  "RizouAlgorithm - calculateVirtualPlacementWithCoords() - binary operator query" must {
    "return the mapping of binary and stream operators to coordinates for the given query that minimizes total bdp" in {
      val uut = initUUT()
      val clientC = new Coordinates(100, 0, 0)
      val publisher1 = new Coordinates(0, 100, 0)
      val publisher2 = new Coordinates(0, -100, 0)
      val host1 = new Coordinates(-75, 0, 0)
      val host2 = new Coordinates(-50, 0, 0)
      setCoordinatesForPlacement(uut, clientC, publisher1, publisher2, host1, host2)

      runOn(client) {
        val s1 = stream[Int](pNames(0))
        val s2 = stream[Int](pNames(1))
        val query = s1.and(s2)
        implicit val dependencyData = Queries.extractOperatorsAndThroughputEstimates(query)
        val publisherCoords = Map(pNames(0) -> publisher1, pNames(1) -> publisher2)
        val result: Map[Query, Coordinates] = uut.calculateVirtualPlacementWithCoords(query, clientC, publisherCoords)

        val andDeps = uut.buildDependencyOperatorCoordinates(query, dependencyData.mapValues(_._1).toMap, result, publisherCoords, clientC)
        val s1Deps = uut.buildDependencyOperatorCoordinates(s1, dependencyData.mapValues(_._1).toMap, result, publisherCoords, clientC)
        val s2Deps = uut.buildDependencyOperatorCoordinates(s2, dependencyData.mapValues(_._1).toMap, result, publisherCoords, clientC)
        val resultBDPAnd = calculateBDPSum(query, andDeps, result(query))
        val resultBDPS1 = calculateBDPSum(s1, s1Deps, result(s1))
        val resultBDPS2 = calculateBDPSum(s2, s2Deps, result(s2))
        val approxBDPAnd = findApproximateMinBDPCoord(query, andDeps)
        val approxBDPS1 = findApproximateMinBDPCoord(s1, s1Deps)
        val approxBDPS2 = findApproximateMinBDPCoord(s2, s2Deps)
        /*
        println(s" result                       | approx                  | diff")
        println(s" ${resultBDPAnd} ${result(query)} | ${approxBDPAnd._2} ${approxBDPAnd._1} | ${math.abs(resultBDPAnd - approxBDPAnd._2)}")
        println(s" ${resultBDPS1} ${result(s1)} | ${approxBDPS1._2} ${approxBDPS1._1} | ${math.abs(resultBDPS1 - approxBDPS1._2)}")
        println(s" ${resultBDPS2} ${result(s2)} | ${approxBDPS2._2} ${approxBDPS2._1} | ${math.abs(resultBDPS2 - approxBDPS2._2)}")
        */
        assert(math.abs(resultBDPAnd - approxBDPAnd._2) <= bdpAccuracy, "BDP between AND and its dependencies must be close to the approximate minimum BDP")
        assert(math.abs(resultBDPS1 - approxBDPS1._2) <= bdpAccuracy, "BDP between stream 1 and its dependencies must be close to the approximate minimum BDP")
        assert(math.abs(resultBDPS2 - approxBDPS2._2) <= bdpAccuracy, "BDP between stream 2 and its dependencies must be close to the approximate minimum BDP")
        // pub1 (0, 100)
        //  \
        //  s1 (25, 50)
        //    \
        //     and (50, 0) -- client (100, 0)
        //    /
        //   s2 (25, -50)
        //  /
        // pub2 (0, -100)
        println("final coords " + result(query))
        assert(result(query).distance(new Coordinates(55, 0, 0)) <= coordAccuracy)
        assert(result(s1).distance(new Coordinates(28, 50, 0)) <= coordAccuracy)
        assert(result(s2).distance(new Coordinates(28, -50, 0)) <= coordAccuracy)
      }
    }
  }
  testConductor.enter("test calculateVirtualPlacementWithCoords() with binary operator complete")

  "RizouAlgorithm - calculateVirtualPlacementWithCoords() - unary operator query" must {
    "return the mapping of operator to coordinates that has minimal bdp for the given unary operator query" in {
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
        val publisherCoords = Map(pNames(0) -> publisher1, pNames(1) -> publisher2)
        implicit val dependencyData = Queries.extractOperatorsAndThroughputEstimates(f2)

        val result = uut.calculateVirtualPlacementWithCoords(f2, clientC, Map(pNames(0) -> publisher1))
        // since this query contains only unary operators, their optimal coordinates are not unique (operator data rate is constant) -> only test for min bdp

        val f2Deps = uut.buildDependencyOperatorCoordinates(f2, dependencyData.mapValues(_._1).toMap, result, publisherCoords, clientC)
        val f1Deps = uut.buildDependencyOperatorCoordinates(f1, dependencyData.mapValues(_._1).toMap, result, publisherCoords, clientC)
        val sDeps = uut.buildDependencyOperatorCoordinates(s, dependencyData.mapValues(_._1).toMap, result, publisherCoords, clientC)
        val resultBDPf2 = calculateBDPSum(f2, f2Deps, result(f2))
        val resultBDPf1 = calculateBDPSum(f1, f1Deps, result(f1))
        val resultBDPs = calculateBDPSum(s, sDeps, result(s))
        val approxBDPf2 = findApproximateMinBDPCoord(f2, f2Deps)
        val approxBDPf1 = findApproximateMinBDPCoord(f1, f1Deps)
        val approxBDPs = findApproximateMinBDPCoord(s, sDeps)

        assert(math.abs(resultBDPf2 - approxBDPf2._2) <= bdpAccuracy, "BDP between f2 and its dependencies must be close to the approximate minimum BDP")
        assert(math.abs(resultBDPf1 - approxBDPf1._2) <= bdpAccuracy, "BDP between f1 and its dependencies must be close to the approximate minimum BDP")
        assert(math.abs(resultBDPs - approxBDPs._2) <= bdpAccuracy, "BDP between stream  and its dependencies must be close to the approximate minimum BDP")

      }
      testConductor.enter("test calculateVirtualPlacementWithCoords() complete")
    }
  }


  "RizouAlgorithm - virtual and then physical placement" must {
    "return a valid mapping of a all operators to hosts" in {
      val uut = initUUT()
      uut.k = 1
      val clientC = new Coordinates(50, 50, 0)
      val pub1 = new Coordinates(0, 0, 0)
      val pub2 = new Coordinates(10, 10, 0)
      val host1 = new Coordinates(25, 25, 0)
      val host2 = new Coordinates(37, 37, 0)
      setCoordinatesForPlacement(uut, clientC, pub1, pub2, host1, host2)

      runOn(client) {
        val s = stream[Int](pNames(0))
        val f1 = Filter1[Int](s, _ => true, Set())
        val f2 = Filter1[Int](f1, _ => true, Set())
        val candidates = uut.findPossibleNodesToDeploy(cluster).map(c => c -> uut.getCoordinatesOfNodeBlocking(c)).toMap
        implicit val dependencyData = Queries.extractOperatorsAndThroughputEstimates(f2)
        val dataRateEstimates = Queries.estimateOutputBandwidths(f2)
        val virtualCoordinates: Map[Query, Coordinates] = Await.result(uut.getVirtualOperatorPlacementCoords(f2, publishers), uut.requestTimeout)
        val physicalPlacements: Map[Query, Member] = virtualCoordinates.map(o => o._1 -> Await.result(uut.findHost(o._2, candidates, o._1, Map(), dataRateEstimates), uut.requestTimeout).member)
        assert(physicalPlacements.values.toSet.map((m: Member) => m.address).subsetOf(
          getMembersWithRole(cluster, "Candidate").map(c => c.address)),
          "all operators must be hosted on candidates")

        val candidateCoordinates = Await.result(uut.getCoordinatesOfMembers(uut.findPossibleNodesToDeploy(cluster)), uut.requestTimeout)
        //println(candidateCoordinates.mkString("\n"))
        //println("\n" + physicalPlacements.map(e => e._1 -> s"${e._2} ${e._2.getRoles} @ ${candidateCoordinates(e._2)}").mkString("\n"))

        assert(physicalPlacements(s).address == node(publisher2).address, "stream operator should be on publisher2")
        assert(physicalPlacements(f1).address == node(node1).address, "filter1 operator should be on host1")
        assert(physicalPlacements(f2).address == node(node2).address, "filter2 operator should be on host2")
      }
      testConductor.enter("test initialVirtualOperatorPlacement and then physical placement complete")
    }
  }

  "RizouAlgorithm - BDP test" must {
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
        val candidates = uut.findPossibleNodesToDeploy(cluster).map(c => c -> uut.getCoordinatesOfNodeBlocking(c)).toMap
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
        implicit val dependencyData = Queries.extractOperatorsAndThroughputEstimates(f1)
        val dataRateEstimates = Queries.estimateOutputBandwidths(f1)
        val virtualCoords: Map[Query, Coordinates] = uut.calculateVirtualPlacementWithCoords(f1, clientC, Map(pNames(0) -> publisher1))
        val physicalPlacement: Map[Query, (Member, Coordinates)] = virtualCoords.map(vc => vc._1 -> Await.result(
          uut.findHost(vc._2, candidates, vc._1, Map(), dataRateEstimates), uut.requestTimeout))
              .map(e => e._1 -> (e._2.member, uut.getCoordinatesOfNodeBlocking(e._2.member)))
        val placementBDP = getTestBDP(physicalPlacement, clientC, publisher1)
        assert(placementBDP == minBDP, "BDP of placement must be minimum possible BDP of all possible placements")
      }

      testConductor.enter("test minimal BDP complete")
    }
  }

  def calculateBDPSum(operator: Query, dependencies: QueryDependenciesWithCoordinates, p: Coordinates)
                      (implicit dependencyData: mutable.LinkedHashMap[Query, (QueryDependencies, EventRateEstimate, EventSizeEstimate, EventBandwidthEstimate)]): Double = {
    val parentBDP = dependencies.parents.map(dep => dep._2.distance(p) * dependencyData(dep._1)._4 * 0.001)
    val childBDP = dependencies.child.map(dep => dep._2.distance(p) * dependencyData(operator)._4 * 0.001)
    parentBDP.sum + childBDP.sum
  }

  /**
    * approximates the coordinates for which the sum of BDPs to the dependencies becomes minimal by sampling the coordinate space between the dependencies
    * @param dependencies dependency coordinates
    * @param dependencyData estimates of operator output bandwidth
    * @param samplesPerDimension number of samples to make per dimension
    * @return tuple of minimal coordinate and distance sum
    */
  def findApproximateMinBDPCoord(operator: Query,
                                 dependencies: QueryDependenciesWithCoordinates,
                                 samplesPerDimension: Int = 100)
                                (implicit dependencyData: mutable.LinkedHashMap[Query, (QueryDependencies, EventRateEstimate, EventSizeEstimate, EventBandwidthEstimate)]): (Coordinates, Double) = {
    val dependencyCoordinates = dependencies.parents.values ++ dependencies.child.values
    if(samplesPerDimension > 1000) println("more than 1000 samples per dimensions, this can take a while...")
    //if(dependencyCoordinates.size < 3) println("number of dependencies is smaller than 3, note that there can be no unique solution for the coordinate in this case")
    val startTime = System.currentTimeMillis()

    val minX = dependencyCoordinates.minBy(_.x).x
    val minY = dependencyCoordinates.minBy(_.y).y
    val maxX = dependencyCoordinates.maxBy(_.x).x
    val maxY = dependencyCoordinates.maxBy(_.y).y
    val stepSizeX: Double = if(maxX != minX) math.abs(maxX - minX) / samplesPerDimension else 1.0
    val stepSizeY: Double = if(maxY != minY) math.abs(maxY - minY) / samplesPerDimension else 1.0
    val allCoords = (minX to maxX by stepSizeX).flatMap(x => (minY to maxY by stepSizeY).map(y => new Coordinates(x, y, 0)))
    val allDists = allCoords.map(c => (c, calculateBDPSum(operator, dependencies, c)))
    val minDist = allDists.minBy(_._2)
    //println(s"calculation took ${System.currentTimeMillis() - startTime}ms, stepSizes: ($stepSizeX, $stepSizeY)")
    minDist
  }
}
