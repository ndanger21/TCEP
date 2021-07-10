package tcep

import akka.actor.{ActorSystem, Address}
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.Coordinates
import org.scalatest.WordSpecLike
import tcep.data.Queries
import tcep.data.Queries._
import tcep.dsl.Dsl._
import tcep.placement.manets.StarksAlgorithm
import tcep.prediction.PredictionHelper.Throughput
import tcep.simulation.tcep.{MobilityData, SectionDensity}

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits
import scala.concurrent.duration.FiniteDuration

class MDCEPTest extends TestKit(ActorSystem("testSystem", ConfigFactory.parseString("akka.test.single-expect-default = 5000").withFallback(ConfigFactory.load()))) with WordSpecLike {
  implicit val ec: ExecutionContext = Implicits.global
  val eventIntervalMicros: Long = 500e3.toLong // 500ms

  "MDCEP" must {
    val clientC = new Coordinates(100, 0, 0)
    val pub1 = new Coordinates(0, 100, 0)
    val pub2 = new Coordinates(0, -100, 0)
    val host1 = new Coordinates(75, 0, 0)
    val host2 = new Coordinates(50, 0, 0)
    // pub1 (0, 100)
    //   \
    //    \
    //     host2 (50, 0) -- host1 (75, 0) -- client (100, 0)
    //    /
    //   /
    // pub2 (0, -100)
    val clientAddr = Address("", "", "client", 0)
    val pub1Addr = Address("", "", "pub1", 0)
    val pub2Addr = Address("", "", "pub2", 0)
    val host2Addr = Address("", "", "host2", 0)
    val publishers = Map("A" -> pub1Addr, "B" -> pub2Addr)
    val memberCoords = Map(
      clientAddr -> clientC,
      pub1Addr -> pub1,
      pub2Addr -> pub2,
      Address("", "", "host1", 0) -> host1,
      host2Addr -> host2
    )
    "return a valid placement upon calling initialVirtualPlacement" in within(FiniteDuration(5, TimeUnit.SECONDS)) {
      val s1 = Stream1[Int]("A", Set())
      val s2 = Stream1[Int]("B", Set())
      val s3 = Stream1[Int]("A", Set())
      val s4 = Stream1[Int]("B", Set())
      val and1 = Conjunction11[Int, Int](s1, s2, Set())
      val and2 = Conjunction11[Int, Int](s3, s4, Set())
      val and3 = Conjunction22[Int, Int, Int, Int](and1, and2, Set())
      val f = Filter4[Int, Int, Int, Int](and3, _ => true, Set())

      implicit val baseEventRates = publishers.keys.map(_ -> Throughput(1, FiniteDuration(eventIntervalMicros, TimeUnit.MICROSECONDS))).toMap
      implicit val dependencyMap = Queries.extractOperatorsAndThroughputEstimates(f)
      val uut = StarksAlgorithm
      val placement = uut.traverseNetworkTree(clientAddr, f, List())(memberCoords, publishers)
      assert(placement.nonEmpty)
      assert(Queries.getOperators(f).forall(placement.contains), "placement must contain host for each operator")
      assert(placement(s1) == pub1Addr, "stream operator should be on its own publisher")
      assert(placement(s2) == pub2Addr, "stream operator should be on its own publisher")
      assert(placement(and1) == host2Addr, "binary operator should be on closest possible non-parent host")
      assert(placement(and2) == host2Addr, "binary operator should be on closest possible non-parent host")
      assert(placement(and3) == host2Addr, "binary operator should be on same host as both parents if they are on the same host")
      assert(placement(f) == host2Addr, "unary root operator should be on same host as parent binary operator")

    }

    "place deeper binary queries successfully" in {
      def speedStreams(nSpeedStreamOperators: Int): Vector[Stream1[MobilityData]] = {
        (0 until nSpeedStreamOperators).map(i => Stream1[MobilityData]("A", Set())).toVector
      }
      def singlePublisherNodeDensityStream(section: Int): Stream1[SectionDensity] = {
        val dp = Stream1[SectionDensity]("B", Set())
        dp
      }
      // accident detection query: low speed, high density in section A, high speed, low density in following road section B
      def accidentQuery(requirements: Requirement*) = {

        val sections = 0 to 1 map(section => { // careful when increasing number of subtrees (21 operators per tree) -> too many operators make transitions with MDCEP unreliable
          val avgSpeed = averageSpeedPerSectionQuery(section)
          val density = singlePublisherNodeDensityStream(section)
          //val and = avgSpeed.join(density, slidingWindow(1 instances), slidingWindow(1 instances))
          val and = avgSpeed.and(density)
          val filter = and.where(
            (_: MobilityData, _: SectionDensity) => true /*(sA: Double, dA: Double) => sA <= 60 && dA >= 10*/ // replace with always true to keep events (and measurements) coming in
          )
          filter
        })
        val and01 = sections(0).and(sections(1), requirements: _*)
        // and12 = sections(1).and(sections(2), requirements: _*)
        and01
      }

      // average speed per section (average operator uses only values matching section)
      def averageSpeedPerSectionQuery(section: Int, requirements: Requirement*) = {
        val nSpeedStreamOperators = 8
        val streams = speedStreams(nSpeedStreamOperators)
        val and01 = streams(0).and(streams(1))
        val and23 = streams(2).and(streams(3))
        val and0123 = and01.and(and23)
        val averageA = Average4(and0123, sectionFilter = Some(section))

        val and67 = streams(4).and(streams(5))
        val and89 = streams(6).and(streams(7))
        val and6789 = and67.and(and89)
        val averageB = Average4(and6789, sectionFilter = Some(section))

        val andAB = averageA.and(averageB)
        val averageAB = Average2(andAB, Set(requirements: _*), sectionFilter = Some(section))
        averageAB
      }

      val uut = StarksAlgorithm
      val accident = accidentQuery()
      implicit val baseEventRates = publishers.keys.map(_ -> Throughput(1, FiniteDuration(eventIntervalMicros, TimeUnit.MICROSECONDS))).toMap
      implicit val dependencyMap = Queries.extractOperatorsAndThroughputEstimates(accident)
      val placement = uut.traverseNetworkTree(clientAddr, accident, List())(memberCoords, publishers)

      assert(placement.nonEmpty)
      assert(Queries.getOperators(accident).forall(placement.contains), "placement must contain host for each operator")
      //assert(placement() == pub1Addr, "stream operator should be on its own publisher")
    }
  }
}
