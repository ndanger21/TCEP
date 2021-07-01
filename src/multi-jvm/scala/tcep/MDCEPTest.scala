package tcep

import akka.actor.{ActorSystem, Address}
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.Coordinates
import org.scalatest.WordSpecLike
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import tcep.data.Queries
import tcep.data.Queries.{Conjunction11, Filter2, Stream1}
import tcep.placement.manets.StarksAlgorithm
import tcep.prediction.PredictionHelper.Throughput

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits
import scala.concurrent.duration.FiniteDuration

class MDCEPTest extends TestKit(ActorSystem("testSystem", ConfigFactory.parseString("akka.test.single-expect-default = 5000").withFallback(ConfigFactory.load()))) with WordSpecLike {
  implicit val ec: ExecutionContext = Implicits.global
  val eventIntervalMicros: Long = 500e3.toLong // 500ms

  "MDCEP" must {
    "return a valid placement upon calling initialVirtualPlacement" in within(5 seconds) {
      val uut = StarksAlgorithm
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
      val s1 = Stream1[Int]("A", Set())
      val s2 = Stream1[Int]("B", Set())
      val and = Conjunction11[Int, Int](s1, s2, Set())
      val f = Filter2[Int, Int](and, _ => true, Set())
      val clientAddr = Address("", "", "client", 0)
      val pub1Addr = Address("", "", "pub1", 0)
      val pub2Addr = Address("", "", "pub2", 0)
      val host2Addr = Address("", "", "host2", 0)
      val publishers = Map("A" -> pub1Addr, "B" -> pub2Addr)
      implicit val baseEventRates = publishers.keys.map(_ -> Throughput(1, FiniteDuration(eventIntervalMicros, TimeUnit.MICROSECONDS))).toMap
      implicit val dependencyMap = Queries.extractOperatorsAndThroughputEstimates(f)

      val memberCoords = Map(
        clientAddr -> clientC,
        pub1Addr -> pub1,
        pub2Addr -> pub2,
        Address("", "", "host1", 0) -> host1,
        host2Addr -> host2
      )
      val placement = uut.traverseNetworkTree(clientAddr, f)(memberCoords, publishers)
      assert(placement.nonEmpty)
      assert(Queries.getOperators(f).forall(placement.contains), "placement must contain host for each operator")
      assert(placement(s1) == pub1Addr, "stream operator should be on its own publisher")
      assert(placement(s2) == pub2Addr, "stream operator should be on its own publisher")
      assert(placement(and) == host2Addr, "binary operator should be on closest possible non-parent host")
      assert(placement(f) == host2Addr, "unary root operator should be on same host as parent binary operator")

    }
  }
}
