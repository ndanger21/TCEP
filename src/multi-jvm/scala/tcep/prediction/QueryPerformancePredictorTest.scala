package tcep.prediction

import akka.actor.Props
import akka.pattern.ask
import akka.testkit.TestActorRef
import akka.util.Timeout
import tcep.data.Queries.Query2
import tcep.dsl.Dsl.{query1ToQuery1Helper, query2ToQuery2Helper, stream}
import tcep.prediction.PredictionHelper.{EndToEndLatency, MetricPredictions, Throughput}
import tcep.prediction.QueryPerformancePredictor.GetPredictionForPlacement
import tcep.{MultiJVMTestSetup, TCEPMultiNodeConfig}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}


class QueryPerformancePredictorMultiJvmClient extends QueryPerformancePredictorTest
class QueryPerformancePredictorMultiJvmPublisher1 extends QueryPerformancePredictorTest
class QueryPerformancePredictorMultiJvmPublisher2 extends QueryPerformancePredictorTest

abstract class QueryPerformancePredictorTest extends MultiJVMTestSetup(3) {

  import TCEPMultiNodeConfig._

  "A QueryPerformancePredictor" should {
    val streamA = stream[Int](pNames(0))
    val streamB = stream[Float](pNames(1))
    val conjunction: Query2[Int, Float] = streamA.and(streamB)
    val filter: Query2[Int, Float] = conjunction.where((_, _) => true)
    val baseEventRates = Map(pNames(0) -> Throughput(5, 1 second), pNames(1) -> Throughput(8, 1 second))

    "correctly combine per-operator end-to-end latency and throughput predictions into a per-query prediction" in {
      runOn(client) {
        val perOperatorPredictions = Map(
          streamA -> MetricPredictions(EndToEndLatency(5 milliseconds), Throughput(5, 1 second)),
          streamB -> MetricPredictions(EndToEndLatency(1 seconds), Throughput(5, 1 second)),
          conjunction -> MetricPredictions(EndToEndLatency(8 milliseconds), Throughput(10, 1 second)),
          filter -> MetricPredictions(EndToEndLatency(5 milliseconds), Throughput(10, 1 second))
        )
        val queryPredictor: TestActorRef[QueryPerformancePredictor] = TestActorRef.create(system, Props(classOf[QueryPerformancePredictor], cluster))
        val perQueryPredictions = queryPredictor.underlyingActor.combinePerOperatorPredictions(filter, perOperatorPredictions)
        assert(perQueryPredictions.E2E_LATENCY == EndToEndLatency(1013 milliseconds), "predicted end-to-end latency must equal longest path among all parents")
        assert(perQueryPredictions.THROUGHPUT == Throughput(10, 1 second), "query throughput must equal root operator throughput")
      }
      testConductor.enter("test passed")
    }

    "reply with a valid prediction for an initial placement (no previous operator instances of the query are running)" in within(5 seconds) {
      runOn(client) {
        val initialPlacement = Map(
          streamA -> node(publisher1).address,
          streamB -> node(publisher2).address,
          conjunction -> node(client).address,
          filter -> node(client).address
        )
        val queryPerformancePredictor = system.actorOf(Props(classOf[QueryPerformancePredictor], cluster))
        implicit val timeout: Timeout = Timeout(remaining)
        val f = (queryPerformancePredictor ? GetPredictionForPlacement(filter, None, initialPlacement, baseEventRates)).mapTo[MetricPredictions]
        f.onComplete {
          case Failure(exception) => exception.printStackTrace()
          case Success(value) => value
        }
        val res = Await.result(f, remaining)
        //queryPerformancePredictor ! GetPredictionForPlacement(filter, None, initialPlacement, baseEventRates)
        //val res = expectMsgClass(classOf[MetricPredictions])
        assert(res.E2E_LATENCY == EndToEndLatency(3 milliseconds))
        assert(res.THROUGHPUT == Throughput(1, 1 second))
        println(s"initial placement per-query prediction: $res")
      }
      testConductor.enter("test passed")
    }

    "reply with a valid prediction for a transition placement (previous operator instances are running and monitors have gathered metrics)" in {

    }
  }
}
