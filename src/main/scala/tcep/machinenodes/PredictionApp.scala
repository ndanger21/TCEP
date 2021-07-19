package tcep.machinenodes

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import breeze.stats.meanAndVariance.MeanAndVariance
import com.typesafe.config.ConfigFactory
import tcep.data.Queries.{ClientDummyQuery, PublisherDummyQuery, Query}
import tcep.graph.qos.OperatorQosMonitor.{OperatorQoSMetrics, Sample}
import tcep.machinenodes.qos.BrokerQoSMonitor.{BrokerQosMetrics, IOMetrics}
import tcep.placement.MobilityTolerantAlgorithm
import tcep.prediction.PredictionHelper.Throughput
import tcep.prediction.PredictionHttpClient

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

object PredictionApp extends App {

  val actorSystem: ActorSystem = ActorSystem("predictorTest")
  //implicit val blockingIoDispatcher: ExecutionContext = actorSystem.dispatchers.lookup("blocking-io-dispatcher")
  val samplingInterval: FiniteDuration = FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.MILLISECONDS)

  val predictionEndpoint = actorSystem.actorOf(Props(new Actor with ActorLogging with PredictionHttpClient {
    implicit val ec = actorSystem.dispatcher

    override def receive: Receive = {
      case sample: Sample =>
        println("received sample")
        getMetricPredictions(ClientDummyQuery(), sample, MobilityTolerantAlgorithm.name).pipeTo(sender())

      case samples: Map[Query, Sample] =>
        println("received samples")
        //updateBatchOnlineModel(testSamples, MobilityTolerantAlgorithm.name)
        getBatchMetricPredictions(samples, "Relaxation").pipeTo(sender())

      case msg => log.error(s"unhandled message $msg")
    }
  }), "predictionEndpoint")



  val testSample = (OperatorQoSMetrics(
    eventSizeIn = List(1), eventSizeOut = 1,
    interArrivalLatency = MeanAndVariance(1, 1, 1),
    processingLatency = MeanAndVariance(1, 1, 1),
    networkToParentLatency = MeanAndVariance(1, 1, 1),
    endToEndLatency = MeanAndVariance(1, 1, 1),
    ioMetrics = IOMetrics(incomingEventRate = Throughput(1, samplingInterval), outgoingEventRate = Throughput(1, samplingInterval))
  ), BrokerQosMetrics(cpuLoad = 1, cpuThreadCount = 1, deployedOperators = 1, IOMetrics = IOMetrics()))

  val testSamples: Map[Query, (OperatorQoSMetrics, BrokerQosMetrics)] = (1 to 20).toList.map(i => PublisherDummyQuery(s"$i", Set()) -> testSample).toMap
  //val testSamples = Map(, PublisherDummyQuery("B", Set()) -> testSample)
  implicit val timeout = Timeout(5, TimeUnit.SECONDS)

  implicit val ec = ExecutionContext.global
  val f = (predictionEndpoint ? testSamples)
  f.onComplete {
    case Failure(exception) => println("failed"); exception.printStackTrace()
    case Success(value) => println(s"result is $value")
  }

  Await.result(f, timeout.duration)
  actorSystem.terminate()
}
