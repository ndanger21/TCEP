package tcep.prediction

import akka.actor.{Actor, ActorLogging}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.stream.Materializer
import akka.util.ByteString
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.ConfigFactory
import scalaj.http.HttpStatusException
import tcep.data.Queries.Query
import tcep.graph.qos.OperatorQosMonitor.{Sample, getFeatureValue, getTargetMetricValue}
import tcep.graph.transition.mapek.DynamicCFMNames
import tcep.prediction.PredictionHelper.{EndToEndLatency, MetricPredictions, Throughput}
import tcep.prediction.QueryPerformancePredictor.PredictionResponse
import tcep.utils.SpecialStats

import java.io.StringWriter
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter
import scala.util.Failure

trait PredictionHttpClient extends Actor with ActorLogging {
  val jsonMapper = new ObjectMapper()
  jsonMapper.registerModule(DefaultScalaModule)
  val predictionEndPointAddress: Uri = ConfigFactory.load().getString("constants.prediction-endpoint")
  val samplingInterval: FiniteDuration = FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.MILLISECONDS)
  val algorithmNames = ConfigFactory.load().getStringList("benchmark.general.algorithms").asScala

  private def getSampleAsJsonStr(sample: Sample, placementStr: String): String = {
    val sampleMap = algorithmNames.map {
      case name if name == placementStr => name -> 1
      case n => n -> 0
    } ++ DynamicCFMNames.ALL_FEATURES.map(f => f -> getFeatureValue(sample, f)).toMap ++
      DynamicCFMNames.ALL_TARGET_METRICS.map(m => m -> getTargetMetricValue(sample, m))
    val out = new StringWriter()
    jsonMapper.writeValue(out, sampleMap)
    val sampleJson = out.toString
    sampleJson
  }

  def sendRequestToPredictionEndpoint(uri: Uri, jsonStr: String): Future[HttpResponse] = {
    Http(context.system).singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = uri,
        entity = HttpEntity(ContentTypes.`application/json`, jsonStr)
      )
    )
  }

  def updateOnlineModel(sample: Sample, placementStr: String)(implicit executionContext: ExecutionContext): Unit = {
    sendRequestToPredictionEndpoint(predictionEndPointAddress.withPath(Path("/updateOnline")), getSampleAsJsonStr(sample, placementStr)) map {
      case response@HttpResponse(StatusCodes.Accepted, _, _, _) =>
        response.discardEntityBytes(context.system)
      case response@HttpResponse(code, _, _, _) =>
        response.discardEntityBytes(context.system)
        log.error("failed to update online model with sample, response: {}", response)
        throw HttpStatusException(code.intValue(), "", "bad prediction server response")
    }
  }

  def getMetricPredictions(operator: Query, sample: Sample, placementStr: String)(implicit ec: ExecutionContext): Future[MetricPredictions] = {
    val request = sendRequestToPredictionEndpoint(predictionEndPointAddress.withPath(Path("/predict")), getSampleAsJsonStr(sample, placementStr))
    request.onComplete {
      case Failure(exception) => log.error(exception, "failed to complete prediction request for {}", operator)
      case _ =>
    }
    request flatMap {
      case response@HttpResponse(StatusCodes.OK, headers, entity, protocol) =>
        implicit val mat: Materializer = Materializer(context)
        entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(bytes => bytes.toArray)(ec)
          .map(bytes => {
            val predictions = jsonMapper.readValue(bytes, classOf[PredictionResponse])
            val predictedLatency = EndToEndLatency(FiniteDuration(predictions.offline("latency").toLong, TimeUnit.MILLISECONDS))
            val predictedThroughput = Throughput(predictions.offline("throughput"), samplingInterval)
            SpecialStats.log(this.toString, "offline_predictions", s"latency;truth;${sample._1.processingLatency.mean};predicted;${predictedLatency};throughput;truth;${sample._1.ioMetrics.outgoingEventRate};predicted;$predictedThroughput; for operator $operator")
            val onlinePredictionsLatency = predictions.online.get("processingLatencyMean")
            val onlinePredictionsThroughput = predictions.online.get("eventRateOut")
            //SpecialStats.log(this.toString, "predictions_raw", s"${predictions.online}")
            if(onlinePredictionsLatency.isDefined) {
              SpecialStats.log(this.toString, "online_predictions_latency", s"latency;truth;${sample._1.processingLatency.mean};predictions;${onlinePredictionsLatency.get.mkString(";")}")
            }
            if(onlinePredictionsThroughput.isDefined) {
              SpecialStats.log(this.toString, "online_predictions_throughput", s"throughput;truth;${sample._1.ioMetrics.outgoingEventRate};predictions;${onlinePredictionsThroughput.get.mkString(";")}")
            }

            MetricPredictions(predictedLatency, predictedThroughput)
          })(ec)
      case response@HttpResponse(code, _, _, _) =>
        response.discardEntityBytes(context.system)
        log.error("failed to get prediction for {}, response was {}", operator, response)
        throw HttpStatusException(code.intValue(), "", "bad prediction server response")
    }
  }
}
