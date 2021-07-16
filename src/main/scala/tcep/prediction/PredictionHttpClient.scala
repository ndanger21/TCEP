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
import tcep.graph.transition.mapek.DynamicCFMNames._
import tcep.prediction.PredictionHelper.{MetricPredictions, OfflineAndOnlinePredictions, ProcessingLatency, Throughput}
import tcep.prediction.QueryPerformancePredictor.{BatchPredictionResponse, PredictionResponse}
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
  private type QueryIdentifier = Int

  def getSampleAsJsonStr(sample: Sample, placementStr: String): String = {
    val sampleMap = algorithmNames.map {
      case name if name == placementStr => name -> 1
      case n => n -> 0
    } ++ ALL_FEATURES.map(f => f -> getFeatureValue(sample, f)).toMap ++
      ALL_TARGET_METRICS.map(m => m -> getTargetMetricValue(sample, m))
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

  def updateBatchOnlineModel(lastSamples: Map[Query, Sample], placementStr: String)(implicit executionContext: ExecutionContext): Unit = {
    val start = System.nanoTime()
    val queryIDMap: Map[QueryIdentifier, Query] = (0 until(lastSamples.size)).toList.zip(lastSamples).map(e => e._1 -> e._2._1).toMap
    val inverseQueryIdMap: Map[Query, QueryIdentifier] = queryIDMap.map(e => e._2 -> e._1)

    val mapOfJsons: Map[QueryIdentifier, String] = lastSamples.map(s => inverseQueryIdMap(s._1) -> getSampleAsJsonStr(s._2, placementStr))
    val out = new StringWriter()
    jsonMapper.writeValue(out, mapOfJsons)
    val jsonMap = out.toString
    sendRequestToPredictionEndpoint(predictionEndPointAddress.withPath(Path("/updateOnlineBatch")), jsonMap) map {
      case response@HttpResponse(StatusCodes.Accepted, _, _, _) =>
        response.discardEntityBytes(context.system)
        log.info("onlinemodel update took {}ms", (System.nanoTime() - start) / 1e6)
      case response@HttpResponse(code, _, _, _) =>
        response.discardEntityBytes(context.system)
        log.error("failed to update online model with sample, response: {}", response)
        throw HttpStatusException(code.intValue(), "", "bad prediction server response")
    }
  }

  def getBatchMetricPredictions(lastSamples: Map[Query, Sample], placementStr: String)(implicit ec: ExecutionContext): Future[Map[Query, OfflineAndOnlinePredictions]] = {
    val start = System.nanoTime()
    type QueryIdentifier = Int

    val queryIDMap: Map[QueryIdentifier, Query] = (0 until(lastSamples.size)).toList.zip(lastSamples).map(e => e._1 -> e._2._1).toMap
    val inverseQueryIdMap: Map[Query, QueryIdentifier] = queryIDMap.map(e => e._2 -> e._1)

    val mapOfJsons: Map[QueryIdentifier, String] = lastSamples.map(s => inverseQueryIdMap(s._1) -> getSampleAsJsonStr(s._2, placementStr))
    val out = new StringWriter()
    jsonMapper.writeValue(out, mapOfJsons)
    val jsonMap = out.toString
    val request = sendRequestToPredictionEndpoint(predictionEndPointAddress.withPath(Path("/predictBatch")), jsonMap)
    request.onComplete {
      case Failure(exception) => log.error(exception, "failed to complete batch prediction request")
      case _ =>
    }
    request flatMap {
      case response@HttpResponse(StatusCodes.OK, headers, entity, protocol) =>
        implicit val mat: Materializer = Materializer(context)
        entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(bytes => bytes.toArray)(ec)
          .map(bytes => {
            val predictions = jsonMapper.readValue(bytes, classOf[BatchPredictionResponse])
            //log.info(s"received batch response: \n $predictions")
            // offline: Map(latency -> Map(op1 -> 0.0, op2 ->), throughput -> Map(...))
            // online: metric -> algo -> ops -> values
            val offlineThroughput = predictions.offline("throughput")     //.map(e => queryIDMap(e._1) -> Throughput(e._2, samplingInterval))
            val offlineLatency = predictions.offline("latency")           //.map(e => queryIDMap(e._1) -> ProcessingLatency(FiniteDuration((e._2 * 1e6).toLong, TimeUnit.NANOSECONDS)))

            val onlineThroughput = predictions.online(EVENTRATE_OUT) //.map(e => e._1 -> e._2.map(q => queryIDMap(q._1) -> Throughput(q._2, samplingInterval)))
            val onlineLatency = predictions.online(PROCESSING_LATENCY_MEAN_MS) //.map(e => e._1 -> e._2.map(q => queryIDMap(q._1) -> ProcessingLatency(FiniteDuration((q._2 * 1e6).toLong, TimeUnit.NANOSECONDS))))

            //SpecialStats.log(this.toString, "online_predictions_latency", s"latency;truth;${sample._1.processingLatency.mean};predictions;${onlinePredictionsLatency.get.map(e => s"${e._1};${e._2}").mkString(";")}")
            //SpecialStats.log(this.toString, "online_predictions_throughput", s"throughput;truth;${sample._1.ioMetrics.outgoingEventRate};predictions;${onlinePredictionsThroughput.get.map(e => s"${e._1};${e._2}").mkString(";")}")
            val response = inverseQueryIdMap.map(q => q._1 -> {


              val offProcessingLatency = ProcessingLatency(FiniteDuration((offlineLatency(q._2.toString) * 1e6).toLong, TimeUnit.NANOSECONDS))
              val offThroughput = Throughput(offlineThroughput(q._2.toString), samplingInterval)
              val onLatency = onlineLatency.map(m => m._1 -> ProcessingLatency(FiniteDuration((m._2(q._2.toString) * 1e6).toLong, TimeUnit.NANOSECONDS)))
              val onThroughput = onlineThroughput.map(m => m._1 -> Throughput(m._2(q._2.toString), samplingInterval))
              SpecialStats.log(this.getClass.getSimpleName, "offline_predictions_throughput", s";truth;${lastSamples(q._1)._1.ioMetrics.outgoingEventRate.amount};predicted;${offThroughput.amount};for operator; ${q._1}")
              SpecialStats.log(this.getClass.getSimpleName, "offline_predictions_latency", s";truth;${lastSamples(q._1)._1.processingLatency.mean};predicted;${offProcessingLatency.amount.toNanos / 1e6};for operator; ${q._1}")
              SpecialStats.log(this.getClass.getSimpleName, "online_predictions_latency", s"latency;truth;${lastSamples(q._1)._1.processingLatency.mean};predictions;${onLatency.mkString(";").replace("\n", "|")};for operator;${q._1}")
              SpecialStats.log(this.getClass.getSimpleName, "online_predictions_throughput", s"throughput;truth;${lastSamples(q._1)._1.ioMetrics.outgoingEventRate.amount};predictions;${onThroughput.mkString(";").replace("\n", "|")};for operator;${q._1}")
              OfflineAndOnlinePredictions(
                offline = MetricPredictions(offProcessingLatency, offThroughput),
                onLatency, onThroughput
              )
            })
            /*
            BatchOfflineAndOnlinePredictions(
              offline = offlineThroughput.map(e => e._1 -> MetricPredictions(offlineLatency(e._1), e._2)),
              onlineLatency = onlineLatency,
              onlineThroughput = onlineThroughput
            )
             */
            response
          })


      case response@HttpResponse(code, _, _, _) =>
        response.discardEntityBytes(context.system)
        log.error("failed to get batch prediction , response was {}", response)
        throw HttpStatusException(code.intValue(), "", "bad prediction server response")
    }
  }

  def getMetricPredictions(operator: Query, sample: Sample, placementStr: String)(implicit ec: ExecutionContext): Future[OfflineAndOnlinePredictions] = {
    val start = System.nanoTime()
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
            val predictedLatency = ProcessingLatency(FiniteDuration((predictions.offline("latency") * 1e6).toLong, TimeUnit.NANOSECONDS))
            val predictedThroughput = Throughput(predictions.offline("throughput"), samplingInterval)
            SpecialStats.log(this.toString, "offline_predictions", s"latency;truth;${sample._1.processingLatency.mean};predicted;${predictedLatency};throughput;truth;${sample._1.ioMetrics.outgoingEventRate};predicted;$predictedThroughput; for operator $operator")
            val onlinePredictionsLatency = predictions.online.get(PROCESSING_LATENCY_MEAN_MS)
            val onlinePredictionsThroughput = predictions.online.get(EVENTRATE_OUT)
            //SpecialStats.log(this.toString, "predictions_raw", s"${predictions.online}")
            if(onlinePredictionsLatency.isDefined) {
              SpecialStats.log(this.toString, "online_predictions_latency", s"latency;truth;${sample._1.processingLatency.mean};predictions;${onlinePredictionsLatency.get.map(e => s"${e._1};${e._2}").mkString(";")}")
            }
            if(onlinePredictionsThroughput.isDefined) {
              SpecialStats.log(this.toString, "online_predictions_throughput", s"throughput;truth;${sample._1.ioMetrics.outgoingEventRate};predictions;${onlinePredictionsThroughput.get.map(e => s"${e._1};${e._2}").mkString(";")}")
            }
            log.info("prediction request took {}ms", (System.nanoTime() - start) / 1e6)
            OfflineAndOnlinePredictions(
              offline = MetricPredictions(predictedLatency, predictedThroughput),
              onlineLatency = onlinePredictionsLatency.get.map(l => l._1 -> ProcessingLatency(FiniteDuration((l._2 * 1e6).toLong, TimeUnit.NANOSECONDS))),
              onlineThroughput = onlinePredictionsThroughput.get.map(t => t._1 -> Throughput(t._2, samplingInterval))
            )

          })(ec)
      case response@HttpResponse(code, _, _, _) =>
        response.discardEntityBytes(context.system)
        log.error("failed to get prediction for {}, response was {}", operator, response)
        throw HttpStatusException(code.intValue(), "", "bad prediction server response")
    }
  }
}
