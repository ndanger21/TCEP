package tcep.prediction

import akka.actor.{Actor, ActorLogging, ActorRef, Address, RootActorPath}
import akka.cluster.Cluster
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.pattern.{ask, pipe}
import akka.stream.Materializer
import akka.util.{ByteString, Timeout}
import breeze.stats.meanAndVariance
import breeze.stats.meanAndVariance.MeanAndVariance
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.ConfigFactory
import scalaj.http.HttpStatusException
import tcep.data.Queries
import tcep.data.Queries._
import tcep.graph.qos.OperatorQosMonitor.{GetOperatorQoSMetrics, OperatorQoSMetrics, Sample, getFeatureValue}
import tcep.graph.transition.mapek.DynamicCFMNames
import tcep.machinenodes.qos.BrokerQoSMonitor.BandwidthUnit.BytePerSec
import tcep.machinenodes.qos.BrokerQoSMonitor.{Bandwidth, BrokerQosMetrics, GetBrokerMetrics, IOMetrics}
import tcep.prediction.PredictionHelper.{EndToEndLatency, MetricPredictions, Throughput}
import tcep.prediction.QueryPerformancePredictor.{GetPredictionForPlacement, PredictionResponse}
import tcep.utils.TCEPUtils

import java.io.StringWriter
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class QueryPerformancePredictor(cluster: Cluster) extends Actor with ActorLogging {
  implicit val askTimeout: Timeout = Timeout(FiniteDuration(ConfigFactory.load().getInt("constants.default-request-timeout"), TimeUnit.SECONDS))
  implicit val blockingIoDispatcher: ExecutionContext = cluster.system.dispatchers.lookup("blocking-io-dispatcher")
  val jsonMapper = new ObjectMapper()
  jsonMapper.registerModule(DefaultScalaModule)
  val predictionEndPointAddress: Uri = ConfigFactory.load().getString("constants.prediction-endpoint")
  val samplingInterval: FiniteDuration = FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.MILLISECONDS)

  override def receive: Receive = {
    case GetPredictionForPlacement(rootOperator, currentPlacement, newPlacement, publisherEventRates, contextFeatureSample) =>
      val queryDependencyMap = Queries.extractOperatorsAndThroughputEstimates(rootOperator)(publisherEventRates)
      val publishers = TCEPUtils.getPublisherHosts(cluster)

      //println(s"publisherEventRate map:\n${publisherEventRates.mkString("\n")}\n")
      //println(s"queryDependencyMap: \n${queryDependencyMap.mkString("\n")}\n")
      // ===== helper functions =====
      def getBrokerSamples(broker: Address): Future[BrokerQosMetrics] = {
        val withoutPrevInstances: Option[Set[ActorRef]] = currentPlacement.map(_.values.filter(_.path.address == broker).toSet)
        log.debug("retrieving brokerSamples from {} without {}", broker, withoutPrevInstances)
        cluster.system.actorSelection(RootActorPath(broker) / "user" / "TaskManager*" / "BrokerQosMonitor*")
          .resolveOne() // broker actorRef
          .flatMap(monitor => (monitor ? GetBrokerMetrics(withoutPrevInstances)).mapTo[BrokerQosMetrics])
      }

      def getOperatorSamples(operator: (Query, Option[ActorRef])): Future[OperatorQoSMetrics] = {
        val parents = queryDependencyMap(operator._1)._1.parents.get
      // transition placement, can fetch metrics and predictions from deployed operators
        if (operator._2.isDefined)
          cluster.system.actorSelection(operator._2.get.path.child("operatorQosMonitor"))
            .resolveOne() // operator actorRef
            .flatMap(monitor => (monitor ? GetOperatorQoSMetrics).mapTo[OperatorQoSMetrics])

        //initial placement, no measured metrics for operators yet -> use default estimates
        else for {
          newParentCoords <- Future.traverse(parents
            .map(p => p -> TCEPUtils.getCoordinatesOfNode(cluster, newPlacement.getOrElse(p,
              publishers(p.asInstanceOf[PublisherDummyQuery].p).address)))
          )(e => e._2.map(e._1 -> _))
          newOpCoord <- TCEPUtils.getCoordinatesOfNode(cluster, newPlacement(operator._1))
        } yield {
          val vivDistToParents = newParentCoords.map(_._2.distance(newOpCoord))
          val parentEventRates = parents.map(p => p -> queryDependencyMap(p)._2.getEventsPerSec).toMap
          val parentEventBandwidth = parentEventRates.map(p => p._2 * queryDependencyMap(p._1)._3)
          val outgoingEventRate = queryDependencyMap(operator._1)._2.getEventsPerSec
          val defaultIOMetrics = IOMetrics(
            incomingEventRate = parentEventRates.values.sum,
            outgoingEventRate = outgoingEventRate,
            incomingBandwidth = Bandwidth(parentEventBandwidth.sum, BytePerSec),
            outgoingBandwidth = Bandwidth(outgoingEventRate * queryDependencyMap(operator._1)._3, BytePerSec)
          )
          OperatorQoSMetrics(
            eventSizeIn = parents.map(queryDependencyMap(_)._3),
            eventSizeOut = queryDependencyMap(operator._1)._3,
            interArrivalLatency = meanAndVariance(List(1 / parentEventRates.values.sum)),
            processingLatency = meanAndVariance(List(1.0)), //TODO how reasonably to estimate this? create local operator instance, send some events and use avg?
            networkToParentLatency = meanAndVariance(vivDistToParents),
            endToEndLatency = MeanAndVariance(-1, 0, 0),
            ioMetrics = defaultIOMetrics
          )
        }
      }
      // ==== helper functions end ====

      log.debug("received prediction request")
      //TODO initial placement prediction: include parent event rate prediction in child's operator samples default values
      //DONE broker metrics: include placement information to update number of operators, cumul eventrate (or bandwidth), cpu load (how?)
      //DONE get current publisher event rate from publishers; should do same during transition (currently passing initial event rate around among successors)
      val samples: Future[Map[Query, Sample]] = if(contextFeatureSample.isDefined) Future(contextFeatureSample.get)
      else {
        val placementMap: Map[Query, (Address, Option[ActorRef])] = newPlacement
          .map(e => e._1 -> (e._2, if (currentPlacement.isDefined) currentPlacement.get.get(e._1) else None))
        log.debug("placement map is {}", placementMap)
        Future.traverse(placementMap)(query => {
          for {
            brokerSamples: BrokerQosMetrics <- getBrokerSamples(query._2._1)
            operatorSamples: OperatorQoSMetrics <- getOperatorSamples(query._1, query._2._2)
          } yield {
            log.debug("samples for {} are {} and {}", query._1, brokerSamples, operatorSamples)
            query._1 -> (operatorSamples, brokerSamples)
          }
      }).map(_.toMap) }

      val perQueryPredictions: Future[MetricPredictions] = samples flatMap  { samples =>
        getPerOperatorPredictions(rootOperator, samples, publisherEventRates)
      } flatMap  { perOperatorPredictions =>
        Future(combinePerOperatorPredictions(rootOperator, perOperatorPredictions))
      }
      pipe(perQueryPredictions) to sender()

    case m => log.error(s"received unknown message $m")
  }


  /**
    * traverses the query tree from publishers towards root and predicts metrics for each operator. Predicted Throughput of parent serves as input event rate of child
    * @param rootOperator root operator of the query
    * @param operatorSampleMap current values of context features
    * @param publisherEventRates base event rates of all publishers
    * @return metric predictions for each operator (currently throughput and end-to-end latency)
    */
  def getPerOperatorPredictions(rootOperator: Query, operatorSampleMap: Map[Query, Sample], publisherEventRates: Map[String, Throughput]): Future[Map[Query, MetricPredictions]] = {
      // start predictions at publishers; use parent throughput predictions as input event rate for child operators
    def getPerOperatorPredictionsRec(curOp: Query): Future[Map[Query, MetricPredictions]] = {
      curOp match {
        case b: BinaryQuery => for {
          parentPredictions <- getPerOperatorPredictionsRec(b.sq1).zip(getPerOperatorPredictionsRec(b.sq2)).map(f => f._1 ++ f._2)
          predictions <- predictMetrics(operatorSampleMap(b), parentPredictions(b.sq1).THROUGHPUT, parentPredictions(b.sq2).THROUGHPUT)
        } yield parentPredictions.updated(b, predictions)

        case u: UnaryQuery => for {
          parentPredictions <- getPerOperatorPredictionsRec(u.sq)
          predictions <- predictMetrics(operatorSampleMap(u), parentPredictions(u.sq).THROUGHPUT)
        } yield parentPredictions.updated(u, predictions)

        case s: StreamQuery => predictMetrics(operatorSampleMap(s), publisherEventRates(s.publisherName)).map(f => Map(s -> f))
        case s: SequenceQuery => predictMetrics(operatorSampleMap(s), publisherEventRates(s.s1.publisherName), publisherEventRates(s.s2.publisherName)).map(f => Map(s -> f))
        case _ => throw new IllegalArgumentException(s"unknown operator type $curOp")
      }
    }
    getPerOperatorPredictionsRec(rootOperator)
  }


  def predictMetrics(sample: Sample, parentEventRates: Throughput*): Future[MetricPredictions] = {
    //TODO eval: update incomingEventrate with parent event rate predictions
    val sampleMap = DynamicCFMNames.ALL_FEATURES.map(f => f -> getFeatureValue(sample, f)).toMap ++ DynamicCFMNames.ALL_TARGET_METRICS.map(m => m -> 0)
    val out = new StringWriter()
    jsonMapper.writeValue(out, sampleMap)
    val sampleJson = out.toString
    Http(context.system).singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = predictionEndPointAddress,
        entity = HttpEntity(ContentTypes.`application/json`, sampleJson)
      )
    ) flatMap  {
        case response@HttpResponse(StatusCodes.OK, headers, entity, protocol) =>
          implicit val mat: Materializer = Materializer(context)
          entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(bytes => bytes.toArray)
            .map(bytes => {
              val predictions = jsonMapper.readValue(bytes, classOf[PredictionResponse])
              val predictedLatency = EndToEndLatency(FiniteDuration(predictions.latency.toLong, TimeUnit.MILLISECONDS))
              val predictedThroughput = Throughput(predictions.throughput, samplingInterval)
              log.debug("predictions are {}", predictions)
              MetricPredictions(predictedLatency, predictedThroughput)
            })
        case response@HttpResponse(code, _, _, _) =>
          response.discardEntityBytes(context.system)
          throw HttpStatusException(code.intValue(), "", "bad prediction server response")
      }
  }

  /**
    * combine per-operator predictions according to query graph structure into per-query prediction for each metric
    * @param rootOperator root operator of query
    * @param predictionsPerOperator map of per-operator predictions
    */
  def combinePerOperatorPredictions(rootOperator: Query, predictionsPerOperator: Map[Query, MetricPredictions]): MetricPredictions = {
    def combinePerOperatorPredictionsRec(curOp: Query): MetricPredictions = {
      curOp match {
        case b: BinaryQuery => predictionsPerOperator(b) + MetricPredictions(
          // take critical path (highest predicted latency of two parents)
          E2E_LATENCY = List(
            combinePerOperatorPredictionsRec(b.sq1).E2E_LATENCY,
            combinePerOperatorPredictionsRec(b.sq2).E2E_LATENCY).maxBy(_.amount),
          // throughput is always current operator's output (since parent's predicted output rates are input for current operator)
          THROUGHPUT = predictionsPerOperator(curOp).THROUGHPUT
        )
        case u: UnaryQuery => predictionsPerOperator(u) + combinePerOperatorPredictionsRec(u.sq)
        case s: LeafQuery => predictionsPerOperator(s)
        case _ => throw new IllegalArgumentException(s"unknown operator type $curOp")
      }
    }

    combinePerOperatorPredictionsRec(rootOperator)
  }
}

object QueryPerformancePredictor {
  case class GetPredictionForPlacement(query: Query, currentPlacement: Option[Map[Query, ActorRef]], newPlacement: Map[Query, Address], baseEventRates: Map[String, Throughput], sample: Option[Map[Query, Sample]])
  case class QosPredictions()
  private case class PlacementBrokerMonitors(brokerMonitors: List[ActorRef], predictionRequester: ActorRef)
  case class PredictionResponse(latency: Double, throughput: Double)
}
