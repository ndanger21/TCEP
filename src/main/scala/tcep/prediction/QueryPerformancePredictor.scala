package tcep.prediction

import akka.actor.{ActorRef, Address, RootActorPath}
import akka.cluster.Cluster
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import breeze.stats.meanAndVariance
import breeze.stats.meanAndVariance.MeanAndVariance
import com.typesafe.config.ConfigFactory
import tcep.data.Queries
import tcep.data.Queries._
import tcep.graph.qos.OperatorQosMonitor.{GetOperatorQoSMetrics, OperatorQoSMetrics, Sample}
import tcep.machinenodes.qos.BrokerQoSMonitor.BandwidthUnit.BytePerSec
import tcep.machinenodes.qos.BrokerQoSMonitor.{Bandwidth, BrokerQosMetrics, GetBrokerMetrics, IOMetrics}
import tcep.prediction.PredictionHelper.{MetricPredictions, Throughput}
import tcep.prediction.QueryPerformancePredictor.GetPredictionForPlacement
import tcep.utils.TCEPUtils

import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class QueryPerformancePredictor(cluster: Cluster) extends PredictionHttpClient {
  implicit val askTimeout: Timeout = Timeout(FiniteDuration(ConfigFactory.load().getInt("constants.default-request-timeout"), TimeUnit.SECONDS))
  val blockingIoDispatcher: ExecutionContext = cluster.system.dispatchers.lookup("blocking-io-dispatcher")
  val singleThreadDispatcher: ExecutionContext = scala.concurrent.ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  override def receive: Receive = {
    case GetPredictionForPlacement(rootOperator, currentPlacement, newPlacement, publisherEventRates, contextFeatureSample) =>
      implicit val ec = blockingIoDispatcher
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
          val maxVivDistToParents = newParentCoords.map(_._2.distance(newOpCoord)).max
          val parentEventRates = parents.map(p => p -> queryDependencyMap(p)._2).toMap // per sampling interval
          val parentEventBandwidth = parentEventRates.map(p => p._2.getEventsPerSec * queryDependencyMap(p._1)._3)
          val outgoingEventRate = queryDependencyMap(operator._1)._2
          val defaultIOMetrics = IOMetrics(
            incomingEventRate = Throughput(parentEventRates.values.map(_.amount).sum, samplingInterval),
            outgoingEventRate = outgoingEventRate,
            incomingBandwidth = Bandwidth(parentEventBandwidth.sum, BytePerSec),
            outgoingBandwidth = Bandwidth(outgoingEventRate.getEventsPerSec * queryDependencyMap(operator._1)._3, BytePerSec)
          )
          OperatorQoSMetrics(
            eventSizeIn = parents.map(queryDependencyMap(_)._3),
            eventSizeOut = queryDependencyMap(operator._1)._3,
            interArrivalLatency = meanAndVariance(List(1 / parentEventRates.values.map(_.getEventsPerSec).sum)),
            processingLatency = meanAndVariance(List(1.0)), //TODO how reasonably to estimate this? create local operator instance, send some events and use avg?
            networkToParentLatency = meanAndVariance(List(maxVivDistToParents)),
            endToEndLatency = MeanAndVariance(-1, 0, 0),
            ioMetrics = defaultIOMetrics
          )
        }
      }
      // ==== helper functions end ====

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

      val perQueryPredictions: Future[MetricPredictions] = samples flatMap ( s => {
        log.debug("retrieving predictions from endpoint {}", predictionEndPointAddress)
        val pred = getPerOperatorPredictions(rootOperator, s, publisherEventRates) // avoid running prediction requests in parallel since online models are updated with them as well
        pred.onComplete {
          case Success(value) => log.debug("per-operator predictions are \n{}", value.mkString("\n"))
          case Failure(exception) => log.error(exception, s"failed to retrieve predictions from endpoint $predictionEndPointAddress")
        }
        pred
      }) flatMap  { perOperatorPredictions =>
        Future {
          val combinedPrediction = combinePerOperatorPredictions(rootOperator, perOperatorPredictions)
          log.info("per-query prediction is {}", combinedPrediction)
          combinedPrediction
        }
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
    implicit val ec = singleThreadDispatcher
    //TODO eval: update incomingEventrate with parent event rate predictions

    // start predictions at publishers
    def getPerOperatorPredictionsRec(curOp: Query): Future[Map[Query, MetricPredictions]] = {
      curOp match {
        case b: BinaryQuery => for {
          parentPredictions <- getPerOperatorPredictionsRec(b.sq1).zip(getPerOperatorPredictionsRec(b.sq2)).map(f => f._1 ++ f._2)
          predictions <- getMetricPredictions(curOp, operatorSampleMap(b))
        } yield parentPredictions.updated(b, predictions)

        case u: UnaryQuery => for {
          parentPredictions <- getPerOperatorPredictionsRec(u.sq)
          predictions <- getMetricPredictions(curOp, operatorSampleMap(u))
        } yield parentPredictions.updated(u, predictions)

        case s: StreamQuery => getMetricPredictions(curOp, operatorSampleMap(s)).map(f => Map(s -> f))
        case s: SequenceQuery => getMetricPredictions(curOp, operatorSampleMap(s)).map(f => Map(s -> f))
        case _ => throw new IllegalArgumentException(s"unknown operator type $curOp")
      }
    }

    getPerOperatorPredictionsRec(rootOperator)
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
