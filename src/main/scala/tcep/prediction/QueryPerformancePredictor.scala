package tcep.prediction

import akka.actor.{Actor, ActorLogging, ActorRef, Address, RootActorPath}
import akka.cluster.Cluster
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import breeze.stats.meanAndVariance
import breeze.stats.meanAndVariance.MeanAndVariance
import com.typesafe.config.ConfigFactory
import tcep.data.Queries
import tcep.data.Queries._
import tcep.graph.qos.OperatorQosMonitor.{GetOperatorQoSMetrics, OperatorQoSMetrics}
import tcep.machinenodes.qos.BrokerQoSMonitor.BandwidthUnit.BytePerSec
import tcep.machinenodes.qos.BrokerQoSMonitor.{Bandwidth, BrokerQosMetrics, GetBrokerMetrics, IOMetrics}
import tcep.prediction.PredictionHelper.{EndToEndLatency, MetricPredictions, Throughput}
import tcep.prediction.QueryPerformancePredictor.GetPredictionForPlacement
import tcep.utils.TCEPUtils

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

class QueryPerformancePredictor(cluster: Cluster) extends Actor with ActorLogging {
  implicit val askTimeout: Timeout = Timeout(FiniteDuration(ConfigFactory.load().getInt("constants.default-request-timeout"), TimeUnit.SECONDS))
  implicit val blockingIoDispatcher: ExecutionContext = cluster.system.dispatchers.lookup("blocking-io-dispatcher")


  override def receive: Receive = {
    case GetPredictionForPlacement(rootOperator, currentPlacement, newPlacement, publisherEventRates) =>
      val queryDependencyMap = Queries.extractOperators(rootOperator)(publisherEventRates)
      val publishers = TCEPUtils.getPublisherHosts(cluster)

      println(s"publisherEventRate map:\n${publisherEventRates.mkString("\n")}\n")
      println(s"queryDependencyMap: \n${queryDependencyMap.mkString("\n")}\n")
      // ===== helper functions =====
      def getBrokerSamples(broker: Address): Future[BrokerQosMetrics] = {
        val withoutPrevInstances: Option[Set[ActorRef]] = currentPlacement.map(_.values.filter(_.path.address == broker).toSet)
        println(s"retrieving brokerSamples from $broker without $withoutPrevInstances")
        cluster.system.actorSelection(RootActorPath(broker) / "user" / "TaskManager*" / "BrokerQosMonitor*")
          .resolveOne() // broker actorRef
          .flatMap(monitor => (monitor ? GetBrokerMetrics(withoutPrevInstances)).mapTo[BrokerQosMetrics])
      }

      def getOperatorSamples(operator: (Query, Option[ActorRef])): Future[Option[OperatorQoSMetrics]] = {
        val parents = queryDependencyMap(operator._1)._1.parents.get
      // transition placement, can fetch metrics and predictions from deployed operators
        if (operator._2.isDefined)
          cluster.system.actorSelection(operator._2.get.path.child("operatorQosMonitor"))
            .resolveOne() // operator actorRef
            .flatMap(monitor => (monitor ? GetOperatorQoSMetrics).mapTo[OperatorQoSMetrics])
            .map(Some(_))

        //TODO insert default estimates here
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
          Some(
          OperatorQoSMetrics(
            eventSizeIn = parents.map(queryDependencyMap(_)._3),
            eventSizeOut = queryDependencyMap(operator._1)._3,
            selectivity = defaultIOMetrics.outgoingEventRate / defaultIOMetrics.incomingEventRate,
            interArrivalLatency = meanAndVariance(List(1 / parentEventRates.values.sum)),
            processingLatency = meanAndVariance(List(1.0)), //TODO how reasonably to estimate this?
            networkToParentLatency = meanAndVariance(vivDistToParents),
            endToEndLatency = MeanAndVariance(-1, 0, 0),
            ioMetrics = defaultIOMetrics
          ))
        }
      }
      // ==== helper functions end ====

      println("received prediction request")
      val placementMap: Map[Query, (Address, Option[ActorRef])] = newPlacement.map(e => e._1 -> (e._2, if (currentPlacement.isDefined) currentPlacement.get.get(e._1) else None))
      println(s"placement map is $placementMap")
      //TODO better to include sampling data retrieval in recursive prediction, so we can include parent throughput predictions in child samples for initial placement
      //TODO broker metrics: include placement information to update number of operators, cumul eventrate (or bandwidth), cpu load (how?)
      //TODO get current publisher event rate from publishers; should do same during transition (currently passing initial event rate around among successors)
      //TODO use throughput predictions to replace input event rate estimates if possible?
      val perQueryPrediction: Future[MetricPredictions] = Future.traverse(placementMap)(query => {
        for {
          brokerSamples: BrokerQosMetrics <- getBrokerSamples(query._2._1)
          _ = println(s"brokerSamples $brokerSamples")
          operatorSamples: Option[OperatorQoSMetrics] <- getOperatorSamples(query._1, query._2._2)
        } yield {
          println(s"samples for ${query._1} are $brokerSamples and $operatorSamples")
          query._1 -> (brokerSamples, operatorSamples)
        }
      }) map { samples =>
        getPerOperatorPredictions(rootOperator, samples.toMap, publisherEventRates)
      } map { perOperatorPredictions =>
        println(s"per-operator predictions are \n ${perOperatorPredictions.mkString("\n")}")
        combinePerOperatorPredictions(rootOperator, perOperatorPredictions) }
      pipe(perQueryPrediction) to sender()

    case m => log.error(s"received unknown message $m")
  }


  /**
    * traverses the query tree from publishers towards root and predicts metrics for each operator. Predicted Throughput of parent serves as input event rate of child
    * @param rootOperator root operator of the query
    * @param operatorSampleMap current values of context features
    * @param publisherEventRates base event rates of all publishers
    * @return metric predictions for each operator (currently throughput and end-to-end latency)
    */
  def getPerOperatorPredictions(rootOperator: Query, operatorSampleMap: Map[Query, (BrokerQosMetrics, Option[OperatorQoSMetrics])], publisherEventRates: Map[String, Throughput]): Map[Query, MetricPredictions] = {
      // start predictions at publishers; use parent throughput predictions as input event rate for child operators
    def getPerOperatorPredictionsRec(curOp: Query): Map[Query, MetricPredictions] = {
      curOp match {
        case b: BinaryQuery =>
          val parentPredictions = getPerOperatorPredictionsRec(b.sq1) ++ getPerOperatorPredictionsRec(b.sq2)
          val latencyPredictions = predictLatency(operatorSampleMap(b), parentPredictions(b.sq1).THROUGHPUT, parentPredictions(b.sq2).THROUGHPUT)
          val throughputPredictions = predictThroughput(operatorSampleMap(b), parentPredictions(b.sq1).THROUGHPUT, parentPredictions(b.sq2).THROUGHPUT)
          parentPredictions.updated(b, MetricPredictions(latencyPredictions, throughputPredictions))

        case u: UnaryQuery =>
          val parentPredictions = getPerOperatorPredictionsRec(u.sq)
          val latencyPredictions = predictLatency(operatorSampleMap(u), parentPredictions(u.sq).THROUGHPUT)
          val throughputPredictions = predictThroughput(operatorSampleMap(u), parentPredictions(u.sq).THROUGHPUT)
          parentPredictions.updated(u, MetricPredictions(latencyPredictions, throughputPredictions))

        case s: StreamQuery => Map(s -> MetricPredictions(
          E2E_LATENCY = predictLatency(operatorSampleMap(s), publisherEventRates(s.publisherName)),
          THROUGHPUT = predictThroughput(operatorSampleMap(s), publisherEventRates(s.publisherName))
        ))
        case s: SequenceQuery => Map(s -> MetricPredictions(
          E2E_LATENCY = predictLatency(operatorSampleMap(s), publisherEventRates(s.s1.publisherName), publisherEventRates(s.s2.publisherName)),
          THROUGHPUT = predictThroughput(operatorSampleMap(s), publisherEventRates(s.s1.publisherName), publisherEventRates(s.s2.publisherName))
        ))
        case _ => throw new IllegalArgumentException(s"unknown operator type $curOp")
      }
    }
    getPerOperatorPredictionsRec(rootOperator)
  }

  //TODO implement prediction models
  def predictLatency(samples: (BrokerQosMetrics, Option[OperatorQoSMetrics]), parentEventRates: Throughput*): EndToEndLatency = EndToEndLatency(1 millisecond)
  def predictThroughput(samples: (BrokerQosMetrics, Option[OperatorQoSMetrics]), parentEventRates: Throughput*): Throughput = Throughput(1, 1 second)

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
  case class GetPredictionForPlacement(query: Query, currentPlacement: Option[Map[Query, ActorRef]], newPlacement: Map[Query, Address], baseEventRates: Map[String, Throughput])
  case class QosPredictions()
  private case class PlacementBrokerMonitors(brokerMonitors: List[ActorRef], predictionRequester: ActorRef)
}
