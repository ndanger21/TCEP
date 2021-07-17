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
import tcep.graph.qos.OperatorQosMonitor._
import tcep.graph.transition.mapek.DynamicCFMNames.{EVENTRATE_OUT, NEW_TARGET_METRICS, PROCESSING_LATENCY_MEAN_MS}
import tcep.machinenodes.qos.BrokerQoSMonitor.BandwidthUnit.BytePerSec
import tcep.machinenodes.qos.BrokerQoSMonitor.{Bandwidth, BrokerQosMetrics, GetBrokerMetrics, IOMetrics}
import tcep.prediction.PredictionHelper._
import tcep.prediction.QueryPerformancePredictor.GetPredictionForPlacement
import tcep.utils.{SpecialStats, TCEPUtils}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class QueryPerformancePredictor(cluster: Cluster) extends PredictionHttpClient {
  implicit val askTimeout: Timeout = Timeout(FiniteDuration(ConfigFactory.load().getInt("constants.default-request-timeout"), TimeUnit.SECONDS))
  lazy val predictionDispatcher: ExecutionContext = context.system.dispatchers.lookup("prediction-dispatcher")
  //val singleThreadDispatcher: ExecutionContext = scala.concurrent.ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  //var leadingOnlineModels: Map[String, Option[String]] = ALL_TARGET_METRICS.map(_ -> None).toMap // make this per-operator?
  val useOnlinePredictions: Boolean = ConfigFactory.load().getBoolean("constants.mapek.exchangeable-model.use-online-model-weighting")
  val minOnlineSamples: Int = ConfigFactory.load().getInt("constants.mapek.exchangeable-model.online-model-warmup-phase")


  override def receive: Receive = {
    case pr@GetPredictionForPlacement(rootOperator, currentPlacement, newPlacement, publisherEventRates, lastContextFeatureSamplesAndPredictions, newPlacementAlgorithmStr) =>
      log.debug("received query performance prediction request for placement {} {}\n{}", pr.newPlacementAlgorithmStr, newPlacement.size, newPlacement.toList.map(e => e._1.getClass.getSimpleName -> e._2).mkString("\n"))
      val sampleSizes = lastContextFeatureSamplesAndPredictions.getOrElse(Map()).map(e => e._1 -> e._2._1.size)
      log.debug("last samples ({}) sizes: \n{}", sampleSizes.size, sampleSizes.toList.map(e => e._1.getClass.getSimpleName -> e._2).mkString("\n"))
      implicit val ec = predictionDispatcher
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

      //DONE broker metrics: include placement information to update number of operators, cumul eventrate (or bandwidth), cpu load (how?)
      //DONE get current publisher event rate from publishers; should do same during transition (currently passing initial event rate around among successors)
      val currentSample: Future[Map[Query, Sample]] = if(lastContextFeatureSamplesAndPredictions.isDefined) Future(lastContextFeatureSamplesAndPredictions.get.map(s => s._1 -> s._2._1.head))
      else { // no samples provided
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

      val perQueryPredictions: Future[EndToEndLatencyAndThroughputPrediction] = currentSample flatMap ( s => {
        val currentOperatorPred: Future[Map[Query, OfflineAndOnlinePredictions]] =
          if(lastContextFeatureSamplesAndPredictions.isDefined) Future(lastContextFeatureSamplesAndPredictions.get.map(s => s._1 -> s._2._2.head)) // use prediction previously obtained by monitor
          else {
            log.warning("no sample predictions supplied, retrieving predictions from endpoint {}", predictionEndPointAddress)
            getPerOperatorPredictions(rootOperator, s, publisherEventRates, newPlacementAlgorithmStr) // avoid running prediction requests in parallel since online models are updated with them as well
          }
        currentOperatorPred.onComplete {
          case Success(value) => log.debug("per-operator predictions are (count: {} of {} \n{}", value.size, Queries.getOperators(rootOperator).size, value.mkString("\n"))
          case Failure(exception) => log.error(exception, s"failed to retrieve predictions from endpoint $predictionEndPointAddress")
        }
        currentOperatorPred
      }) flatMap { currentOperatorPredictions =>
        log.debug("currentPredictions are {}", currentOperatorPredictions.size)

        val usedOperatorPredictions = currentOperatorPredictions.map(p => p._1 -> {
          if(lastContextFeatureSamplesAndPredictions.isDefined && sampleSizes.minBy(_._2)._2 >= minOnlineSamples) {
            val opLastNSamples = lastContextFeatureSamplesAndPredictions.get(p._1)._1.take(minOnlineSamples)
            val opLastNPred = lastContextFeatureSamplesAndPredictions.get(p._1)._2.take(minOnlineSamples)
            val weightedPredictions = weightedAveragePrediction(opLastNSamples, opLastNPred, p._2) // use weighted average of offline + current best online model; weight determined by prediction error over last N samples
            if(useOnlinePredictions) weightedPredictions
            else p._2.offline
          } else p._2.offline
        })

          val publisherDummys: Map[Query, Address] = queryDependencyMap.filterKeys(_.isInstanceOf[PublisherDummyQuery])
            .map(p => {
              try {
              p._1 -> publishers.find(_._1 == p._1.asInstanceOf[PublisherDummyQuery].p.split("-").head).get._2.address
              } catch {
                case e: Throwable =>
                    log.error(e, "failed to find publisher {} among \n{}", p._1.asInstanceOf[PublisherDummyQuery].toString(), publishers.mkString("\n"))
                    throw e
              }}).toMap

        for {
          nodeCoords <- TCEPUtils.makeMapFuture((publisherDummys ++ newPlacement).map(n => n._1 -> TCEPUtils.getCoordinatesOfNode(cluster, n._2)))
        } yield {
          log.debug("node coords are {}\n{}", nodeCoords.size, nodeCoords.toList.map(e => e._1.getClass.getSimpleName -> e._2).mkString("\n"))
          log.debug("querydependencies are {}\n{}", queryDependencyMap.size, queryDependencyMap.toList.map(e => e._1.getClass.getSimpleName -> e._2._1.parents.getOrElse(List()).size))

          val networkLatencyToParents: Map[Query, FiniteDuration] = newPlacement.map(op => op._1 -> {
            try {
              val vivDistancesToParents = queryDependencyMap(op._1)
                ._1.parents.get.map(p => FiniteDuration(nodeCoords(p)
                .distance(nodeCoords(op._1)).toLong, TimeUnit.MILLISECONDS)) // check if this works
              //log.info(s"viv distances to parents of $op are $vivDistancesToParents")
              vivDistancesToParents.max // use the largest network latency to parent since we want the critical path
            } catch {
              case e: Throwable =>
                log.error(s" parent: ${queryDependencyMap(op._1)._1.parents.map(_.map(p => p.getClass.getSimpleName + " | " + p.id))}")
                log.error(e, s"current op was ${op._1.getClass.getSimpleName}\n ${op._1.id}\n, networkCoords was (${nodeCoords.size}) \n ${nodeCoords.keys.map(e => (if(e.isInstanceOf[PublisherDummyQuery]) e.toString() else e.getClass.getSimpleName) + " | " + e.id).mkString("\n")} \n contains: ${nodeCoords.get(op._1)}")
                throw e
            }
          })
          val operatorSelectivityMap = lastContextFeatureSamplesAndPredictions.get.map(op => op._1 -> op._2._1.head._1.selectivity)
          val combinedPrediction = combinePerOperatorPredictions(rootOperator, usedOperatorPredictions, networkLatencyToParents, operatorSelectivityMap)
          log.debug("per-query prediction is {}", combinedPrediction)
          combinedPrediction
        }
      }
      perQueryPredictions.onComplete {
        case Failure(exception) => log.error(exception, s"failed to calculate per-query prediction for $newPlacementAlgorithmStr")
        case Success(value) => log.debug("per-query performance prediction for {} is {}", newPlacementAlgorithmStr, value)
      }
      pipe(perQueryPredictions) to sender()

    case m => log.error(s"received unknown message $m")
  }


  /**
    * traverses the query tree from publishers towards root and predicts metrics for each operator.
    * @param rootOperator root operator of the query
    * @param operatorSampleMap current values of context features
    * @param publisherEventRates base event rates of all publishers
    * @return metric predictions for each operator (currently throughput and end-to-end latency)
    */
  def getPerOperatorPredictions(rootOperator: Query, operatorSampleMap: Map[Query, Sample], publisherEventRates: Map[String, Throughput], placementStr: String): Future[Map[Query, OfflineAndOnlinePredictions]] = {
    implicit val ec = predictionDispatcher

    // start predictions at publishers
    def getPerOperatorPredictionsRec(curOp: Query): Future[Map[Query, OfflineAndOnlinePredictions]] = {
      curOp match {
        case b: BinaryQuery => for {
          parentPredictions <- getPerOperatorPredictionsRec(b.sq1).zip(getPerOperatorPredictionsRec(b.sq2)).map(f => f._1 ++ f._2)
          predictions <- getMetricPredictions(curOp, operatorSampleMap(b), placementStr)
        } yield parentPredictions.updated(b, predictions)

        case u: UnaryQuery => for {
          parentPredictions <- getPerOperatorPredictionsRec(u.sq)
          predictions <- getMetricPredictions(curOp, operatorSampleMap(u), placementStr)
        } yield parentPredictions.updated(u, predictions)

        case s: StreamQuery => getMetricPredictions(curOp, operatorSampleMap(s), placementStr).map(f => Map(s -> f))
        case s: SequenceQuery => getMetricPredictions(curOp, operatorSampleMap(s), placementStr).map(f => Map(s -> f))
        case _ => throw new IllegalArgumentException(s"unknown operator type $curOp")
      }
    }

    getPerOperatorPredictionsRec(rootOperator)
  }




  /**
    * combine per-operator predictions according to query graph structure into per-query prediction for each metric
    * combines processing latency prediction and parent network latency into end-to-end latency prediction
    * @param rootOperator root operator of query
    * @param predictionsPerOperator map of per-operator predictions: processing latency and throughput
    * @return prediction of end-to-end latency and throughput of the entire query
    */
  def combinePerOperatorPredictions(rootOperator: Query, predictionsPerOperator: Map[Query, MetricPredictions], networkLatencyToParents: Map[Query, FiniteDuration], operatorSelectivity: Map[Query, Double]): EndToEndLatencyAndThroughputPrediction = {
    def combinePerOperatorPredictionsRec(curOp: Query): EndToEndLatencyAndThroughputPrediction = {
      curOp match {
        case b: BinaryQuery =>
          val parent1CombinedPredictions = combinePerOperatorPredictionsRec(b.sq1)
          val parent2CombinedPredictions = combinePerOperatorPredictionsRec(b.sq2)
          implicit val selectivity: Double = operatorSelectivity(b)
          EndToEndLatencyAndThroughputPrediction(
            EndToEndLatency(predictionsPerOperator(b).processingLatency.amount + networkLatencyToParents(b)),
            predictionsPerOperator(b).throughput
          ) + EndToEndLatencyAndThroughputPrediction(
            // take critical path (highest predicted latency of two parents)
            endToEndLatency = List(parent1CombinedPredictions.endToEndLatency, parent2CombinedPredictions.endToEndLatency).maxBy(_.amount),
            // this is the eventRateIn (total incoming events) the current operator will see
            throughput = parent1CombinedPredictions.throughput + parent2CombinedPredictions.throughput
        )
          // this operators throughput depends on how + is defined (see EndToEndLatencyAndThroughputPrediction)

        case u: UnaryQuery =>
          try {
            implicit val selectivity: Double = operatorSelectivity(u)
            EndToEndLatencyAndThroughputPrediction(
              EndToEndLatency(predictionsPerOperator(u).processingLatency.amount
              + networkLatencyToParents(u)),
              predictionsPerOperator(u).throughput
            ) + combinePerOperatorPredictionsRec(u.sq)
          } catch {
            case e: Throwable =>
              log.error(s"networkLatencyToParents contains op: ${networkLatencyToParents.contains(u)}")
              log.error(e, s"current op was ${u.getClass.getSimpleName}\n ${u.id}\n, predictionsPerOperator was (${predictionsPerOperator.size}) \n ${predictionsPerOperator.keys.map(e => e.getClass.getSimpleName + " | " + e.id).mkString("\n")} \n contains: ${predictionsPerOperator.get(u)}")
              throw e
          }
        case s: LeafQuery => EndToEndLatencyAndThroughputPrediction(
          EndToEndLatency(predictionsPerOperator(s).processingLatency.amount + networkLatencyToParents(s)),
          predictionsPerOperator(s).throughput)
        case _ => throw new IllegalArgumentException(s"unknown operator type $curOp")
      }
    }

    combinePerOperatorPredictionsRec(rootOperator)
  }

  /**
    * combine online and offline predictions of an operator according to prediction error of each model on last n samples
    * @param lastNSamples
    * @param lastNPredictions
    * @param currentPrediction
    * @return
    */
  def weightedAveragePrediction(lastNSamples: Samples, lastNPredictions: List[OfflineAndOnlinePredictions], currentPrediction: OfflineAndOnlinePredictions): MetricPredictions = {
    val start = System.nanoTime()
    log.debug("lastNSamples {} lastNPredictions {} minOnlineSamples {}", lastNSamples.size, lastNPredictions.size, minOnlineSamples)
    assert(lastNSamples.size == lastNPredictions.size && lastNSamples.size == minOnlineSamples,
      s"can calculate weighted average only with at least $minOnlineSamples samples and predictions, currently: ${lastNSamples.size} and ${lastNPredictions.size}")
    val metricPredictions = NEW_TARGET_METRICS.map(m => m -> {
      val lastMetricTruths = lastNSamples.map(s => m match {
        case EVENTRATE_OUT => getTargetMetricValue(s, m)
        case PROCESSING_LATENCY_MEAN_MS => getTargetMetricValue(s, m) * 1e6 // ms -> ns
        case _ => throw new IllegalArgumentException(s"unknown metric $m")
      })
      val allOfflinePredictions: List[Double] = (currentPrediction ::lastNPredictions).map(e => m match {
        case EVENTRATE_OUT => e.offline.throughput.amount
        case PROCESSING_LATENCY_MEAN_MS => e.offline.processingLatency.amount.toNanos.toDouble
      })
      val onlinePredictions: List[Map[String, Double]] = (currentPrediction :: lastNPredictions).map(e => m match {
        case EVENTRATE_OUT => e.onlineThroughput.map(model => model._1 -> model._2.amount)
        case PROCESSING_LATENCY_MEAN_MS => e.onlineLatency.map(model => model._1 -> model._2.amount.toNanos.toDouble)
      })

      val onlineRAEPerModel: Option[List[(String, Double)]] = onlinePredictions.headOption
        .map(e => e.keys
          .map(model => model -> relativeAbsoluteError(lastMetricTruths, onlinePredictions.map(f => f(model)).tail))
          .toList)
      val bestOnlineModel = if(onlineRAEPerModel.isDefined) {
        val sorted = onlineRAEPerModel.get.sortBy(_._2) // get model with smallest RAE
        Some(sorted.head)
        //leadingOnlineModels = leadingOnlineModels.updated(m, Some(bestOnlineModel.))
      } else None
      /*
      val allOnlinePredictions = (currentPrediction :: lastNPredictions).map(e => m match {
        case EVENTRATE_OUT => e.onlineThroughput(leadingOnlineModelThroughput.getOrElse(e.onlineThroughput.head._1)).amount
        case PROCESSING_LATENCY_MEAN_MS => e.onlineLatency(leadingOnlineModelLatency.getOrElse(e.onlineLatency.head._1)).amount.toNanos
      })*/
      // no best online model because not enough samples -> use only offline model
      val offlineRAE = relativeAbsoluteError(lastMetricTruths, allOfflinePredictions.tail) // without current prediction since there is no truth for it yet

      val prediction = if(bestOnlineModel.isDefined && bestOnlineModel.get._2 >= 0 && bestOnlineModel.get._2 != Double.PositiveInfinity && onlinePredictions.head(bestOnlineModel.get._1) >= 0.0d) {
        // unnormalized RAE: weight = (1 - RAE) / (2 - RAE - RAE_other)
        // both have minimum error (0) -> each has weight 0.5
        // online error is ~0.5, offline ~0.1 -> weightOffline = 0.64, weightOnline = 0.36
        // error > 1 would lead to negative weight, and make model with larger error get more weight -> use normlized RAE instead
        // with normalizedRAE: onlineRAE 0.5, offlineRAE 0.1 -> weightOffline = 0.833 weightOnline = 0.1666
        val onlineRAE = bestOnlineModel.get._2
        val normalizedOfflineRAE = offlineRAE / (offlineRAE + onlineRAE)
        val normalizedOnlineRAE = onlineRAE / (offlineRAE + onlineRAE)

        val weightOffline = 1 - normalizedOfflineRAE
        val weightOnline = 1 - normalizedOnlineRAE
        //assert(weightOnline + weightOnline - 1 <= 1e-3, s"weights must add up to 1, but are $weightOnline, $weightOffline")
        val weightedPred = weightOffline * allOfflinePredictions.head + weightOnline * onlinePredictions.head(bestOnlineModel.get._1)

        SpecialStats.log(this.toString, "weightedAveragePredictions", s"${m};truth;${lastMetricTruths.head};weightedPred;${weightedPred};" +
          s"offline;${allOfflinePredictions.head};online;${onlinePredictions.head(bestOnlineModel.get._1)};weight_offline_online;${weightOffline};${weightOnline};RAE_offline_online;${offlineRAE};${onlineRAE};online_model_name;${bestOnlineModel.get._1.replace("\n", "|")};calculation took ${(System.nanoTime() - start) / 1e6}ms")

        weightedPred
      } else {
        allOfflinePredictions.head
      }
      prediction
    }).toMap

    MetricPredictions(
      processingLatency = ProcessingLatency(FiniteDuration(metricPredictions(PROCESSING_LATENCY_MEAN_MS).toLong, TimeUnit.NANOSECONDS)),
      throughput = Throughput(metricPredictions(EVENTRATE_OUT), samplingInterval))
  }

  def relativeAbsoluteError(truths: List[Double], predictions: List[Double]): Double = {
    assert(truths.size == predictions.size, "must have equal number of truths and predictions")
    val pairs = truths.zip(predictions)
    val truthMean = truths.sum / truths.size
    val modelResiduals = pairs.map(e => math.abs(e._1 - e._2)).sum
    val naiveResiduals = truths.map(t => math.abs(t - truthMean)).sum
    modelResiduals / naiveResiduals
  }
}

object QueryPerformancePredictor {
  case class GetPredictionForPlacement(query: Query, currentPlacement: Option[Map[Query, ActorRef]], newPlacement: Map[Query, Address],
                                       baseEventRates: Map[String, Throughput], lastSamplesAndPredictions: Option[Map[Query, (Samples, List[OfflineAndOnlinePredictions])]], newPlacementAlgorithmStr: String)
  case class QosPredictions()
  private case class PlacementBrokerMonitors(brokerMonitors: List[ActorRef], predictionRequester: ActorRef)
  //case class PredictionResponse(latency: Double, throughput: Double)
  // offline = Map(latency -> 0.0, throughput -> 0.0)
  // online = Map(latency -> Map(algo1 -> 0.0, algo2 -> ...), throughput -> Map(algo1 -> ...))
  case class PredictionResponse(offline: Map[String, Double], online: Map[String, Map[String, Double]])
  // offline: Map(latency -> Map(op1 -> 0.0, op2 ->), throughput -> Map(...))
  // online: metric -> algo -> ops -> values
  case class BatchPredictionResponse(offline: Map[String, Map[String, Double]], online: Map[String, Map[String, Map[String, Double]]])
}
