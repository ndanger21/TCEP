package tcep.prediction

import akka.actor.{Actor, ActorLogging, ActorRef, Timers}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import tcep.graph.qos.OperatorQosMonitor.{GetOperatorQoSMetrics, OperatorQoSMetrics}
import tcep.machinenodes.qos.BrokerQoSMonitor.{BrokerQosMetrics, GetBrokerMetrics}
import tcep.prediction.OperatorPerformancePredictor._
import tcep.prediction.PredictionHelper.{EndToEndLatency, MetricPredictions, Throughput, Timestamp}

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

class OperatorPerformancePredictor(operator: Option[ActorRef], brokerMonitor: ActorRef, defaultOperatorQosMetrics: Option[OperatorQoSMetrics] = None) extends Actor with ActorLogging with Timers {

  assert(operator.isDefined && defaultOperatorQosMetrics.isEmpty || operator.isEmpty && defaultOperatorQosMetrics.isDefined,
    s"faulty constructor arguments for $self: $operator $defaultOperatorQosMetrics - only one may be defined (default OperatorQosMetrics for initial deployment")

  implicit val ec: ExecutionContext = context.dispatcher
  implicit val askTimeout: Timeout = Timeout(FiniteDuration(ConfigFactory.load().getInt("constants.default-request-timeout"), TimeUnit.SECONDS))
  val samplingInterval: FiniteDuration = FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.MILLISECONDS)
  val predictionLagCount: Int = ConfigFactory.load().getInt("constants.prediction-lag-count")
  val predictionHorizon: PredictionHorizon = PredictionHorizon()
  var prevOpMetrics: ListBuffer[OperatorQoSMetrics] = ListBuffer.empty
  var prevBrokerMetrics: ListBuffer[BrokerQosMetrics] = ListBuffer.empty
  var predictions: Vector[(Timestamp, MetricPredictions)] = Vector()

  override def preStart(): Unit = {
    timers.startTimerWithFixedDelay(GetMetricsTickKey, GetMetricsTick, samplingInterval)
  }

  override def receive: Receive = {
    case GetMetricsTick =>
      if(operator.isDefined) operator.get ! GetOperatorQoSMetrics
      // initial deployment; no operator metrics available yet, need to use default estimates provided by constructor
      else self ! defaultOperatorQosMetrics
      brokerMonitor ! GetBrokerMetrics

    case opMetrics: OperatorQoSMetrics =>
      prevOpMetrics.+=:(opMetrics)
      if(prevOpMetrics.size > predictionLagCount) prevOpMetrics.trimEnd(prevOpMetrics.size - predictionLagCount)
      checkPredictionReady()

    case brokerMetrics: BrokerQosMetrics =>
      prevBrokerMetrics.+=:(brokerMetrics)
      if(prevOpMetrics.size > predictionLagCount) prevBrokerMetrics.trimEnd(prevBrokerMetrics.size - predictionLagCount)
      checkPredictionReady()

    case PredictionTick =>
      val predictionTimestamps = predictionHorizon.getTimestamps(System.currentTimeMillis())
      predictions = predictMetrics(prevOpMetrics.zip(prevBrokerMetrics).toVector, predictionTimestamps)
      log.info(s"updated predictions for next ${predictionHorizon.length}ms to \n${predictions.mkString("\n")}")

    case GetOperatorPrediction => sender() ! OperatorPerformancePrediction(predictions)

    case msg => log.warning(s"received unhandled msg $msg")
  }

  def checkPredictionReady(): Unit = {
    if(predictionLagCount == prevOpMetrics.size && prevOpMetrics.size == prevBrokerMetrics.size)
      self ! PredictionTick
  }
  //TODO what to do about missing operator metrics during initial placement? -> defaults provided in constructor (by QueryGraph?)
  def predictMetrics(previousTimestampMetrics: Vector[(OperatorQoSMetrics, BrokerQosMetrics)],
                     remainingTimestamps: Vector[Timestamp],
                     predictedTimestamps: Vector[(Timestamp, MetricPredictions)] = Vector()
                    ): Vector[(Timestamp, MetricPredictions)] = {

    val currentFeatureTSPrediction = featureTimeSeriesPrediction()
    val currentPrediction = MetricPredictions(
      predictLatency(),
      predictThroughput()
    )
    val updatedMetrics = previousTimestampMetrics.+:(currentFeatureTSPrediction).take(predictionLagCount)
    predictMetrics(updatedMetrics, remainingTimestamps.tail, predictedTimestamps.:+((remainingTimestamps.head), currentPrediction))
  }

  def featureTimeSeriesPrediction() = ???
  def predictLatency(): EndToEndLatency = ??? //TODO
  def predictThroughput(): Throughput = ??? //TODO

}

object OperatorPerformancePredictor {
  private case object GetMetricsTickKey
  private case object GetMetricsTick
  private case object PredictionTick
  case object GetOperatorPrediction
  case class OperatorPerformancePrediction(prediction: Vector[(Timestamp, MetricPredictions)])
  case class PredictionHorizon(
                                length: Int = ConfigFactory.load().getInt("constants.predictionHorizonLength"),
                                steps: Int = ConfigFactory.load().getInt("constants.predictionHorizonStepCount")) {
    val stepLength: Double = length.toDouble / steps
    def getTimestamps(t_now: Long): Vector[Long] = {
      0 to steps map (i => t_now + i * stepLength) map(_.toLong) toVector
    }
  }
}

object PredictionHelper {
  type Timestamp = Long

  abstract class PerformanceMetric() {
    def metricHeader: String
  }

  case class EndToEndLatency(amount: FiniteDuration) extends PerformanceMetric {
    override def toString: String = f"${amount.toUnit(MILLISECONDS)}%2.3f"
    def +(that: EndToEndLatency): EndToEndLatency = EndToEndLatency(this.amount + that.amount)
    def metricHeader: String = "end-to-end latency [ms]"
  }

  case class Throughput(amount: Int, interval: FiniteDuration) extends PerformanceMetric {
    override def toString: String = f"${amount}"
    def metricHeader = s"throughput [per ${interval}]"
  }

  case class MetricPredictions(E2E_LATENCY: EndToEndLatency, THROUGHPUT: Throughput) {
    def +(parentPrediction: MetricPredictions): MetricPredictions = {
      MetricPredictions(
        E2E_LATENCY + parentPrediction.E2E_LATENCY,
        THROUGHPUT // always keep the child output rate since we want the output at the root operator
      )
    }
  }

}
