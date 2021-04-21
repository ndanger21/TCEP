package tcep.graph.transition.mapek.learnon

import breeze.linalg._
import org.cardygan.config.Config
import tcep.ClusterActor
import tcep.data.Queries
import tcep.data.Queries.Requirement
import tcep.graph.transition.mapek.contrast.CFM
import tcep.graph.transition.mapek.learnon.LearningModelMessages.{GetMechanism, Receive}

import scala.concurrent.{ExecutionContext, Future}

abstract class LearningModel() extends ClusterActor {
  override implicit val ec: ExecutionContext = blockingIoDispatcher

  /**
    * Abstract class to implement different learning models. Is deployed on the K of MAPEK
    * and provides two interfaces for interaction: Receive and GetMechanism.
    * Receive will be called by K which transmits a new context observation, while GetMechanism is used by
    * P component to get the mechanism to transition to.
    * It also provides general functionality like the OLS and WLS estimations, which can be shared among the different learning models.
    */


  override def receive: Receive = {
    case Receive(cfm, contextConfig, currentAlgorithm, qosRequirements) =>
      sender() ! Future {
        this.receive(cfm, contextConfig, currentAlgorithm, qosRequirements)
      }
    case GetMechanism(cfm, contextConfig, latencyAverage, qosRequirements, currentConfig, currentAlgorithm) =>
      sender() ! Future {
        this.getMechanism(cfm, contextConfig, latencyAverage, qosRequirements, currentConfig, currentAlgorithm)
      }
  }

  // Handle the new observation
  def receive(cfm: CFM, contextConfig: Map[String, AnyVal], currentAlgorithm: String, qosRequirements: List[Requirement]): Boolean;

  // Used by Planner for planning
  def getMechanism(cfm: CFM, contextConfig: Config, latencyAverage: Double, qosRequirements: Set[Requirement], currentConfig: Map[String, AnyVal], currentAlgorithm: String): String;


  // BASE MODELS
  def computeOLSParameters(X: DenseMatrix[Double], y: DenseMatrix[Double]): DenseMatrix[Double] = {
    var theta: DenseMatrix[Double] = X.t * X
    theta = pinv(theta)
    theta = theta * X.t
    theta = theta * y
    theta
  }

  def computeWLSParameters(X: DenseMatrix[Double], y: DenseMatrix[Double], weights: DenseVector[Double]): DenseMatrix[Double] = {
    val W = diag(weights)
    var theta: DenseMatrix[Double] = X.t * W
    theta = theta * X
    log.info(s"\n X: ${X} \n y: $y \n weights: $weights \n theta: $theta")
    theta = pinv(theta)
    theta = theta * X.t
    theta = theta * W
    theta = theta * y
    theta
  }
}

object LearningModelMessages {
  case class Receive(cfm: CFM, contextConfig: Map[String, AnyVal], currentAlgorithm: String, qosRequirements: List[Requirement])
  case class GetMechanism(cfm: CFM, contextConfig: Config, latencyAverage: Double, qosRequirements: Set[Queries.Requirement], currentConfig: Map[String,AnyVal], currentAlgorithm: String)
}