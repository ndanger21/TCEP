package tcep.graph.transition.mapek.learnon

import akka.actor.ActorRef
import breeze.linalg.{DenseMatrix, max, sum}
import com.typesafe.config.ConfigFactory
import tcep.ClusterActor
import tcep.data.Queries._
import tcep.graph.transition.mapek.learnon.ModelRLMessages.QTableUpdate
import tcep.graph.transition.mapek.learnon.ModelRLQUpdaterMessages.{InitUpdater, UpdateQTable}

import scala.collection.mutable.{HashMap, ListBuffer}
import scala.concurrent.Future

class ModelRLQUpdater(learningModel: ActorRef) extends ClusterActor {

  /**
    * Helper model, which is soley responsible to perform a Q-Table update.
    * Receives the Q-Table and updates it.
    * Then sends it back to the Learning Model
    */


  val gamma = ConfigFactory.load().getDouble("constants.modelrl.rl-decay")
  var latencyBins: Range = 0.to(1)
  var loadBins: Range = 0.to(1)
  var stateOrder: List[String] = List("None")
  var opOrder: List[String] = List("None")
  var logData: Long = 0
  val zeroStart = ConfigFactory.load().getBoolean("constants.modelrl.zero-start")

  var latencyProbabilities: HashMap[String, DenseMatrix[Double]] = HashMap.empty[String, DenseMatrix[Double]]
  var loadProbabilities: HashMap[String, DenseMatrix[Double]] = HashMap.empty[String, DenseMatrix[Double]]

  var transitionProbabilities = HashMap.empty[String, HashMap[String, DenseMatrix[Double]]]

  var qTable: HashMap[Int, HashMap[Int, ListBuffer[Double]]] = HashMap.empty[Int, HashMap[Int, ListBuffer[Double]]]

  override def receive: Receive = {
    case InitUpdater(latencyBins, loadBins, stateOrder, opOrder) =>
      this.latencyBins = latencyBins
      this.loadBins = loadBins
      this.stateOrder = stateOrder
      this.opOrder = opOrder
      for (op <- opOrder) {
        var mat: DenseMatrix[Double] = DenseMatrix.ones(latencyBins.size-1, latencyBins.size-1)
        if (this.zeroStart)
          mat = DenseMatrix.zeros(latencyBins.size-1, latencyBins.size-1)
        loadProbabilities += (op -> mat)
      }
      this.transitionProbabilities += (stateOrder(0) -> latencyProbabilities)
      for (op <- opOrder) {
        var mat: DenseMatrix[Double] = DenseMatrix.ones(latencyBins.size-1, latencyBins.size-1)
        if (this.zeroStart)
          mat = DenseMatrix.zeros(latencyBins.size-1, latencyBins.size-1)
        latencyProbabilities += (op -> mat)
      }
      this.transitionProbabilities += (stateOrder(1) -> loadProbabilities)
    case UpdateQTable(latencyProbabilities, loadProbabilities, qTable, requirements) =>
      Future {
        this.latencyProbabilities = latencyProbabilities
        this.loadProbabilities = loadProbabilities
        this.qTable = qTable
        this.transitionProbabilities += (stateOrder(0) -> latencyProbabilities)
        this.transitionProbabilities += (stateOrder(1) -> loadProbabilities)
        this.updateQTable(requirements)
      }(blockingIoDispatcher) // this can take several seconds, we don't want to block on the default dispatcher for all actors
  }

  def getP(feature: String, op: String, oldBin: Int, newBin: Int): Double = {
    if (this.zeroStart)
      this.transitionProbabilities.get(feature).get.get(op).get(oldBin-1, newBin-1)/(sum(this.transitionProbabilities.get(feature).get.get(op).get(oldBin-1, ::))+1)
    else
      this.transitionProbabilities.get(feature).get.get(op).get(oldBin-1, newBin-1)/(sum(this.transitionProbabilities.get(feature).get.get(op).get(oldBin-1, ::)))
  }

  def getTransitionProbability(op: String, latencyBin: Int, loadBin: Int, nextLatencyBin: Int, nextLoadBin: Int): Double = {
    val latP = this.getP(stateOrder(0), op, latencyBin, nextLatencyBin)
    val loadP = this.getP(stateOrder(1), op, loadBin, nextLoadBin)
    latP*loadP
  }

  def getQValue(latencyBin: Int, loadBin: Int) = {
    this.qTable.get(latencyBin).get.get(loadBin).get
  }


  /**
    * Implementation of a simple reward function, where we get +1 for fulfillment and -1 for violation
    * of QoS requirements
    */
  def getReward(latencyBin: Int, loadBin: Int, requirements: List[Requirement]) = {
    var reward = 0.0
    for (req <- requirements) {
      req match {
        case latencyReq: LatencyRequirement =>
          val reqValue = latencyReq.latency.toMillis
          val binValue = this.latencyBins(latencyBin)
          if (compareHelper(reqValue, latencyReq.operator, binValue))
            reward += 1.0
          else
            reward -= 1.0
        case loadReq: LoadRequirement =>
          val reqValue = loadReq.machineLoad.value
          val binValue = this.loadBins(loadBin)
          if (compareHelper(reqValue, loadReq.operator, binValue))
            reward += 1.0
          else
            reward -= 1.0
      }
    }
    reward
  }

  /**
    * Performs the q-table update. Iterates over all states and computes the new q-values according
    * to the bellman equation with the use of the state-transition probability estimations and the reward function.
    */
  def updateQTable(qosRequirements: List[Requirement]) = {
    val start = System.currentTimeMillis()
    for (l <- 1 to latencyBins.size-1) {
      for (s <- 1 to loadBins.size-1) {
        val opQs = ListBuffer.empty[Double]
        for (op <- opOrder)
          opQs += 0.0
        for (ln <- 1 to latencyBins.size-1) {
          for (sn <- 1 to loadBins.size-1) {
            val reward = this.getReward(ln, sn, qosRequirements)
            val maxVal = max(this.getQValue(ln, sn))
            for (op <- opOrder) {
              val tProbability = this.getTransitionProbability(op, l, s, ln, sn)
              opQs(opOrder.indexOf(op)) += tProbability*(reward+this.gamma*maxVal)
            }
          }
        }
        this.qTable.get(l).get += (s -> opQs)
      }
    }
    val duration = System.currentTimeMillis()-start
    this.logData = duration
    log.info(s"Q-Table update took ${duration}ms.")
    this.learningModel ! QTableUpdate(this.qTable.clone(), this.logData)
  }

  /**
    * Created by Niels on 14.04.2018.
    * Copied from RequirementChecker
    *
    * helper function to compare a requirement value to an actual value
    *
    * @param reqVal value of the requirement
    * @param op comparison operator
    * @param otherVal value to compare to
    * @return true if requirement is condition holds, false if violated
    */
  def compareHelper(reqVal: Double, op: Operator, otherVal: Double): Boolean = {
    op match {
      case Equal => reqVal == otherVal
      case NotEqual => reqVal != otherVal
      case Greater => otherVal > reqVal
      case GreaterEqual => otherVal >= reqVal
      case Smaller => otherVal < reqVal
      case SmallerEqual => otherVal <= reqVal
    }
  }
}

object ModelRLQUpdaterMessages {
  case class InitUpdater(latencyBins: Range, loadBins: Range, stateOrder: List[String], opOrder: List[String])
  case class UpdateQTable(latencyProbabilities: HashMap[String, DenseMatrix[Double]], loadProbabilities: HashMap[String, DenseMatrix[Double]], qTable: HashMap[Int, HashMap[Int, ListBuffer[Double]]], requirements: List[Requirement])
}
