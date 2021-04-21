package tcep.graph.transition.mapek.learnon

import akka.actor.Cancellable
import breeze.linalg.DenseMatrix
import com.typesafe.config.ConfigFactory
import org.cardygan.config.Config
import tcep.data.Queries
import tcep.data.Queries.Requirement
import tcep.graph.transition.mapek.contrast.CFM
import tcep.graph.transition.mapek.contrast.FmNames._
import tcep.graph.transition.mapek.learnon.ModelRLMessages.{GetModelRLLogData, QTableUpdate}
import tcep.graph.transition.mapek.learnon.ModelRLQUpdaterMessages.{InitUpdater, UpdateQTable}

import java.util.concurrent.TimeUnit
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.concurrent.duration.FiniteDuration
import scala.util.control.Breaks.{break, breakable}

class ModelRL(mapek: LearnOnMAPEK) extends LearningModel {

  /**
    * RL based transition decision model. The state and action space need to be defined in the constructor.
    * After that, the transition probabilities and the Q-Talbe is initialized according to the space.
    *
    * TODO Future work: Select a more efficient representation for the Q-Table (no Hashmap)
    */

  log.info("ModelRL created")

  var logData = ListBuffer.empty[Double]
  val updateEvery = ConfigFactory.load().getInt("constants.modelrl.update-every")
  val init = ConfigFactory.load().getBoolean("constants.modelrl.init-phase")
  val zeroStart = ConfigFactory.load().getBoolean("constants.modelrl.zero-start")
  val latencyBins = 0.to(4000).by(200)
  val loadBins = 0.to(40).by(2)
  //val latencyBins = 0.to(4000).by(100)
  //val loadBins = 0.to(20).by(1)
  val stateOrder = List(LATENCY, AVG_LOAD)
  val opOrder = List(RELAXATION, RIZOU, MOBILITY_TOLERANT, GLOBAL_OPTIMAL_BDP, STARKS, RANDOM)
  val stateSize = (latencyBins.size-1)*(loadBins.size-1)*opOrder.size
  val featureToRange = Map(stateOrder(0) -> latencyBins.toList, stateOrder(1) -> loadBins.toList)
  var lastState: Option[ListBuffer[Int]] = None
  var requirements: List[Requirement] = List()

  var qTableUpdateScheduler: Cancellable = _

  var latencyProbabilities: HashMap[String, DenseMatrix[Double]] = HashMap.empty[String, DenseMatrix[Double]]
  for (op <- opOrder) {
    var mat: DenseMatrix[Double] = DenseMatrix.ones(latencyBins.size-1, latencyBins.size-1)
    if (this.zeroStart)
      mat = DenseMatrix.zeros(latencyBins.size-1, latencyBins.size-1)
    latencyProbabilities += (op -> mat)
    logData += 0.0
  }
  logData += 0.0
  var loadProbabilities: HashMap[String, DenseMatrix[Double]] = HashMap.empty[String, DenseMatrix[Double]]
  for (op <- opOrder) {
    var mat: DenseMatrix[Double] = DenseMatrix.ones(loadBins.size-1, loadBins.size-1)
    if (this.zeroStart)
      mat = DenseMatrix.zeros(loadBins.size-1, loadBins.size-1)
    loadProbabilities += (op -> mat)
  }
  var transitionProbabilities = Map(stateOrder(0) -> latencyProbabilities, stateOrder(1) -> loadProbabilities)

  var qInitialized = false
  var qTable: HashMap[Int, HashMap[Int, ListBuffer[Double]]] = HashMap.empty[Int, HashMap[Int, ListBuffer[Double]]]

  var toInit = ListBuffer.empty[String]
  for (op <- opOrder.slice(1, opOrder.size))
    toInit += op


  override def preStart(): Unit = {
    super.preStart()
  }

  override def receive: Receive = super.receive orElse {
    case GetModelRLLogData =>
      sender() ! this.logData
    case QTableUpdate(table, updateTime) =>
      this.logData = this.logData.slice(0, this.logData.size-1)++ListBuffer(updateTime.toDouble)
      this.qTable = table.clone()
      this.qTableUpdateScheduler = this.context.system.scheduler.scheduleOnce(FiniteDuration(this.updateEvery, TimeUnit.SECONDS))(this.updateQTable)
  }

  // Maps a continious value to the corresponding bin.
  def valueToBin(dataRange: List[Int], value: Double): Int = {
    if (value < dataRange(1))
      return 1
    else if (value >= dataRange(dataRange.size-1))
      return dataRange.size-1
    var out = 0
    breakable {
      for (bin <- 2 to dataRange.size-2) {
        if (value > dataRange(bin-1) && value <= dataRange(bin)) {
          out = bin
          break
        }
      }
    }
    return out
  }

  //Initializes Q-Table with zero values for each state
  def initQTable() = {
    val start = System.currentTimeMillis()
    var qVals = ListBuffer.empty[Double]
    for (o <- opOrder) {
      qVals += 0.0
    }
    for (l <- 1 to latencyBins.size-1) {
      var lat = HashMap.empty[Int, ListBuffer[Double]]
      for (s <- 1 to loadBins.size-1) {
        lat += (s -> qVals.clone())
      }
      this.qTable += (l -> lat)
    }
    val duration = System.currentTimeMillis()-start
    log.info(s"Q-Table of size ${stateSize} took ${duration}ms to create!")
    this.qInitialized=true
    this.mapek.rlUpdater ! InitUpdater(latencyBins, loadBins, stateOrder, opOrder)
    if (!this.init)
      this.qTableUpdateScheduler = this.context.system.scheduler.scheduleOnce(FiniteDuration(this.updateEvery+60, TimeUnit.SECONDS), this.self, this.updateQTable)
  }

  //Used to convert arbitrary value to double
  def handleValue(value: AnyVal) = {
    val out = value match {
      case booleanValue: Boolean => if(booleanValue) 1.0 else 0.0
      case intValue: Int => intValue.toDouble
      case longValue: Long => longValue.toDouble
      case anyVal =>
        anyVal.asInstanceOf[Double]
    }
    out
  }

  //Converts context observation to a manageable state representation for the model.
  def convertToState(contextConfig: Map[String,AnyVal], currentAlgorithm: String): ListBuffer[Int] = {
    val stateList = ListBuffer.empty[Int]
    for (feature <- stateOrder){
      val currentValue = this.handleValue(contextConfig.get(feature).get)
      val valueBin = this.valueToBin(this.featureToRange.get(feature).get, currentValue)
      stateList += valueBin
    }
    stateList += opOrder.indexOf(currentAlgorithm)
    stateList
  }

  // Increment the state stransition probability for the observed transition between two states
  def updateTransitionProbability(state: String, op: String, oldValue: Int, newValue: Int) = {
    this.transitionProbabilities.get(state).get.get(op).get(oldValue.toInt-1, newValue.toInt-1) += 1.0
  }

  def updateTransitionProbabilities(newState: ListBuffer[Int]) = {
    if (this.lastState.isDefined) {
      for (state <- stateOrder) {
        val pos = stateOrder.indexOf(state)
        val oldValue = this.lastState.get(pos)
        val newValue = newState(pos)
        val op = opOrder(newState.last)
        this.updateTransitionProbability(state, op, oldValue, newValue)
      }
    }
    this.lastState = Some(newState)
  }

  // Returns q-values for each mechanims for a given state
  def getQValue(latencyBin: Int, loadBin: Int) = {
    this.qTable.get(latencyBin).get.get(loadBin).get
  }

  //Starts a q-table update
  def updateQTable() = {
    this.mapek.rlUpdater ! UpdateQTable(this.latencyProbabilities.clone(), this.loadProbabilities.clone(), this.qTable.clone(), this.requirements)
  }

  //Selects op mechanism according to highest q-value
  def policy(state: ListBuffer[Int]) = {
    val qVals = this.qTable.get(state(0)).get.get(state(1)).get
    val m = qVals.indexOf(qVals.max)
    (this.opOrder(m).substring(2,this.opOrder(m).size),qVals(m))
  }

  override def receive(cfm: CFM, contextConfig: Map[String, AnyVal], currentAlgorithm: String, qosRequirements: List[Requirement]): Boolean = {
    val startTime = System.currentTimeMillis()
    val state = this.convertToState(contextConfig, "fs"+currentAlgorithm)
    log.info(s"State is: ${state}")
    if (!this.qInitialized)
      this.initQTable()
    this.updateTransitionProbabilities(state)
    this.requirements = qosRequirements
    var qVals = ListBuffer(0.0, 0.0, 0.0, 0.0)
    try {
      qVals = this.getQValue(state(0), state(1))
    } catch {
      case e: Throwable =>
        log.info(s"Q-Vals not received. Exception: $e")
    }
    this.logData = qVals ++ ListBuffer(this.logData.last)
    val endTime = System.currentTimeMillis()
    val duration = endTime-startTime
    log.info(s"Receive took ${duration}ms.")
    true
  }

  override def getMechanism(cfm: CFM, contextConfig: Config, latencyAverage: Double, qosRequirements: Set[Queries.Requirement], currentConfig: Map[String, AnyVal], currentAlgorithm: String): String = {
    log.info("GETMECHANISM called!")
    if (this.init & this.toInit.size > 0) {
      log.info("INIT PHASE -> not using policy")
      val out = this.toInit.head
      this.toInit -= out
      if (this.toInit.size <= 0)
        this.qTableUpdateScheduler = this.context.system.scheduler.scheduleOnce(FiniteDuration(this.updateEvery+60, TimeUnit.SECONDS), this.self, this.updateQTable)
      out.substring(2, out.size)
    } else {
      val state = this.convertToState(currentConfig, currentAlgorithm)
      val algo = this.policy(state)
      log.info(s"Selected ${algo._1} with Q-Val ${algo._2}")
      algo._1
    }
  }
}

object ModelRLMessages {
  case object GetModelRLLogData
  case class QTableUpdate(table: HashMap[Int, HashMap[Int, ListBuffer[Double]]], updateTime: Long)
}