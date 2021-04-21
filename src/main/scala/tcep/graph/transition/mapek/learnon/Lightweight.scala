package tcep.graph.transition.mapek.learnon

import com.typesafe.config.ConfigFactory
import org.cardygan.config.Config
import tcep.data.Queries
import tcep.data.Queries._
import tcep.graph.transition.mapek.contrast.CFM
import tcep.graph.transition.mapek.contrast.FmNames._
import tcep.graph.transition.mapek.learnon.LightweightMessages.GetLightweightLogData
import tcep.placement.benchmarking.BenchmarkingNode

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

class Lightweight extends LearningModel {

  /**
    * Lightweight transitions approach defined by Luthra et al. in TCEP paper.
    */

  var possibleAlgorithms: ListBuffer[String] = new ListBuffer()
  var requirements: ListBuffer[String] = new ListBuffer()
  var scores: mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]] = mutable.HashMap.empty[String, mutable.HashMap[String,mutable.HashMap[String,Double]]]
  var decay: Double = ConfigFactory.load().getDouble("constants.mapek.lightweight-decay")
  var logData: (List[Any],List[Any]) = (List.empty[Any],List.empty[Any])

  override def preStart(): Unit = {
    super.preStart()
    var placementAlgorithms = BenchmarkingNode.getPlacementAlgorithms()
    for (algo <- ConfigFactory.load().getStringList("constants.mapek.blacklisted-algorithms").asScala.toList) {
      placementAlgorithms = placementAlgorithms.filter(p => "fs" + p.placement.name != algo)
    }
    for(algo <- placementAlgorithms)
      this.addAlgorithm(algo.placement.name)
  }

  override def receive: Receive = super.receive orElse {
    case GetLightweightLogData =>
      sender() ! this.logData
  }

  /**
    * Once a new observation is received. The fulfillment of the current QoS is checked and the respective counter is increased
    * depending wether fulfilled or not fulfilled.
    */
  override def receive(cfm: CFM, contextConfig: Map[String, AnyVal], currentAlgorithm: String, qosRequirements: List[Queries.Requirement]): Boolean = {
    for (req <- qosRequirements) {
      this.addRequirement(req.name)
    }
    for (contextVal <- contextConfig) {
      var requirement = qosRequirements.find(p => p.name == contextVal._1.toLowerCase().substring(1, contextVal._1.length))
      if (contextVal._1.equals(AVG_LOAD))
        requirement = qosRequirements.find(p => p.name == "machineLoad")
      if (requirement.isDefined) {
        requirement.get match {
          case latencyReq: LatencyRequirement =>
            val reqValue = latencyReq.latency.toMillis
            val currentValue = this.handleValue(contextVal._2)
            log.info(s"${requirement.get.name} Requirement is: $reqValue and current Value is: $currentValue")
            if (compareHelper(reqValue, latencyReq.operator, currentValue))
              this.increaseFitness(currentAlgorithm, requirement.get.name)
            else
              this.addSample(currentAlgorithm, requirement.get.name)
          case loadReq: LoadRequirement =>
            val reqValue = loadReq.machineLoad.value
            val currentValue = this.handleValue(contextVal._2)
            log.info(s"${requirement.get.name} Requirement is: $reqValue and current Value is: $currentValue")
            if (compareHelper(reqValue, loadReq.operator, currentValue))
              this.increaseFitness(currentAlgorithm, requirement.get.name)
            else
              this.addSample(currentAlgorithm, requirement.get.name)
        }
      }
    }
    true
  }

  /**
    * Function used to convert any variable to double
    */
  def handleValue(value: AnyVal) = {
    val out = value match {
      case booleanValue: Boolean => if(booleanValue) 1.0 else 0.0
      case intValue: Int => intValue.toDouble
      case integerValue: Int => integerValue.toDouble
      case longValue: Long => longValue.toDouble
      case anyVal =>
        anyVal.asInstanceOf[Double]
    }
    out
  }

  /**
    * Returns the randomly selected op mechanism according to their fitness score ranking.
    * On receipt, the fitness scores of each mechanism is updated according to the fulfillment scores.
    * Then a ranking is computed and the corresponding selection probabilities.
    */
  override def getMechanism(cfm: CFM, contextConfig: Config, latencyAverage: Double, qosRequirements: Set[Queries.Requirement], currentConfig: Map[String, AnyVal], currentAlgorithm: String): String = {
    val toInit: String = this.getPlacementToInit()
    log.info("GETMECHANISM")
    if(toInit.equals("None")) {
      log.info("Computing Fitness Scores")
      this.updateFitness()
      var opScores: mutable.HashMap[String, Double] = mutable.HashMap.empty[String, Double]
      for (qos <- scores) {
        for (op <- qos._2) {
          if (opScores.contains(op._1)) {
            val newVal = opScores.get(op._1).get + this.scores.get(qos._1).get(op._1).get("fitness").get
            opScores += (op._1 -> newVal)
          } else {
            opScores += (op._1 -> this.scores.get(qos._1).get(op._1).get("fitness").get)
          }
        }
      }
      val ranking = mutable.LinkedHashMap(opScores.toSeq.sortBy(_._2): _*)
      val probas = computeRankingProbabilities()
      val selection = selectAccordingToProbas(probas)
      log.info(s"Ranking is ${ranking.keys.toList}. Probabilities are: ${probas.toList.toString()}. Selection is: $selection")
      var rank = 0
      for (op <- ranking.toList) {
        rank += 1
      }
      ranking.toList(selection)._1
    } else {
      toInit
    }
  }

  /**
    * Initialization of a new algorithm, which get added to the Model.
    */
  def addAlgorithm(name: String): Unit = {
    if (!possibleAlgorithms.contains(name)) {
      possibleAlgorithms += name
      for (req <- requirements) {
        scores += (req -> mutable.HashMap(name -> mutable.HashMap("scored" -> 0.0)))
        scores.get(req).get(name) += ("total" -> 0.0)
        scores.get(req).get(name) += ("fitness" -> 0.0)
      }
    }
  }

  def addAlgorithms(names: List[String]): Unit = {
    for (name <- names)
      addAlgorithm(name)
  }

  /**
    * Used to add an additional requirement to the model. Typically the mechanisms are added before the
    * requirements.
    */
  def addRequirement(name: String): Unit = {
    if (!requirements.contains(name)){
      requirements += name
      scores += (name -> mutable.HashMap.empty[String, mutable.HashMap[String, Double]])
      for (op <- possibleAlgorithms) {
        scores.get(name).get += (op -> mutable.HashMap("scored" -> 0.0))
        scores.get(name).get(op) += ("total" -> 0.0)
        scores.get(name).get(op) += ("fitness" -> 0.0)
      }
      log.info(s"Requirements added: ${scores}")
    }
  }

  def addRequirements(names: List[String]): Unit = {
    for (name <- names)
      addRequirement(name)
  }

  // Used to increase the fulfillment score of a mechanism.
  def increaseFitness(op: String, req: String): Unit = {
    var newVal = scores.get(req).get(op).get("scored").get + 1.0
    if (!requirements.contains(req))
      addRequirement(req)
    scores.get(req).get(op) += ("scored" -> newVal)
    newVal = scores.get(req).get(op).get("total").get + 1.0
    scores.get(req).get(op) += ("total" -> newVal)
  }

  def addSample(op: String, req: String): Unit = {
    if (!requirements.contains(req))
      addRequirement(req)
    val newVal = scores.get(req).get(op).get("total").get + 1.0
    scores.get(req).get(op) += ("total" -> newVal)
  }

  def getOpScore(qos: String, op: String): Double ={
    val score = scores.get(qos).get(op).get("scored").get/(if (scores.get(qos).get(op).get("total").get > 0.0) scores.get(qos).get(op).get("total").get else 1.0)
    score
  }

  /**
    * Iterate through all available mechanisms and calculate the respective fitness score according
    * to the TCEP paper.
    */
  def updateFitness(): Unit = {
    var scoreSave = mutable.HashMap.empty[String, mutable.HashMap[String, Double]]
    for (qos <- scores) {
      log.info(s"for ${qos._1}")
      scoreSave += (qos._1 -> mutable.HashMap("mean" -> 0.0))
      scoreSave.get(qos._1).get += ("max" -> 0.0)
      scoreSave.get(qos._1).get += ("min" -> Double.MaxValue)
      for (op <- qos._2){
        val currentScore = getOpScore(qos._1, op._1)
        val newMean = currentScore + scoreSave.get(qos._1).get("mean")
        scoreSave.get(qos._1).get += ("mean" -> newMean)
        if (scoreSave.get(qos._1).get("min") > currentScore)
          scoreSave.get(qos._1).get += ("min" -> currentScore)
        if (scoreSave.get(qos._1).get("max") < currentScore)
          scoreSave.get(qos._1).get += ("max" -> currentScore)
      }
      scoreSave.get(qos._1).get += ("mean" -> scoreSave.get(qos._1).get("mean") / possibleAlgorithms.length)
      val range = if (scoreSave.get(qos._1).get("max")-scoreSave.get(qos._1).get("min") > 0) scoreSave.get(qos._1).get("max")-scoreSave.get(qos._1).get("min") else 1.0
      //log.info(s"Scoresave is: $scoreSave and range is $range")
      for (op <- qos._2) {
        var scoreComputation = getOpScore(qos._1, op._1) - scoreSave.get(qos._1).get("mean")
        scoreComputation = scoreComputation/range + 1.0
        if (scores.get(qos._1).get(op._1).contains("fitness")) {
          val old = scores.get(qos._1).get(op._1).get("fitness").get
          val newScore = (old*decay)+((1-decay) * scoreComputation)
          scores.get(qos._1).get(op._1) += ("fitness" -> newScore)
        } else
          scores.get(qos._1).get(op._1) += ("fitness" -> scoreComputation)
      }
    }
    log.info(s"Fitness updated: $scores")
    this.setLogData()
  }

  // Checks if the initialization phase is over. Phase is over when every mechanism has received a score.
  def getPlacementToInit(): String = {
    val req = scores.head._1
    var out = "None"
    for (op <- Random.shuffle(possibleAlgorithms)) {
      if (scores.get(req).get(op).get("total").get == 0.0)
        out = op
    }
    out
  }

  //Calculation of the max and min probabilities according to TCEP paper
  def getMinMaxProba(): (Double,Double) = {
    val fullScores: mutable.HashMap[String, Double] = mutable.HashMap.empty[String, Double]
    for (qos <- scores) {
      for (op <- scores.get(qos._1).get) {
        if (!fullScores.contains(op._1))
          fullScores += (op._1 -> 0.0)
        val old = fullScores.get(op._1)
        val newVal = scores.get(qos._1).get(op._1).get("fitness").get+old.get
        fullScores += (op._1 -> newVal)
      }
    }
    var sum = 0.0
    for (op <- fullScores)
      sum += fullScores.get(op._1).get
    var max = 0.0
    var min = Double.MaxValue
    for (op <- fullScores) {
      val tmp = fullScores.get(op._1).get / sum
      if (tmp > max)
        max = tmp
      if (tmp < min)
        min = tmp
    }
    (min, max)
  }

  //Calculation of the rank propabilities accorting to the Linear Rank Selection.
  def computeRankingProbabilities(): Array[Double] = {
    val N: Int = this.possibleAlgorithms.size
    val rankProbas: Array[Double] = new Array[Double](N)
    //val max: Double = ConfigFactory.load().getDouble("constants.mapek.lightweight-bestProbability")
    //val min: Double = 2-max
    val minMax = getMinMaxProba()
    //val max = minMax._2
    val min = minMax._1
    val max: Double = 2-min
    var tmp = 0.0
    for (i <- (0 to N-1)) {
      tmp = min+(max-min)*(i.toDouble/(N-1))
      //log.info(s"min is: $min; Range is: ${max-min}; i/(N-1) is ${i.toDouble/(N-1)}")
      tmp = tmp / N
      //log.info(s"Computed Probability for rank $i is $tmp")
      rankProbas(i) = tmp
    }
    rankProbas
  }

  //Used for logging of the current scores for each mechanism
  def setLogData() = {
    log.info("Gettint Log Data")
    val score_out = new ListBuffer[Any]
    val proba_out = new ListBuffer[Any]
    val toInit = getPlacementToInit()
    var opScores: mutable.HashMap[String, Double] = mutable.HashMap.empty[String, Double]
    for (qos <- scores) {
      for (op <- qos._2) {
        if (opScores.contains(op._1)) {
          val newVal = opScores.get(op._1).get + this.scores.get(qos._1).get(op._1).get("fitness").get
          opScores += (op._1 -> newVal)
        } else {
          opScores += (op._1 -> this.scores.get(qos._1).get(op._1).get("fitness").get)
        }
      }
    }
    for (score <- opScores)
      score_out.append(score)
    if (toInit.equals("None")) {
      for (p <- computeRankingProbabilities())
        proba_out.append(p)
    } else {
      for (op <- possibleAlgorithms)
        proba_out.append(0.0)
    }
    this.logData = (score_out.toList,proba_out.toList)
  }

  /**
    * Performs random selection according to probability distribution.
    * Selects a random uniform value and adds probability for each rank untill
    * uniform random value is exceeded.
    * Maybe there is a better way to do this.
    */
  def selectAccordingToProbas(rankProbas: Array[Double]): Int = {
    val rnd: Double = scala.util.Random.nextDouble()
    var sum: Double = 0
    var out: Int = rankProbas.size-1
    breakable {
      for (i <- 0 to rankProbas.size-1) {
        sum += rankProbas(i)
        if (rnd <= sum){
          out = i
          break
        }
      }
    }
    out
  }

  def stop(): Unit = {
    //plotOut.close()
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
object LightweightMessages {
  case object GetLightweightLogData
}