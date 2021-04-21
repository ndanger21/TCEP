package tcep.graph.transition.mapek.learnon

import breeze.linalg._
import breeze.numerics
import breeze.numerics.{abs, pow}
import breeze.stats.mean
import com.typesafe.config.ConfigFactory
import org.cardygan.config.util.ConfigUtil
import org.cardygan.config.{Config, Instance}
import org.cardygan.fm.Feature
import org.coala.model.{PerformanceInfluenceModel, PerformanceInfluenceTerm}
import tcep.data.Queries
import tcep.data.Queries._
import tcep.data.Structures.MachineLoad
import tcep.graph.transition.mapek.contrast.FmNames._
import tcep.graph.transition.mapek.contrast.{CFM, FmNames, PlannerHelper, RequirementChecker}
import tcep.graph.transition.mapek.learnon.LearnOnMessages.GetLearnOnLogData

import java.util.NoSuchElementException
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer}

class LearnOn(cfm: CFM, latencyPIMPath: String, loadPIMPath: String) extends LearningModel {
  log.info("LearnOn created!")

  val featureOrder = cfm.optionalFeatureNames ++ cfm.getAllAttributeNames ++ metricElements.filter(p => p == "mLatency" || p == "mAvgSystemLoad"/* || p == "mMsgHops"*/)
  val latencyModel = PlannerHelper.extractModelFromFile(cfm.getFM, latencyPIMPath).getOrElse(throw new NoSuchElementException(s"could not find ${latencyPIMPath}"))
  val loadModel = PlannerHelper.extractModelFromFile(cfm.getFM, loadPIMPath).getOrElse(throw new NoSuchElementException(s"could not find ${loadPIMPath}"))
  //val hopsModel = PlannerHelper.extractModelFromFile(cfm.getFM, "/performanceModels/msgHopsModel.log").getOrElse(throw new NoSuchElementException("could not find /performanceModels/msgHopsModel.log"))
  var perfModels = HashMap("latency" -> latencyModel, "load" -> loadModel/*, "hops" -> hopsModel*/)

  val metricConvert: mutable.HashMap[Int, String] = mutable.HashMap((2 -> "latency"), (1 -> "load")/*, (1 -> "hops")*/)
  //var perfModelThetas: mutable.HashMap[String,mutable.HashMap[Int,DenseMatrix[Double]]] = mutable.HashMap("latency" -> mutable.HashMap.empty[Int, DenseMatrix[Double]], "load" -> mutable.HashMap.empty[Int, DenseMatrix[Double]], "hops" -> mutable.HashMap.empty[Int, DenseMatrix[Double]])
  var perfModelThetas: mutable.HashMap[String,mutable.HashMap[Int,DenseMatrix[Double]]] = mutable.HashMap("latency" -> mutable.HashMap.empty[Int, DenseMatrix[Double]], "load" -> mutable.HashMap.empty[Int, DenseMatrix[Double]])
  //var perfModelBetas: mutable.HashMap[String,mutable.HashMap[Int,Double]] = mutable.HashMap("latency" -> mutable.HashMap.empty[Int, Double], "load" -> mutable.HashMap.empty[Int, Double], "hops" -> mutable.HashMap.empty[Int, Double])
  var perfModelBetas: mutable.HashMap[String,mutable.HashMap[Int,Double]] = mutable.HashMap("latency" -> mutable.HashMap.empty[Int, Double], "load" -> mutable.HashMap.empty[Int, Double]) // beta: amount of trust in prediction of model
  var dataStorage: Option[DenseMatrix[Double]] = Option.empty
  var lastDataChunc: Option[DenseMatrix[Double]] = Option.empty
  var addHypothesis: Boolean = false
  var lastHId: Int = 1

  var perfFeatures: Map[String, List[List[Map[Int, Int]]]] = Map()
  for (model <- perfModels) {
    perfFeatures += (model._1 -> extractFeaturesFromPIM(model._2, model._1))
    log.info(s"Performance model is: ${this.perfFeatures.get(model._1).get}")
  }
  log.info(s"FEATURE ORDER is: ${this.featureOrder}, ${this.featureOrder.length}")
  private val naiveModels = ConfigFactory.load().getBoolean("constants.learnon.naive-models")

  var minReqSamples = Integer.MIN_VALUE
  for (pair <- this.perfFeatures){
    if(pair._2.size > minReqSamples)
      minReqSamples = pair._2.size
  }
  minReqSamples *= 30
  if (this.naiveModels)
    minReqSamples = 18*30
  log.info(s"At least ${minReqSamples} are needed.")
  //private val storageSize = ConfigFactory.load().getInt("constants.learnon.opie-storage")
  private val storageSize = if(ConfigFactory.load().getInt("constants.learnon.opie-storage") > minReqSamples) ConfigFactory.load().getInt("constants.learnon.opie-storage") else minReqSamples
  log.info(s"Starting with Storage Size of ${this.storageSize} samples.")
  private val lossType = ConfigFactory.load().getString("constants.learnon.opie-loss-type")
  private val blacklistedAlgorithms = ConfigFactory.load().getStringList("constants.mapek.blacklisted-algorithms").asScala.toList
  private val improvementThreshold = ConfigFactory.load().getDouble("constants.mapek.improvement-threshold")
  private val power = ConfigFactory.load().getBoolean("constants.learnon.power")
  private val predictionType = ConfigFactory.load().getString("constants.learnon.prediction")
  private val offlineWeight = ConfigFactory.load().getDouble("constants.learnon.offline-weight")
  private val logPrediction = ConfigFactory.load().getBoolean("constants.learnon.log-prediction")
  private val fullModelWeight = ConfigFactory.load().getBoolean("constants.learnon.full-model-weight")
  private val hWeighting = ConfigFactory.load().getBoolean("constants.learnon.hypothesis-weighting")
  private val weightDecay = ConfigFactory.load().getDouble("constants.learnon.weight-decay")

  if (!this.hWeighting){
    for (metric <- this.metricConvert){
      this.perfModelBetas.get(metric._2).get += (this.lastHId -> 2.0)
    }
  }


  override def receive: Receive = super.receive orElse {
    case GetLearnOnLogData(cfm, currentConfig, currentAlgorithm) =>
    sender() ! this.getLogData(cfm, currentConfig, currentAlgorithm)
  }

  /**
    *
    * @return a map with a list of hypothesis tuples of the form (hypothesisPrediction, hypothesisWeight, hypothesisId)
    */
  def getLogData(cfm: CFM, currentConfig: Map[String,AnyVal], currentAlgorithm: String) = {
    //log.info(s"${currentAlgorithm} Context: ${currentConfig}")
    val x = this.createVector(cfm, currentConfig, currentAlgorithm)
    // HashMap[MetricName, List[(hypothesisPrediction, hypothesisWeight, hypothesisId)]]
    var logData: HashMap[String, List[List[Double]]] = mutable.HashMap.empty[String, List[List[Double]]]
    for (metricId <- List(2,1)) {
      var outputs = ListBuffer.empty[(Int,Double,Double)]
      val metric = this.metricConvert.get(metricId).get
      val featX = this.extractFeatures(x, metric)
      if (this.hWeighting & this.lastHId < 3) {
        val theta = this.perfModelThetas.get(metric).get(1)
        var prediction = featX * theta
        if (this.logPrediction & metricId == 2)
          logData += (metric -> List(List(numerics.exp(prediction.toArray.head),1.0)))
        else
          logData += (metric -> List(List(prediction.toArray.head,1.0)))
      } else {
        if (this.hWeighting) {
          var betaSum = 0.0
          for (hId <- 1 to this.lastHId-1) {
            val betas = this.perfModelBetas.get(metric).get
            val beta = betas.get(hId).get
            betaSum = betaSum + beta
            val thetas = this.perfModelThetas.get(metric).get
            val theta = thetas.get(hId).get
            var prediction = featX * theta
            outputs += ((hId,beta,prediction.toArray.head))
          }
          if (this.predictionType.equals("median")) {
            outputs = outputs.sortBy(_._3)
            var metricPredictions = ListBuffer.empty[List[Double]]
            betaSum = 0.5*betaSum
            var out = outputs.last._3
            var outBeta = outputs.last._2
            var tmp = 0.0
            var flag = true
            for (pair <- outputs) {
              tmp = tmp + pair._2
              if (flag && tmp >= betaSum) {
                out = pair._3
                outBeta = pair._2
                flag = false
              } else {
                metricPredictions += List(pair._3,pair._2)
              }
            }
            if (this.logPrediction & (metricId == 2 | this.naiveModels))
              logData += (metric -> (List(List(numerics.exp(out),outBeta)) ++ metricPredictions.toList))
            else
              logData += (metric -> (List(List(out,outBeta)) ++ metricPredictions.toList))
          } else {
            var metricPredictions = ListBuffer.empty[List[Double]]
            //Dont ignore the offline model for the rest of the prediction to give it more weight
            var totalPrediction = 0.0
            var offlinePred = 0.0
            for (pair <- outputs) {
              if (pair._1 != 1) {
                val hPrediction = pair._2/betaSum*pair._3
                totalPrediction += hPrediction
                metricPredictions += List(pair._3,pair._2, pair._1)
              } else {
                offlinePred = pair._3
                //Dont ignore the offline model for the ensemble prediction
                val hPrediction = pair._2/betaSum*pair._3
                totalPrediction += hPrediction
                metricPredictions += List(pair._3,pair._2, pair._1)
              }
            }
            totalPrediction = (totalPrediction*(1.0-this.offlineWeight))+(offlinePred*this.offlineWeight)
            if (this.logPrediction & (metricId == 2 | this.naiveModels))
              totalPrediction = numerics.exp(totalPrediction)
            logData += (metric -> (List(List(totalPrediction, 0.0, 0.0)) ++ metricPredictions.toList))
          }
        } else { // no hypothesis weighting
          if(this.lastHId < 2) {
            val theta = this.perfModelThetas.get(metric).get(1)
            var prediction = featX * theta
            if (this.logPrediction & (metricId == 2 | this.naiveModels))
              logData += (metric -> List(List(numerics.exp(prediction.toArray.head),1.0)))
            else
              logData += (metric -> List(List(prediction.toArray.head,1.0)))
          } else {
            var betaSum = 0.0
            for (hId <- 1 to this.lastHId) {
              val betas = this.perfModelBetas.get(metric).get
              val beta = betas.get(hId).get
              betaSum = betaSum + beta
              val thetas = this.perfModelThetas.get(metric).get
              val theta = thetas.get(hId).get
              var prediction = featX * theta
              outputs += ((hId, beta, prediction.toArray.head))
            }
            //Only for mean predictions
            var metricPredictions = ListBuffer.empty[List[Double]]
            var totalPrediction = 0.0
            var offlinePred = 0.0
            for (pair <- outputs) {
              if (pair._1 != 1) {
                val hPrediction = pair._2/betaSum*pair._3
                totalPrediction += hPrediction
                metricPredictions += List(pair._3,pair._2, pair._1)
              } else {
                offlinePred = pair._3
                //Dont ignore the offline model for the ensemble prediction
                val hPrediction = pair._2/betaSum*pair._3
                totalPrediction += hPrediction
                metricPredictions += List(pair._3,pair._2, pair._1)
              }
            }
            totalPrediction = (totalPrediction*(1.0-this.offlineWeight))+(offlinePred*this.offlineWeight)
            if (this.logPrediction & (metricId == 2 | this.naiveModels))
              totalPrediction = numerics.exp(totalPrediction)
            logData += (metric -> (List(List(totalPrediction, 0.0, 0.0)) ++ metricPredictions.toList))
          }
        }
      }
    }
    log.info(s"Logdata is: ${logData}")
    logData
  }


  /**
    * Loads the offline performance influence model and extracts the
    * relevant features from it.
    * TODO Future: Needs additional tests for fsSystem feature, since we did not use such feature
    */
  def extractFeaturesFromPIM(model: PerformanceInfluenceModel, metric: String): List[List[Map[Int, Int]]] = {
    log.info(s"Extract Features called for $metric!")
    var features: ListBuffer[List[Map[Int, Int]]] = ListBuffer.empty[List[Map[Int, Int]]]
    val terms = model.getTerms.asScala.toList
    var featureWeights: ListBuffer[Double] = ListBuffer.empty[Double]
    for (term <- terms) {
      featureWeights += term.getWeight
      var termEntries: ListBuffer[Map[Int, Int]] = ListBuffer.empty[Map[Int, Int]]
      for (feat <- term.getFeatures.asScala){
        val name = feat._1.getName
        val pos = featureOrder.indexOf(name)
        val value = feat._2.toInt
        termEntries += Map(pos -> value)
      }
      for (attr <- term.getAttributes.asScala.toMap) {
        val name = attr._1.getName
        val pos = featureOrder.indexOf(name)
        val value = attr._2.toInt
        termEntries += Map(pos -> value)
      }
      features += termEntries.toList
    }
    log.info(s"Extraced Theta is: ${featureWeights}")
    perfModelThetas.get(metric).get += (this.lastHId -> new DenseMatrix[Double](featureWeights.size, 1, featureWeights.toArray))
    features.toList
  }

  //Updates the current performance influence model by iterating over current ensemble parameters
  def getPerformanceInfluenceModel(i: Int, metric: String): PerformanceInfluenceModel = {
    val theta = this.perfModelThetas.get(metric).get.get(i).get.flatten()
    val oldPimTerms = this.perfModels.get(metric).get.getTerms.asScala.toList
    var pimTerms: ListBuffer[PerformanceInfluenceTerm] = ListBuffer.empty[PerformanceInfluenceTerm]
    for (tId <- 0 to oldPimTerms.size-1) {
      val term = oldPimTerms(tId)
      var newWeight = theta(tId)
      newWeight = BigDecimal(newWeight).setScale(8, BigDecimal.RoundingMode.HALF_UP).toDouble
      if (newWeight == 0)
        newWeight = pow(1.0, -6)
      val newTerm = new PerformanceInfluenceTerm(newWeight, term.getFeatures, term.getAttributes)
      pimTerms += newTerm
    }
    val newPim = new PerformanceInfluenceModel(pimTerms.toList.asJava)
    this.perfModels += (metric -> newPim)
    newPim
  }

  //Step 5 of algorithm in Thesis
  //Predicts the error on a newly received data batch according the performance on the previous batch
  //Equal weight for the first time this is called
  def estimateWeights(X: DenseMatrix[Double], metric: String): DenseMatrix[Double] = {
    if (this.lastDataChunc.isDefined) {
      var trainX = this.lastDataChunc.get(::, 1 to -1)
      val trainY = this.lastDataChunc.get(::, -1).asDenseMatrix.reshape(trainX.rows, 1)
      trainX = this.extractFeatures(trainX, metric)
      val theta = this.computeOLSParameters(trainX, trainY)
      val featX = this.extractFeatures(X, metric)
      var out = featX * theta
      //Make sure the predicted weights are a proper probability distribution
      val maxVal = max(out)
      val minVal = min(out) - pow(10.0, -6)
      out = out - minVal
      val range = maxVal - minVal
      out = out/range
      var outSum = sum(out)
      out = out/outSum
      return out
    } else {
      return new DenseMatrix[Double](X.rows, 1, List.fill(X.rows)(1.0/X.rows).toArray)
    }
  }

  /**
    * Constructs a feature vector from a context vector according to the
    * performance influence model features.
    */
  def extractFeatures(X: DenseMatrix[Double], metric: String): DenseMatrix[Double] = {
    var output: DenseMatrix[Double] = DenseMatrix.ones[Double](X.rows,1)
    val terms = this.perfFeatures.get(metric).get
    val numRows = X.rows
    for (term <- terms) {
      var appender: DenseMatrix[Double] = DenseMatrix.ones[Double](numRows, 1)
      for (feat <- term) {
        val till = feat.head._2
        for (i <- 1 to till) {
          val featPos = feat.head._1+1
          appender = appender.*:*(X(::, featPos).asDenseMatrix.reshape(numRows, 1))
        }
      }
      output = DenseMatrix.horzcat(output, appender)
    }
    output(::, 1 to output.cols-1)
  }


  /**
    * Computes the loss accodring to Drucker et al.
    * Removed exponential loss, since there were some errors with Breeze
    */
  def computeLoss(yHat: DenseMatrix[Double], y: DenseMatrix[Double]): DenseMatrix[Double] = {
    val nRows = yHat.rows
    var error = DenseMatrix.ones[Double](nRows, 1)
    var m = 0.0
    if (this.lossType.equals("linear")) {
      error = abs(yHat-y)
      m = max(error) + pow(10.0, -6)
      error = error / m
    } else {
      if (this.lossType.equals("square")) {
        error = pow(yHat - y, 2)
        m = max(error) + pow(10.0, -6)
        error = error / m
      }/* else {
        error = abs(yHat-y)
        m = max(error) + pow(10.0, -6)
        error = 1.0 - exp(-error/m)
      }*/
    }
    error
  }

  //Stores the received data sample in the storage
  def store(sample: DenseMatrix[Double]): Unit = {
    if (this.dataStorage.isDefined) {
      this.dataStorage = Some(DenseMatrix.vertcat(this.dataStorage.get, sample))
    } else {
      this.dataStorage = Some(sample)
    }
    if (this.dataStorage.get.rows >= this.storageSize) {
      this.addHypothesis = true
    }
    log.info(s"DataWindow contains ${this.dataStorage.get.rows} samples.")
  }


  /**
    * Create a vector from a context sample according to the CFM
    */
  def createVector(cfm: CFM, contextConfig: Map[String, AnyVal], currentAlgorithm: String): DenseMatrix[Double] = {
    var vectorList: ListBuffer[Double] = ListBuffer[Double]()
    vectorList += 1.0
    for (feature <- featureOrder) {
      var attrValue = 0.0
      if (allPlacementAlgorithms.contains(feature)){
        if (feature.contains(currentAlgorithm)) {
          attrValue = 1.0
        } else {
          attrValue = 0.0
        }
      } else {
        attrValue = contextConfig.get(feature).get match {
          case booleanValue: Boolean => if(booleanValue) 1.0 else 0.0
          case intValue: Int => intValue.toDouble
          case longValue: Long => longValue.toDouble
          case anyVal =>
            anyVal.asInstanceOf[Double]
        }
      }
      if (this.logPrediction & (feature.equals("mLatency") | (feature.equals("mAvgSystemLoad") & this.naiveModels)))
        vectorList += numerics.log(attrValue)
      else
        vectorList += attrValue
    }
    val vector = new DenseVector(vectorList.toArray).asDenseMatrix
    vector
  }

  /**
    * Returns the id of the hypothesis to use for the given metric
    * Only used for median prediction
    */
  def getPredictions(x: DenseMatrix[Double], metric: String): Int = {
    var betaSum = 0.0
    var outputs = ListBuffer.empty[(Int,Double,Double)]
    val featX = this.extractFeatures(x, metric)
    for (hId <- 1 to this.lastHId-1) {
      val betas = this.perfModelBetas.get(metric).get
      val beta = betas.get(hId).get
      betaSum = betaSum + beta
      val thetas = this.perfModelThetas.get(metric).get
      val theta = thetas.get(hId).get
      val prediction = featX * theta
      outputs += ((hId,beta,prediction.toArray.head))
    }
    outputs = outputs.sortBy(_._3)
    betaSum = 0.5*betaSum
    var out = outputs.last._1
    var tmp = 0.0
    var flag = true
    for (pair <- outputs) {
      tmp = tmp + pair._2
      if (flag && tmp >= betaSum) {
        out = pair._1
        flag = false
      }
    }
    out
  }


  /**
    * Construct a performance influence model from the online model according to a
    * weighted mean for later use in the RequirementChecker
    */
  def getWeightedPim(x: DenseMatrix[Double], metric: String): PerformanceInfluenceModel = {
    var weightedTheta: DenseMatrix[Double] = DenseMatrix.zeros(1,1)
    var betaSum = 0.0
    if (this.hWeighting) {
      for (hId <- 1 to this.lastHId-1){
        if (hId == 1) {
          weightedTheta = this.perfModelThetas.get(metric).get.get(hId).get*this.offlineWeight
          betaSum += this.perfModelBetas.get(metric).get.get(hId).get
        } else {
          betaSum += this.perfModelBetas.get(metric).get.get(hId).get
        }
      }
    } else {
      for (hId <- 1 to this.lastHId){
        if (hId == 1) {
          weightedTheta = this.perfModelThetas.get(metric).get.get(hId).get*this.offlineWeight
          betaSum += this.perfModelBetas.get(metric).get.get(hId).get
        } else {
          betaSum += this.perfModelBetas.get(metric).get.get(hId).get
        }
      }
    }
    if (this.hWeighting) {
      for (hId <- 1 to this.lastHId-1) {
        weightedTheta += this.perfModelThetas.get(metric).get.get(hId).get*((1.0-this.offlineWeight)*this.perfModelBetas.get(metric).get.get(hId).get/betaSum)
      }
    } else {
      for (hId <- 1 to this.lastHId) {
        weightedTheta += this.perfModelThetas.get(metric).get.get(hId).get*((1.0-this.offlineWeight)*this.perfModelBetas.get(metric).get.get(hId).get/betaSum)
      }
    }
    val oldPimTerms = this.perfModels.get(metric).get.getTerms.asScala.toList
    var pimTerms: ListBuffer[PerformanceInfluenceTerm] = ListBuffer.empty[PerformanceInfluenceTerm]
    val theta = weightedTheta.flatten()
    for (tId <- 0 to oldPimTerms.size-1) {
      val term = oldPimTerms(tId)
      var newWeight = theta(tId)
      newWeight = BigDecimal(newWeight).setScale(8, BigDecimal.RoundingMode.HALF_UP).toDouble
      if (newWeight == 0)
        newWeight = pow(1.0, -6)
      val newTerm = new PerformanceInfluenceTerm(newWeight, term.getFeatures, term.getAttributes)
      pimTerms += newTerm
    }
    val newPim = new PerformanceInfluenceModel(pimTerms.toList.asJava)
    this.perfModels += (metric -> newPim)
    newPim
  }

  /**
    * Retrives the performance influence model and forwards to the Requirement checker
    */
  def getPerformanceModelsForChecker(x: DenseMatrix[Double]): Map[Symbol, PerformanceInfluenceModel] = {
    if (this.power) {
      var out: Map[Symbol,PerformanceInfluenceModel] = Map()
      if (this.predictionType.equals("median")) {
        for (i <- 1 to 2) {
          val metric = this.metricConvert.get(i).get
          var toUseId = 1
          if (this.lastHId > 2)
            toUseId = this.getPredictions(x, metric)
          val pim = this.getPerformanceInfluenceModel(toUseId, metric)
          log.info(s"For ${metric}, best Hid: ${toUseId} with model: ${pim.getTerms().toString}")
          out += (Symbol.apply(metric) -> pim)
        }
        out
      } else {
        if (this.hWeighting & this.lastHId>2) {
          var out: Map[Symbol,PerformanceInfluenceModel] = Map()
          for (i <- 1 to 2) {
            val metric = this.metricConvert.get(i).get
            out += (Symbol.apply(metric) -> this.getWeightedPim(x, metric))
          }
          out
        } else {
          if (this.hWeighting) {
            //if we can calculate no mean, because not enough models are available
            var out: Map[Symbol,PerformanceInfluenceModel] = Map()
            for (model <- this.perfModels)
              out += (Symbol.apply(model._1) -> model._2)
            out
          } else {
            if (this.lastHId >= 2) {
              var out: Map[Symbol,PerformanceInfluenceModel] = Map()
              for (i <- 1 to 2) {
                val metric = this.metricConvert.get(i).get
                out += (Symbol.apply(metric) -> this.getWeightedPim(x, metric))
              }
              out
            } else {
              var out: Map[Symbol,PerformanceInfluenceModel] = Map()
              for (model <- this.perfModels)
                out += (Symbol.apply(model._1) -> model._2)
              out
            }
          }
        }
      }
    } else {
      var out: Map[Symbol,PerformanceInfluenceModel] = Map()
      for (model <- this.perfModels)
        out += (Symbol.apply(model._1) -> model._2)
      out
    }
  }


  /**
    * Computes the optimal system feature configuration for a given CFM und the current context with the given performance influence model
    * Mainly copied from ContrastPlanner, with small adjustments to support log prediction usw.
    */
  def getOptimalConfig(cfm: CFM, reqChecker: RequirementChecker, contextConfig: Config, reqPerfModels: Map[Symbol, PerformanceInfluenceModel], qosRequirements: Set[Queries.Requirement], latencyAverage: Double): Option[String] = {
    try {
      val fulfillableRequirements = reqChecker.excludeUnfitConfigurations(qosRequirements)
      val optimalConfig: Option[Config] =
        if(fulfillableRequirements.values.forall(_ == true)) // for every requirement at least one potentially fulfilling algorithm must exist
          PlannerHelper.calculateOptimalSystemConfig(cfm, contextConfig, reqChecker.perfModels('latency), qosRequirements)
        else None
      val placementAlgorihm: Option[String] =
        if (optimalConfig.isDefined) {
          val optimalAlgorithmFeature: Feature = ConfigUtil.getFeatureInstanceByName(optimalConfig.get.getRoot, FmNames.PLACEMENT_ALGORITHM).getChildren.get(0).getType
          val optimalSystemConfig = reqChecker.generateSystemConfig(optimalAlgorithmFeature) // FM config without the context side set
          var predictedLatency: Double = reqChecker.predictMetricFromModel(optimalSystemConfig, performanceModel = reqChecker.perfModels('latency))
          if (this.logPrediction)
            predictedLatency = numerics.exp(predictedLatency)
          log.info(s"RunPlanner - current latency: $latencyAverage, predicted latency with ${optimalAlgorithmFeature.getName}: $predictedLatency ")
          // extract placement algorithm from optimal config
          val placementAlgorithmGroup: Instance = ConfigUtil.getFeatureInstanceByName(optimalSystemConfig.getRoot, FmNames.PLACEMENT_ALGORITHM)
          val placementAlgorithmList = placementAlgorithmGroup.getChildren.asScala.toList
          val nextPlacementAlgorithmInstance = placementAlgorithmList.headOption
          var out: Option[String] = None
          if (placementAlgorithmList.size != 1 || nextPlacementAlgorithmInstance.isEmpty) {
            out = Some("NoTransition")
            log.error(s"invalid config with more or less than one placement algorithm: $placementAlgorithmList")
          } else if(predictedLatency <= latencyAverage * (1 - improvementThreshold)) { // a solution for the ILP model exists within the given requirement constraints; its performance is predicted to be at least x% better
            out = Some(nextPlacementAlgorithmInstance.get.getName.substring(2)) // remove the "fs" prefix
            log.info(s"sending notification to Executor for algorithm ${nextPlacementAlgorithmInstance.get.getName}: estimated improvement ${(1 - (predictedLatency / latencyAverage)) * 100.0}% (threshold: ${improvementThreshold*100.0})%")
          } else {
            out = Some("NoTransition")
            log.info(s"not sending new config to Executor since it is not estimated to be at least ${improvementThreshold*100.0}% better than the current configuration")
          }
          out
        }  else {
          None
        }
      placementAlgorihm
    } catch {
      case e: Throwable => log.info(s"error while calculating optimal system config with error: $e")
        Some("NoTransition")
    }
  }

  /**
    * Compute the parameter vector of the ensemble according to a weighted mean.
    */
  def getEnsembleTheta(metric: String) = {
    var weightedTheta: DenseMatrix[Double] = DenseMatrix.zeros(1,1)
    var betaSum = 0.0
    for (hId <- 1 to this.lastHId){
      if (hId == 1) {
        weightedTheta = this.perfModelThetas.get(metric).get.get(hId).get*this.offlineWeight
        betaSum += this.perfModelBetas.get(metric).get.get(hId).get
      } else {
        betaSum += this.perfModelBetas.get(metric).get.get(hId).get
      }
    }
    for (hId <- 1 to this.lastHId) {
      weightedTheta += this.perfModelThetas.get(metric).get.get(hId).get*((1-this.offlineWeight)*this.perfModelBetas.get(metric).get.get(hId).get/betaSum)
    }
    weightedTheta
  }

  /**
    * Called by K-Component to store new observations
    * Upon receipt of a new observation. Stores it in the storage and computes new hypothesis as necessary.
    * More info in the design section
    */

  override def receive(cfm: CFM, contextConfig: Map[String, AnyVal], currentAlgorithm: String, qosRequirements: List[Requirement]): Boolean = {
    if (this.power) {
      //log.info(s"Storing ${currentAlgorithm} context: ${contextConfig}")
      val dataVector = this.createVector(cfm, contextConfig, currentAlgorithm)
      this.store(dataVector)
      if (this.addHypothesis) {
        log.info("ADDING Hypothesis")
        this.addHypothesis = false
        val X = this.dataStorage.get//(::, 1 to -2)
        var newDataChunk = X.copy
        for (pos <- List(2,1)) {
          val Y = this.dataStorage.get(::, -pos).asDenseMatrix.reshape(X.rows, 1)
          val metric = this.metricConvert.get(pos).get
          val pHat = this.estimateWeights(X, metric)
          val featX = this.extractFeatures(X, metric)
          var sums = ListBuffer.empty[Double]
          for (col <- 0 to featX.cols-1)
            sums += sum(featX(::, col))
          log.info(s"$metric Feature Col Sums: ${sums}")
          val thetas = this.perfModelThetas.get(metric).get
          var theta = thetas.get(this.lastHId).get
          var yHat = featX.*(theta)
          var loss = this.computeLoss(yHat, Y)
          var scaledLoss = loss *:* pHat
          var epsilon = sum(scaledLoss)
          if (epsilon == 0)
            epsilon = pow(10, -6)
          var beta = epsilon/(1.0-epsilon)
          val beta_weight = numerics.log((1.0/beta)+1.0)

          for (beta <- this.perfModelBetas.get(metric).get)
            this.perfModelBetas.get(metric).get += (beta._1 -> beta._2*this.weightDecay)
          if (this.hWeighting)
            this.perfModelBetas.get(metric).get += (this.lastHId -> beta_weight)

          var P = pHat
          if (beta <= 1)
            P = pHat.*:*(beta.^:^(1.0-loss))
          else
            P = pHat.*:*(beta.^:^(loss))
          P = P/sum(P)
          if (this.fullModelWeight & this.lastHId>=2){
            //Compute sample weights wrt. complete model
            theta = this.getEnsembleTheta(metric)
            log.info(s"Weighted theta: ${theta.flatten()}")
            yHat = featX * theta
            loss = this.computeLoss(yHat, Y)
            epsilon = mean(loss)
            if (epsilon == 0)
              epsilon = pow(10, -6)
            beta = epsilon/(1-epsilon)
            if (beta <= 1)
              P = pHat.*:*(beta.^:^(1.0-loss))
            else
              P = pHat.*:*(beta.^:^(loss))
            P = P/sum(P)
          }
          theta = this.getEnsembleTheta(metric)
          var newTheta = this.computeWLSParameters(featX, Y, P.flatten())
          var thetaBuffer = ListBuffer.empty[Double]
          for (i <- 0 to sums.size-1) {
            val s = sums(i)
            if (s == 0) {
              thetaBuffer += theta.flatten()(i)
            } else {
              thetaBuffer += newTheta.flatten()(i)
            }
          }
          newTheta = new DenseMatrix[Double](newTheta.rows, 1, thetaBuffer.toArray)
          log.info(s"new THETA is: ${newTheta.flatten()}")
          val hId = this.lastHId+1
          this.perfModelThetas.get(metric).get += (hId -> newTheta)
          if (!this.hWeighting)
            this.perfModelBetas.get(metric).get += (hId -> 2.0)
          newDataChunk = DenseMatrix.horzcat(newDataChunk, P)
        }
        this.lastHId += 1
        this.lastDataChunc = Some(newDataChunk)
        this.dataStorage = None
      }
      log.info(s"BETAS are: ${this.perfModelBetas}, Hid is: ${this.lastHId}")
    } else
      log.info("No learning done, because turned off in configuration file.")
    true
  }

  //used for evaluation purposes
  def getUpdatedPerformanceModels(cfm: CFM, currentConfig: Map[String,AnyVal], currentAlgorithm: String) = {
    val x = this.createVector(cfm, currentConfig, currentAlgorithm)
    this.getPerformanceModelsForChecker(x)
  }

  //Called by P-Component for planning.
  //Similar functionality as COntrastPlanner
  override def getMechanism(cfm: CFM, contextConfig: Config, latencyAverage: Double, qosRequirements: Set[Queries.Requirement], currentConfig: Map[String,AnyVal], currentAlgorithm: String): String = {
    log.info("GETMECHANISM called!")
    var requirements = qosRequirements
    val x = this.createVector(cfm, currentConfig, currentAlgorithm)
    var reqPerfModels = this.getPerformanceModelsForChecker(x)
    var i = 1
    var configFound: Boolean = false
    var transitionTarget = currentAlgorithm
    while (i <= 4 && !configFound) {
      val fm = cfm.getFM
      fm.getCrossTreeConstraints.clear()
      val reqChecker = new RequirementChecker(fm, contextConfig, blacklistedFeatures = blacklistedAlgorithms, perfModels = reqPerfModels)
      val optimalConfig = this.getOptimalConfig(cfm, reqChecker, contextConfig, reqPerfModels, requirements, latencyAverage)
      if (optimalConfig.isDefined) {
        log.info(s"Optimal config is defined and is: ${optimalConfig.get}")
        configFound = true
        transitionTarget = optimalConfig.get
      } else {
        log.error(s"No feasible solution found. -> Relaxing the requirements by 10% for the $i th time.")
        val weakenedRequirements: Set[Requirement] = requirements.map(req => {
          if(!reqChecker.requirementHolds(req)) {
            req match {
              case lr: LatencyRequirement =>
                val delta = lr.latency.dividedBy(10)
                lr.operator match {
                  case Greater | GreaterEqual => LatencyRequirement(lr.operator, lr.latency.minus(delta), lr.otherwise)
                  case Smaller | SmallerEqual => LatencyRequirement(lr.operator, lr.latency.plus(delta), lr.otherwise)
                  case _ => lr
                }
              case lr: LoadRequirement =>
                val delta = lr.machineLoad.value / 10.0
                lr.operator match {
                  case Greater | GreaterEqual => LoadRequirement(lr.operator, MachineLoad(lr.machineLoad.value - delta), lr.otherwise)
                  case Smaller | SmallerEqual => LoadRequirement(lr.operator, MachineLoad(lr.machineLoad.value + delta), lr.otherwise)
                  case _ => lr
                }
              case hr: MessageHopsRequirement =>
                hr.operator match {
                  case Greater | GreaterEqual => MessageHopsRequirement(hr.operator, hr.requirement - 1, hr.otherwise)
                  case Smaller | SmallerEqual => MessageHopsRequirement(hr.operator, hr.requirement + 1, hr.otherwise)
                  case _ => hr
                }
              case _ => req
            }
          } else req
        })
        requirements = weakenedRequirements
        i += 1
      }
    }
    transitionTarget
  }
}
object LearnOnMessages {
  case class GetLearnOnLogData(cfm: CFM, currentConfig: Map[String,AnyVal], currentAlgorithm: String)
}