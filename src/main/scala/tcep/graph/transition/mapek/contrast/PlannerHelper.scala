package tcep.graph.transition.mapek.contrast

import java.io.{File, FileOutputStream, InputStream}

import org.cardygan.config.Config
import org.cardygan.fm.FM
import org.coala.Coala
import org.coala.model.PerformanceInfluenceModel
import org.coala.util.SplConquererLogFileReader
import org.slf4j.LoggerFactory
import tcep.data.Queries.Requirement
/**
  * Created by Niels on 24.05.2018.
  */
object PlannerHelper {

  val log = LoggerFactory.getLogger(getClass)

  /**
    * reads a performance model from a logfile (read from a jar, written to a tmp file)
    * @param cfm CFM specifying the features and attributes occurring in the logfile
    * @param filename filepath
    * @return option for the performance influence model
    */
  def extractModelFromFile(cfm: FM, filename: String): Option[PerformanceInfluenceModel] = {

    log.info(s"extractModelFromFile() - entered, filename: ${filename}")
    //if (res.toString.startsWith("jar:"))
      try {
      //log.debug(s"extractModelFromFile() - reading from jar, writing model to temporary file")
      val input: InputStream = getClass.getResourceAsStream(filename)
      //log.debug(s"input stream: ${input}")
      val file: File = File.createTempFile("tempfile", ".tmp")
      val out = new FileOutputStream(file)
      //log.debug(s"output stream: ${out}")
      //log.debug(s"tmpfile: ${file} ${file.getAbsolutePath}")
      var read = 0
      val bytes = new Array[Byte](1024)
      do {
        read = input.read(bytes)
        //log.debug(s"readline: $read")
        if(read != -1) out.write(bytes, 0, read)
      } while (read != -1)

      //log.debug(s"extractModelFromFile() - reading from jar done, calling splc log reader with file $file and contents \n ${FileUtils.readLines(file)}")
      val modelList = SplConquererLogFileReader.readFromFile(file, cfm)
      //log.debug(s"models: ${modelList.size} modelList: ${modelList.asScala.toList}")
      val model = modelList.get(modelList.size() - 1)
      file.deleteOnExit()
      //log.debug(s"extractModelFromFile() - successfully loaded performance model: ${model.getTerms.asScala.toList}")
      Some(model)
    } catch {
        case ex: Throwable =>
          log.error(s"extractModelFromFile() - error while reading performance model $filename", ex)
          None
    }/*
    else {
      //this will work in IDE, but not when running from a JAR
      try {
        log.debug(s"extractModelFromFile() - not using temporary file workaround, loading directly from $filename")
        val modelList = SplConquererLogFileReader.readFromFile(new File(filename), cfm)
        val model = modelList.get(modelList.size() - 1)
        Some(model)
      } catch {
        case ex: Exception =>
          log.error(s"extractModelFromFile() - error while loading from file $filename \n ${ex.getMessage}")
          None
      }
    }*/

  }

  /**
    * Calculates the optimal configuration of system features for the given context feature configuration and performance model.
    * This is done by using the performance model as an objective function for an ILP formulation of the CFM and the given context config
    * @param cfm the CFM to use
    * @param contextConfig the values of the context features which are considered as fixed for the optimization problem
    * @param performanceModel the performance model to optimize by. This model predicts the performance of a configuration of features
    *                         and can be a combination of multiple weighted performance models for different metrics
    * @param qosRequirements the requirements placed on metrics included in the performance model which must not be violated
    * @return the configuration of system features that is optimal for the given context configuration w.r.t. the performance model
    */
  def calculateOptimalSystemConfig(cfm: CFM, contextConfig: Config, performanceModel: PerformanceInfluenceModel,
                                   qosRequirements: Set[Requirement], listener: MyListener = new MyListener()): Option[Config] = {

    try {
      // TODO upgrade to later coala version 0.3.1 to support multiple performance models
      //val perfMap = new java.util.HashMap[PerformanceInfluenceModel, java.lang.Double]()
      //perfMap.put(performanceModel, 1.0d)
      //val coala: Coala = new Coala.CoalaBuilder(cfm.getFM, perfMap)
      val coala: Coala = new Coala.CoalaBuilder(cfm.getFM, performanceModel)
        .withRandomSeed(42)
        // .withSolverOutputFolder(new File("logs/coala.mps"))
        .build()

      coala.registerListener(listener)
      log.debug(s"calculateOptimalSystemConfig - coala listener registered, starting planner with the following context config: ${CFM.configToString(contextConfig)}")

      val optimalSystemConfiguration: Config = coala.plan(contextConfig)
      log.info(s"calculateOptimalSystemConfig - coala has finished, optimal config: " +
        //s"\n ${ConfigUtil.getFeatureInstanceByName(optimalSystemConfiguration.getRoot, FmNames.PLACEMENT_ALGORITHM).getChildren.get(0)}")
        s"\n${CFM.configToString(optimalSystemConfiguration)}")
      Some(optimalSystemConfiguration)

    } catch {
      case e: Throwable =>
        log.error(s"error while executing coala", e)
        None
    }
  }

  /*
  def combinePerformanceModels(models: Map[Symbol, PerformanceInfluenceModel]): PerformanceInfluenceModel = {

    def normalizeZeroToOne(value: Double, min: Double, max: Double): Double = if(max > min) (value - min) / (max - min) else value
    // all models are unmodified as of now -> modify weights of each model's terms to change influence on solution
    var combinedTerms = ListBuffer[List[PerformanceInfluenceTerm]]()
    for(m <- models.values) {

      val terms = m.getTerms.asScala.toList
      val sortedWeights = terms.map(t => t.getWeight).sortWith(_ <= _)
      val min_w = sortedWeights.head
      val max_w = sortedWeights.last

      val normalizedTerms = terms.map(t =>
        new PerformanceInfluenceTerm(normalizeZeroToOne(t.getWeight, min_w, max_w), t.getFeatures, t.getAttributes))
      combinedTerms += normalizedTerms
    }
    new PerformanceInfluenceModel(combinedTerms.flatten.asJava)
  }
  */
}
