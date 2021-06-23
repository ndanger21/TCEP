package tcep.graph.transition.mapek.contrast

import com.google.common.base.Charsets
import com.google.common.io.Resources
import org.cardygan.config._
import org.cardygan.config.util.{ConfigFactoryUtil, ConfigUtil}
import org.cardygan.fm.impl.{IntImpl, RealImpl}
import org.cardygan.fm.util.FmUtil
import org.cardygan.fm.{Attribute, FM, Feature, Real}
import org.cardygan.xtext.util.CardyUtil
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import scala.collection.JavaConverters._
/**
  * Created by Niels on 04.02.2018.
  */
/**
  * loads and maintains the current Context Feature Model (CFM) specified in cfm.cardy
  * provides utilities for handling the CFM and generating its current configuration (Config)
  */
class CFM {

  protected val log: Logger = LoggerFactory.getLogger(getClass)

  var cfm: FM = _
  init()

  def init() = {
    try {
      val cfmAsString: String = Resources.toString(Resources.getResource("cfm.cardy"), Charsets.UTF_8)
      //log.info(s"loading the following cfm: $cfmAsString")
      cfm = CardyUtil.loadFmFromString(cfmAsString)
      //cfm = CardyUtil.loadFmFromFile(Resources.getResource("cfm.cardy"))
    } catch {
      //case e: java.net.UnknownHostException => log.error("error loading CFM: unknown host exception " + e.getMessage + " " + e.getCause +" \n " + e.getStackTraceString)
      case e: Throwable => log.error(s"exception thrown while loading cardyfile", e)
        throw new RuntimeException("death by FM loading!")
    }
  }

  /**
    * generates a config instance with all non-optional features that shall not be reconfigured;
    * @param contextData the values of the fixed features' attributes
    * @return a config to be used as input for Coala: all features and attributes set in this config
    *         are considered fixed during the optimization process and will not be modified
    */
  def getCurrentContextConfig(contextData: Map[String, AnyVal]): Config = {

    import tcep.graph.transition.mapek.contrast.FmNames._
    val root: Feature = FmUtil.findFeatureByName(cfm, ROOT).get
    val systemFeatureGroup: Feature = FmUtil.findFeatureByName(cfm, SYSTEM).get
    val mechanismsGroup: Feature = FmUtil.findFeatureByName(cfm, MECHANISMS).get
    val placementAlgorithmsGroup: Feature = FmUtil.findFeatureByName(cfm, PLACEMENT_ALGORITHM).get

    val contextFeatureGroup: Feature = FmUtil.findFeatureByName(cfm, CONTEXT).get
    val networkSituationGroup: Feature = FmUtil.findFeatureByName(cfm, NETSITUATION).get
    val fixedPropertiesGroup: Feature = FmUtil.findFeatureByName(cfm, FIXED_PROPERTIES).get
    val variablePropertiesGroup: Feature = FmUtil.findFeatureByName(cfm, VARIABLE_PROPERTIES).get
    val mobilityOption: Feature = FmUtil.findFeatureByName(cfm, MOBILITY).get

    // context config
    val contextConfig = ConfigFactoryUtil.createConfig(cfm)
    val rootInstance: Instance = ConfigFactoryUtil.createInstance(root.getName, null, root)
    contextConfig.setRoot(rootInstance)

    val systemGroupInstance: Instance = ConfigFactoryUtil.createInstance(systemFeatureGroup.getName, rootInstance, systemFeatureGroup)
    val mechanismsGroupInstance: Instance = ConfigFactoryUtil.createInstance(mechanismsGroup.getName, systemGroupInstance, mechanismsGroup)
    val placementAlgorithmsGroupInstance: Instance = ConfigFactoryUtil.createInstance(placementAlgorithmsGroup.getName, mechanismsGroupInstance, placementAlgorithmsGroup)

    val contextGroupInstance: Instance = ConfigFactoryUtil.createInstance(contextFeatureGroup.getName, rootInstance, contextFeatureGroup)
    val networkSituationGroupInstance: Instance = ConfigFactoryUtil.createInstance(networkSituationGroup.getName, contextGroupInstance, networkSituationGroup)
    val fixedPropertiesGroupInstance: Instance = ConfigFactoryUtil.createInstance(fixedPropertiesGroup.getName, networkSituationGroupInstance, fixedPropertiesGroup)
    val variablePropertiesGroupInstance: Instance = ConfigFactoryUtil.createInstance(variablePropertiesGroup.getName, networkSituationGroupInstance, variablePropertiesGroup)

    // non-optional leaf attributes
    CFM.getAllAttributes(fixedPropertiesGroup).foreach(a => createAttributeInst(a, fixedPropertiesGroupInstance, contextData.getOrElse(a.getName, 0.0d)))
    CFM.getAllAttributes(variablePropertiesGroup).foreach(a => createAttributeInst(a, variablePropertiesGroupInstance, contextData.getOrElse(a.getName, 0.0d)))

    if(contextData.contains(MOBILITY) && contextData(MOBILITY).asInstanceOf[Boolean]) {
      ConfigFactoryUtil.createInstance(MOBILITY, variablePropertiesGroupInstance, mobilityOption)
      log.info("mobility feature is active in context config")
    }
    // check consistency
    ConfigUtil.checkConfig(contextConfig)
    contextConfig
  }

  /**
    * helper functions for retrieving the names of all mandatory, optional features and attributes from the loaded CFM
    */
  def getFeatureNames(mandatory: Boolean): List[String] =
    FmUtil.getAllFeatures(cfm).asScala.toList
    .filter(f =>
      (f.getFeatureInstanceCardinality.getLowerBound > 0) == mandatory)
    .map(f => f.getName)

  def mandatoryFeatureNames: List[String] = getFeatureNames(true)
  def optionalFeatureNames: List[String] =  getFeatureNames(false)
  def getAllFeatureNames: List[String] = mandatoryFeatureNames ++ optionalFeatureNames
  def getAllAttributeNames: List[String] = CFM.getAllAttributes(cfm.getRoot).map(a => a.getName)
  def getMandatoryAttributes: List[Attribute] = {
    (for (f <- FmUtil.getAllFeatures(cfm).asScala.toList
      .filter(f => f.getFeatureInstanceCardinality.getLowerBound > 0))
      yield f.getAttributes.asScala.toList
      ).flatten
  }

  def getFM: FM = cfm

  /**
    * export the CFM in xml format for SPLConqueror
    * @param outputPath output folder
    */
  def exportCFMAsXML(outputPath: String): Unit = {

    val outputfolder: File = new File(outputPath)
    SplConquerorXMLMaker.makeXML(cfm, "CFM", outputfolder, new File(""))
  }

  /**
    * creates an attribute instance (in a Config) for the given attribute
    * @param attribute attribute in the feature model
    * @param parent parent feature instance
    * @param value value of the attribute instance
    * @return the attribute instance attached to the parent, with the value
    */
   def createAttributeInst(attribute: Attribute, parent: Instance, value: AnyVal): AttrInst = {

      attribute.getDomain match {
        case iimpl: IntImpl => {

          val ret = ConfigFactory.eINSTANCE.createIntAttrInst()
          ret.setName(attribute.getName)
          ret.setType(attribute)
          value match {
            case i: scala.Int => ret.setVal(i)
            case l: Long => ret.setVal(l.toInt)
            case d: Double => ret.setVal(d.toInt)
            case _ => log.error("unknown attribute value type")
          }
          if (parent != null) {
            ret.setParent(parent)
            parent.getAttributes.add(ret)
          }
          ret

        }
        case rimpl: RealImpl => {
          val ret = ConfigFactory.eINSTANCE.createRealAttrInst()

          value match {
            case i: scala.Int => ret.setVal(i.toDouble)
            case l: Long => ret.setVal(l.toDouble)
            case d: Double => ret.setVal(d)
            case _ => log.error("unknown attribute value type")

          }
          ret.setName(attribute.getName)
          ret.setType(attribute)
          if (parent != null) {
            ret.setParent(parent)
            parent.getAttributes.add(ret)
          }
          ret
        }
      }
  }



}

object CFM {

  /**
    * checks if the attribute Instance is within the bounds specified by the loaded feature model (cfm.cardy)
    * @param attrInst attribute instance to check
    * @return true if in bounds, false if not
    */
  def isInBounds(attrInst: AttrInst): Boolean = {

    attrInst match {
      case r: RealAttrInst => r.getVal >= attrInst.getType.getDomain.asInstanceOf[Real].getBounds.getLb &&
        r.getVal <= attrInst.getType.getDomain.asInstanceOf[Real].getBounds.getUb
      case i: IntAttrInst => i.getVal >= attrInst.getType.getDomain.asInstanceOf[org.cardygan.fm.Int].getBounds.getLb &&
        i.getVal <= attrInst.getType.getDomain.asInstanceOf[org.cardygan.fm.Int].getBounds.getUb
    }
  }

  /**
    * helper function to recursively retrieve all attributes from a feature and its children
    * @param current the starting point in the feature tree
    * @return all attributes from features below (and including) the current feature
    */
  private def getAllAttributes(current: Feature): List[Attribute] = {

    var res: List[Attribute] = current.getAttributes.asScala.toList
    val it: java.util.Iterator[Feature] = current.getChildren.iterator()

    while (it.hasNext) {
      val nextFeature: Feature = it.next
      res = res ++ getAllAttributes(nextFeature)
    }
    res
  }

  /**
    * readable representation of a CFM Config
    * @param config the Config to print
    * @return its string representation
    */
  def configToString(config: Config): String = {
    s"\nfeature instances: ${ConfigUtil.getAllInstances(config).asScala.map(f => "\n" + f.getName)}" +
      s"\nattribute instances: ${ConfigUtil.getAttrInstances(config, config.getRoot).asScala
        .map(attrInst => "\n" + {
          attrInst match {
            case r: RealAttrInst => s"(${attrInst.getName} ->  ${r.getVal}) " +
              s"is in bounds [${r.getType.getDomain.asInstanceOf[Real].getBounds.getLb}, ${r.getType.getDomain.asInstanceOf[Real].getBounds.getUb}]:" +
              s" ${isInBounds(attrInst)}"
            case i: IntAttrInst => s"(${attrInst.getName} ->  ${i.getVal}) " +
              s"is in bounds [${i.getType.getDomain.asInstanceOf[org.cardygan.fm.Int].getBounds.getLb}, ${i.getType.getDomain.asInstanceOf[org.cardygan.fm.Int].getBounds.getUb}]:" +
              s" ${isInBounds(attrInst)}"
          }
        })}"
  }
}