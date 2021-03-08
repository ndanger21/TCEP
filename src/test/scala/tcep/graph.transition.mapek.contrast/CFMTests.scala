package tcep.graph.transition.mapek.contrast

import org.cardygan.config._
import org.cardygan.config.util.{ConfigFactoryUtil, ConfigUtil}
import org.cardygan.fm._
import org.cardygan.fm.impl.{IntImpl, RealImpl}
import org.cardygan.fm.util.{FmFactoryUtil, FmUtil}
import org.scalatest.FunSuite
import tcep.graph.transition.mapek.contrast.FmNames._
import tcep.graph.transition.mapek.contrast.testextensions.FmUtils

import scala.collection.JavaConversions._

/**
  * Created by Niels on 18.02.2018.
  */
class CFMTests extends FunSuite {

  test("CFM loading - all features, all attributes present") {

    val cfmClass = new CFM(null)
    val cfm: FM = cfmClass.getFM
    // +1, -1 due to fcMobility (only binary context feature)
    assert(FmUtil.getAllFeatures(cfm).size == FmNames.allFeaturesAndAttributes.size - FmNames.allAttributes.size + 1)
    assert(FmUtils.getAllAttributes(cfm).size == FmNames.allAttributes.size - 1)
  }

  test("CFM loading - correct feature names and types") {

    val cfmClass = new CFM(null)
    val cfm: FM = cfmClass.getFM

    val features: List[String] = FmUtil.getAllFeatures(cfm).map(f => f.getName).toList
    val attributes: List[String] = FmUtils.getAllAttributes(cfm).map(a => a.getName).toList

    FmNames.allFeaturesAndAttributes.map(n => assert(features.contains(n) || attributes.contains(n)))
    features.foreach(f => assert(FmNames.cfmFeatures.contains(f) || f.equals(MOBILITY)))
    attributes.foreach(f => assert((FmNames.fixedCFMAttributes ++ FmNames.variableCFMAttributes).contains(f)))
    FmUtils.getAllAttributes(cfm).foreach(a =>
      assert(a.getDomain.isInstanceOf[IntImpl] || a.getDomain.isInstanceOf[RealImpl], "actual domain type: " + a.getDomain))
  }

  test("CFM loading - correct parents") {

    val cfmClass = new CFM(null)
    val cfm: FM = cfmClass.getFM

    assert(FmUtil.findFeatureByName(cfm, ROOT).get.getParent == null)
    assert(FmUtil.findFeatureByName(cfm, SYSTEM).get.getParent.getName == ROOT)
    assert(FmUtil.findFeatureByName(cfm, MECHANISMS).get.getParent.getName == SYSTEM)
    assert(FmUtil.findFeatureByName(cfm, PLACEMENT_ALGORITHM).get.getParent.getName == MECHANISMS)
    assert(FmUtil.findFeatureByName(cfm, STARKS).get.getParent.getName == PLACEMENT_ALGORITHM)
    assert(FmUtil.findFeatureByName(cfm, RELAXATION).get.getParent.getName == PLACEMENT_ALGORITHM)

    assert(FmUtil.findFeatureByName(cfm, CONTEXT).get.getParent.getName == "root")
    assert(FmUtil.findFeatureByName(cfm, NETSITUATION).get.getParent.getName == CONTEXT)
    assert(FmUtil.findFeatureByName(cfm, FIXED_PROPERTIES).get.getParent.getName == NETSITUATION)
    assert(FmUtil.findFeatureByName(cfm, VARIABLE_PROPERTIES).get.getParent.getName == NETSITUATION)

    val fixedGroupAttributes: List[Attribute] = FmUtil.findFeatureByName(cfm, FIXED_PROPERTIES).get.getAttributes.toList
    val variableGroupAttributes: List[Attribute] = FmUtil.findFeatureByName(cfm, VARIABLE_PROPERTIES).get.getAttributes.toList

    FmNames.fixedCFMAttributes.map(n => assert(fixedGroupAttributes.map(a => a.getName).contains(n)))
    FmNames.variableCFMAttributes.map(n => assert(variableGroupAttributes.map(a => a.getName).contains(n) || n == MOBILITY))

  }

  test("CFM loading - correct Attribute properties") {

    val cfmClass = new CFM(null)
    val cfm: FM = cfmClass.getFM
    val attr: Attribute = FmUtil.findAttributeByName(cfm, NODECOUNT_CHANGERATE).get
    assert(attr.getDomain.isInstanceOf[Real])
    assert(attr.getDomain.asInstanceOf[Real].getBounds.getLb == 0)
    assert(attr.getDomain.asInstanceOf[Real].getBounds.getUb == 8)
  }

  test("CFM context config - correct name and number of non-optional feature instances") {

    val cfmClass = new CFM(null)
    val cfm: FM = cfmClass.getFM
    var contextData: Map[String, AnyVal] = Map[String, AnyVal]()
    FmUtil.getFeatureByName(cfm, FIXED_PROPERTIES).get.getAttributes.forEach(a =>
      a.getDomain match {
      case d: Real => contextData += (a.getName -> 1.0d)
      case d: Int => contextData += (a.getName -> 1)
    })
    FmUtil.getFeatureByName(cfm, VARIABLE_PROPERTIES).get.getAttributes.forEach(a =>
      a.getDomain match {
        case d: Real => contextData += (a.getName -> 1.0d)
        case d: Int => contextData += (a.getName -> 1)
    })

    val contextConfig = cfmClass.getCurrentContextConfig(contextData)
    val rootI: Instance = contextConfig.getRoot
    val instances: List[Instance] = ConfigUtil.getAllInstances(contextConfig).toList
    val attrInstances: List[AttrInst] = ConfigUtil.getAttrInstances(contextConfig, rootI).toList

    assert(contextConfig.getFm == cfm)
    assert(rootI != null)
    assert(instances.size == FmNames.allFeaturesAndAttributes.size - FmNames.allAttributes.size - FmNames.allPlacementAlgorithms.size) // only feature nodes, no attributes or placement algos
    assert(rootI.getName == ROOT)
    assert(rootI.getChildren.get(0).getName == SYSTEM)
    assert(rootI.getChildren.get(0).getChildren.size == 1)
    rootI.getChildren.get(0).getChildren.forEach(i => assert(i.getName == MECHANISMS))

    assert(attrInstances.toSet.map((a: AttrInst) => a.getName).subsetOf(FmNames.allAttributes.toSet))
    assert(FmNames.allAttributes.toSet.subsetOf(attrInstances.toSet.map((a: AttrInst) => a.getName) + FmNames.MOBILITY))
    assert(attrInstances.size == FmNames.allAttributes.size - 1) // fcMobility is a feature

  }

  test("CFM context config - incomplete context data defaults to zero attribute values") {

    val cfmClass = new CFM(null)
    val contextData: Map[String, AnyVal] = Map[String, AnyVal]()

    val contextConfig = cfmClass.getCurrentContextConfig(contextData)
    val rootI: Instance = contextConfig.getRoot
    val attrInstances: List[AttrInst] = ConfigUtil.getAttrInstances(contextConfig, rootI).toList

    println(attrInstances.mkString("\n"))
    assert(attrInstances.nonEmpty)
    assert(attrInstances.find(a => a.getName == LOAD_VARIANCE).get.asInstanceOf[RealAttrInst].getVal.equals(0.0))
    assert(attrInstances.find(a => a.getName == NODECOUNT).get.asInstanceOf[IntAttrInst].getVal == 0)
  }

  /*
  test("CFM config update - adding requirement is reflected in the CFM") {

    val cfmClass = new CFM(null)
    val cfm: FM = cfmClass.getFM
    var contextData: Map[String, AnyVal] = Map[String, AnyVal]()
    var qosRequirements: List[Requirement] = List(latency < timespan(50.milliseconds) otherwise Option.empty)

    assert(ConfigUtil.getFeatureInstanceByName(cfmClass.getCurrentContextConfig(contextData, qosRequirements).getRoot, QOS_REQS).getChildren.get(0).getName == LATENCY_REQ)
    assert(ConfigUtil.getFeatureInstanceByName(cfmClass.getCurrentContextConfig(contextData, qosRequirements).getRoot, QOS_REQS).getChildren.get(0).getAttributes.get(0).asInstanceOf[IntAttrInst].getVal == 50)
  }

  test("CFM context config - add then remove requirement") {

    val cfmClass = new CFM(null)
    val cfm: FM = cfmClass.getFM
    var contextData: Map[String, AnyVal] = Map[String, AnyVal]()
    networkSituationAttributeNames.forEach(n => if(!n.equals("fcNodeCount")) contextData += (n ->  1.0d) else contextData += (n -> 3))
    var qosRequirements: List[Requirement] = List()

    val contextConfig = cfmClass.getCurrentContextConfig(contextData, qosRequirements)
    var rootI: Instance = contextConfig.getRoot
    var instances: List[Instance] = ConfigUtil.getAllInstances(contextConfig).toList
    var attrInstances: List[AttrInst] = ConfigUtil.getAttrInstances(contextConfig, rootI).toList
    var instanceNames: List[String] = instances.map(i => i.getName)
    var attrInstanceNames: List[String] = attrInstances.map(a => a.getName)

    attrInstances.foreach {
      case a: RealAttrInst => assert(a.getVal == 1.0d)
      case a: IntAttrInst => assert(a.getVal == 3)
    }
    qosReqAttributeNames.foreach(n => assert(!attrInstanceNames.contains(n)))

    // add requirements
    val latencyRequirement: LatencyRequirement = latency < timespan(100.milliseconds) otherwise Option.empty
    val messageOverheadRequirement: MessageOverheadRequirement = overhead < 5 otherwise Option.empty
    qosRequirements = List(latencyRequirement, messageOverheadRequirement)
    val updatedContextConfig1 = cfmClass.getCurrentContextConfig(contextData, qosRequirements)
    rootI = updatedContextConfig1.getRoot
    instances = ConfigUtil.getAllInstances(updatedContextConfig1).toList
    instanceNames = instances.map(i => i.getName)
    //attrInstances = ConfigUtil.getAttrInstances(updatedContextConfig1, updatedContextConfig1.getRoot).toList
    attrInstances = ConfigUtil.getAttrInstances(updatedContextConfig1, rootI).toList
    attrInstanceNames = attrInstances.map(a => a.getName)

    assert(instanceNames.contains(LATENCY_REQ) && instanceNames.contains(MSGHOPS_REQ))
    assert(attrInstanceNames.contains(LATENCY_REQ_VAL) && attrInstanceNames.contains(MSGHOPS_REQ_VAL))
    assert(attrInstances.find(i => i.getName.equals(LATENCY_REQ_VAL)).get.asInstanceOf[IntAttrInst].getVal == 100)
    assert(attrInstances.find(i => i.getName.equals(MSGHOPS_REQ_VAL)).get.asInstanceOf[IntAttrInst].getVal == 5)


    // remove requirements
    qosRequirements = List()
    val updatedContextConfig2 = cfmClass.getCurrentContextConfig(contextData, qosRequirements)
    rootI = updatedContextConfig2.getRoot
    instances = ConfigUtil.getAllInstances(updatedContextConfig2).toList
    instanceNames = instances.map(i => i.getName)
    attrInstances = ConfigUtil.getAttrInstances(updatedContextConfig2, rootI).toList
    attrInstanceNames = attrInstances.map(a => a.getName)

    assert(!instanceNames.contains(LATENCY_REQ) && !instanceNames.contains(MSGHOPS_REQ))
    assert(!attrInstanceNames.contains(LATENCY_REQ_VAL) && !attrInstanceNames.contains(MSGHOPS_REQ_VAL))
    assert(instances.find(i => i.getName == QOS_REQS).get.getChildren.size == 0)
  }
  */

  test("FM config general - adding and removing an instance on a parent") {

    val cfmClass: CFM = new CFM(null)
    val fm: FM = FmFactory.eINSTANCE.createFM()
    val root: Feature = FmFactoryUtil.createFeature("root", null, 1, 1)
    fm.setRoot(root)
    val config: Config = ConfigFactoryUtil.createConfig(fm)
    val rootI: Instance = ConfigFactoryUtil.createInstance("root", null, root)
    config.setRoot(rootI)

    val fooFeature: Feature = FmFactoryUtil.createFeature("foo", fm.getRoot, 1, 1)
    val foo: Instance = ConfigFactoryUtil.createInstance("foo", config.getRoot, fooFeature)
    assert(ConfigUtil.getFeatureInstanceByName(config.getRoot, "foo") != null)
    assert(ConfigUtil.getFeatureInstanceByName(config.getRoot, "foo").getParent.getName == "root")
    assert(config.getRoot.getChildren.contains(foo))

    val barFeature: Attribute = FmFactoryUtil.createIntAttribute("bar", fooFeature, 0, 10)
    val bar: AttrInst = cfmClass.createAttributeInst(barFeature, foo, 5)

    assert(ConfigUtil.getAttributeInstanceByName(config.getRoot.getChildren.find(i => i.getName == "foo").get , "bar") != null)
    assert(ConfigUtil.getAttributeInstanceByName(config.getRoot.getChildren.find(i => i.getName == "foo").get, "bar").getParent.getName == "foo")
    assert(ConfigUtil.getFeatureInstanceByName(config.getRoot, "foo").getAttributes.size == 1)
    assert(ConfigUtil.getFeatureInstanceByName(config.getRoot, "foo").getAttributes.get(0).getName == "bar")
    assert(ConfigUtil.getFeatureInstanceByName(config.getRoot, "foo").getAttributes.get(0).asInstanceOf[IntAttrInst].getVal == 5)

  }

  /*
  test("ConfigUtil - getAttrInstances") {
    val cfmClass = new CFM(null)
    var contextData: Map[String, AnyVal] = Map[String, AnyVal]()
    FmNames.allAttributes.forEach(n => if(!n.equals("fcNodeCount")) contextData += (n -> 3))
    val qosRequirements: List[Requirement] = List()

    val contextConfig = cfmClass.getCurrentContextConfig(contextData, qosRequirements)
    val rootI: Instance = contextConfig.getRoot
    val instances: List[Instance] = ConfigUtil.getAllInstances(contextConfig).toList
    println("instances: " + instances.toString())

    val attrInstances_ConfigUtil = ConfigUtil.getAttrInstances(contextConfig, rootI).toList
    println("ConfigUtil.getAttrInstances: " )
    attrInstances_ConfigUtil.foreach(i => println(i))

  }*/

  test("CFM - export cfm as xml"){
    val cfmClass = new CFM(null)
    cfmClass.exportCFMAsXML("src/test/resources")
  }

}
