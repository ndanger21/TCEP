package tcep.graph.transition.mapek.contrast

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.testkit.{TestKit, TestProbe}
import org.cardygan.config.Config
import org.cardygan.config.util.ConfigUtil
import org.cardygan.fm.FM
import org.cardygan.fm.impl.IntImpl
import org.cardygan.fm.util.FmUtil
import org.coala.event.MilpSolverFinishedEvent
import org.coala.model.PerformanceInfluenceModel
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}
import tcep.data.Queries.{MessageHopsRequirement, Requirement}
import tcep.dsl.Dsl._
import tcep.graph.transition.mapek.contrast.FmNames._
import tcep.graph.transition.mapek.contrast.TestUtils._

import scala.collection.JavaConverters._


/**
  * Created by Niels on 24.02.2018.
  */
class PlannerComponentTests extends TestKit(ActorSystem()) with FunSuiteLike with BeforeAndAfterAll with MockitoSugar {

  test("Planner - Coala test different contexts -> different optimal config") {

    val messageHopsRequirement: MessageHopsRequirement = hops < 3 otherwise Option.empty
    val qosRequirements: Set[Requirement] = Set(messageHopsRequirement)
    val cfmClass: CFM = new CFM()
    val cfm: FM = cfmClass.getFM
    val hopsModel = PlannerHelper.extractModelFromFile(cfm, "/performanceModels/simpleModel.log").get
    val contextData = generateMaxedContextData(cfmClass)
    val contextConfigMin = cfmClass.getCurrentContextConfig(contextData = contextData.updated(LINK_CHANGES, 0))
    val contextConfigMax = cfmClass.getCurrentContextConfig(contextData)

    ConfigUtil.getAttrInstances(contextConfigMin, contextConfigMin.getRoot).asScala
      .foreach(attrInst => assert(CFM.isInBounds(attrInst)))
    val optimalSystemConfigMin: Option[Config] = PlannerHelper.calculateOptimalSystemConfig(cfmClass, contextConfigMin, hopsModel, qosRequirements)
    val optimalSystemConfigMax: Option[Config] = PlannerHelper.calculateOptimalSystemConfig(cfmClass, contextConfigMax, hopsModel, qosRequirements)
    val optimalAlgorithmMin = ConfigUtil.getFeatureInstanceByName(optimalSystemConfigMin.get.getRoot, PLACEMENT_ALGORITHM).getChildren.asScala.toList
    val optimalAlgorithmMax = ConfigUtil.getFeatureInstanceByName(optimalSystemConfigMax.get.getRoot, PLACEMENT_ALGORITHM).getChildren.asScala.toList
    assert(optimalAlgorithmMin.head.getName == STARKS)
    assert(optimalAlgorithmMax.head.getName == RELAXATION)
  }

  test("Planner - Coala objective value test") {

    val qosRequirements: Set[Requirement] = Set()
    val cfmClass: CFM = new CFM()
    val cfm: FM = cfmClass.getFM
    val hopsModel = PlannerHelper.extractModelFromFile(cfm, "/performanceModels/simpleModel.log").get
    val contextData = generateMaxedContextData(cfmClass)
    val contextConfigMin = cfmClass.getCurrentContextConfig(contextData = contextData.updated(LINK_CHANGES, 0))
    val contextConfigMax = cfmClass.getCurrentContextConfig(contextData = contextData.updated(LINK_CHANGES, 12))
    ConfigUtil.getAttrInstances(contextConfigMin, contextConfigMin.getRoot).asScala
      .foreach(attrInst => assert(CFM.isInBounds(attrInst), s"${attrInst.getName} is out of bounds"))

    var objMin = 0.0
    var objMax = 0.0
    val listenerMin = new MyListener() {
      override def solverFinished(solverEvent: MilpSolverFinishedEvent) = {
        objMin = solverEvent.getObjectiveValue.get()
      }
    }
    val listenerMax = new MyListener() {
      override def solverFinished(solverEvent: MilpSolverFinishedEvent) = {
        objMax = solverEvent.getObjectiveValue.get()
      }
    }

    val optimalSystemConfigMin: Option[Config] =
      PlannerHelper.calculateOptimalSystemConfig(cfmClass, contextConfigMin, hopsModel, qosRequirements, listenerMin)
    val optimalSystemConfigMax: Option[Config] =
      PlannerHelper.calculateOptimalSystemConfig(cfmClass, contextConfigMax, hopsModel, qosRequirements, listenerMax)

    println(s"objective value min context config: $objMin")
    println(s"objective value max context config: $objMax")

    val nodeUpperBound = FmUtil.findAttributeByName(cfm, FmNames.NODECOUNT).get.getDomain.asInstanceOf[IntImpl].getBounds.getUb
    //simple model: 5 * root + 1 * fsMDCEP * fcBaseLatency + 1.0 * fsRelaxation * fcNodeCount + 1001 * fsRandom + 1111 * fsProducerConsumer
    assert(-1 * objMin == 5)
    assert(-1 * objMax == 5 + 1 * nodeUpperBound)
  }

  test("Planner - Coala minimal test - MobilityTolerant expected") {

    val cfmClass = new CFM()
    val cfm = cfmClass.getFM
    val contextData = TestUtils.generateMaxedContextData(cfmClass)
    val requirements: Set[Requirement] = Set(latency < timespan(100.milliseconds) otherwise Option.empty)

    val config: Config = cfmClass.getCurrentContextConfig(contextData)
    assert(config != null)
    //println("loaded context configuration: ")
    //printConfig(config)

    val filepath = "/performanceModels/simpleModel.log"
    val model: PerformanceInfluenceModel = PlannerHelper.extractModelFromFile(cfm, filepath).get
    assert(model != null)
    //println("influence model terms: ")
    //println(model.getTerms.toString)

    val optimalSystemConfiguration: Config = PlannerHelper.calculateOptimalSystemConfig(cfmClass, config, model, requirements).get
    //println("optimal system config")
    //println(CFM.configToString(optimalSystemConfiguration))
    assert(ConfigUtil.getFeatureInstanceByName(optimalSystemConfiguration.getRoot, PLACEMENT_ALGORITHM).getChildren.size == 1)
    assert(ConfigUtil.getFeatureInstanceByName(optimalSystemConfiguration.getRoot, PLACEMENT_ALGORITHM).getChildren.get(0).getName == RELAXATION)

  }
  /*
  test("Coala test with msgHopsModel - MobilityTolerant expected") {

    val planner = createTestPlannerComponent
    val cfmClass = new CFM(mock[ContrastMAPEK])
    val cfm = cfmClass.getFM
    val contextData = generateRandomContextData(cfmClass)

    val requirements: Set[Requirement] = Set(latency < timespan(100.milliseconds) otherwise Option.empty)
    val config: Config = cfmClass.getCurrentContextConfig(contextData)
    assert(config != null)
    val filepath = "/performanceModels/msgHopsModel.log"

    implicit val timeout1 = Timeout(3, duration.SECONDS)
    val modelRequest: Future[Any] = Patterns.ask(planner, TestExtractModelFromFile(cfm, filepath), timeout1)
    val model: Option[PerformanceInfluenceModel] = Await.result(modelRequest, timeout1.duration).asInstanceOf[Option[PerformanceInfluenceModel]]
    assert(model.isDefined)
    assert(model.get.getTerms.size > 0)
    //assert(model.getTerms.size == 2)
    println("influence model terms: ")
    println(model.get.getTerms.toString)

    implicit val timeout2 = Timeout(30, duration.SECONDS)
    val configRequest: Future[Any] = Patterns.ask(planner, TestCalculateOptimalSystemConfig(cfmClass, config, model.get, requirements), timeout2)
    val optimalSystemConfiguration: Option[Config] = Await.result(configRequest, timeout2.duration).asInstanceOf[Option[Config]]

    println(optimalSystemConfiguration.toString)
    assert(optimalSystemConfiguration.isDefined)
    println("optimal attributes: ")
    ConfigUtil.getAttrInstances(optimalSystemConfiguration.get, optimalSystemConfiguration.get.getRoot).forEach(a => println(a))
    println("optimal features: ")
    ConfigUtil.getAllInstances(optimalSystemConfiguration.get).forEach(i => println(i))

    assert(ConfigUtil.getFeatureInstanceByName(optimalSystemConfiguration.get.getRoot, PLACEMENT_ALGORITHM).getChildren.size == 1)
    assert(ConfigUtil.getFeatureInstanceByName(optimalSystemConfiguration.get.getRoot, PLACEMENT_ALGORITHM).getChildren.get(0).getName == MOBILITY_TOLERANT)

    stopActor(planner)
  }

  test("Planner - test performance influence model loading") {

    val filepath = "/performanceModels/simpleModel.log"
    val cfm = new CFM(mock[ContrastMAPEK]).getFM
    val planner = createTestPlannerComponent

    implicit val timeout = Timeout(5, duration.SECONDS)
    val modelRequest: Future[Any] = Patterns.ask(planner, TestExtractModelFromFile(cfm, filepath), timeout)
    val model: PerformanceInfluenceModel = Await.result(modelRequest, timeout.duration).asInstanceOf[Option[PerformanceInfluenceModel]].get

    assert(model != null)
    println(model.getTerms.asScala.map(t => t.toString + "\n"))
    assert(model.getTerms.size() > 0)

    stopActor(planner)
  }

  def createTestPlannerComponent: ActorRef = {

    val mapek = mock[TestMAPEK]
    system.actorOf(Props(new TestPlannerComponent(mapek)))
  }
  */
  // The following method implementation is taken straight out of the Akka docs:
  // http://doc.akka.io/docs/akka/current/scala/testing.html#Watching_Other_Actors_from_Probes
  def stopActor(actor: ActorRef): Unit = {
    val probe = TestProbe()
    probe watch actor
    actor ! PoisonPill
    probe.expectTerminated(actor)
  }

  def stopActors(actors: ActorRef*): Unit = {
    actors.foreach(stopActor)
  }

  override def afterAll(): Unit = {
    system.terminate()
  }

  def printConfig(config: Config): Unit = {
    ConfigUtil.getAllInstances(config).forEach(i => println(i))
    ConfigUtil.getAttrInstances(config, ConfigUtil.getFeatureInstanceByName(config.getRoot, NETSITUATION)).forEach(a => println(a))
    //ConfigUtil.getFeatureInstanceByName(config.getRoot, QOS_REQS).getChildren.forEach(i => println(i.getAttributes.get(0)))
  }
}
