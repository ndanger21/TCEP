package tcep

import akka.actor.{Actor, ActorContext, ActorRef, ActorSystem, Address, PoisonPill, Props}
import akka.cluster.Cluster
import akka.serialization.{SerializationExtension, Serializers}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.Coordinates
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike}
import org.slf4j.LoggerFactory
import tcep.data.Events._
import tcep.data.Queries
import tcep.data.Queries._
import tcep.dsl.Dsl._
import tcep.graph.nodes.traits.Node.Subscribe
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.transition.MAPEK.{AddOperator, SetClient, SetDeploymentStatus, SetTransitionMode}
import tcep.graph.transition.{AcknowledgeStart, StartExecution}
import tcep.graph.{CreatedCallback, EventCallback, QueryGraph}
import tcep.machinenodes.helper.actors.TaskManagerActor
import tcep.placement.{HostInfo, MobilityTolerantAlgorithm}
import tcep.prediction.PredictionHelper.Throughput
import tcep.publishers.Publisher.AcknowledgeSubscription
import tcep.publishers.TestPublisher

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.runtime.universe.typeOf

class GraphTests extends TestKit(ActorSystem("testSystem", ConfigFactory.parseString("akka.test.single-expect-default = 5000").withFallback(ConfigFactory.load()))) with FunSuiteLike with BeforeAndAfterAll with BeforeAndAfterEach {
  val log = LoggerFactory.getLogger(getClass)
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val creatorAddress: Address = Address("tcp", "tcep", "localhost", 0)
  var actors: List[ActorRef] = List()
  var subscriber: TestProbe = _ // use a TestProbe instead of testActor so that we have an empty mailbox for each test (-> no false failures due to previous failed tests)
  def subAckMsg(implicit query: Query) = AcknowledgeSubscription(query)
  var rootOperator: ActorRef = _
  val defaultThroughput = Throughput(1, FiniteDuration(1, TimeUnit.SECONDS))
  implicit val baseEventRates: Map[String, Throughput] = Map("A" -> defaultThroughput, "B" -> defaultThroughput, "C" -> defaultThroughput)

  def createTestPublisher(name: String): ActorRef = {
    val pub = system.actorOf(Props(TestPublisher()), name)
    actors = pub :: actors
    pub
  }

  case class EventPublishedCallback() extends EventCallback {
    override def apply(event: Event): Any = {
      println("received event" + event)
    }
  }

  // we need to override some functions here to not run the actual placement algorithms with akka cluster (would have to use multiJvmTesting for this, unnecessary overhead)
  class TestQueryGraph(query: Query,
                       subscriberActor: ActorRef,
                       transitionConfig: TransitionConfig,
                       publishers: Map[String, ActorRef],
                       startingPlacementStrategy: Option[String],
                       createdCallback: Option[CreatedCallback],
                       consumer: ActorRef = null)
                      (implicit override val context: ActorContext,
                       override implicit val cluster: Cluster,
                       baseEventRates: Map[String, Queries.EventRateEstimate]
                      )
    extends QueryGraph(query, transitionConfig, publishers, startingPlacementStrategy, createdCallback, consumer) {

    override def createAndStart(eventCallback: Option[EventCallback]): Future[ActorRef] = {
      val queryDependencies = extractOperatorsAndThroughputEstimates(query)
      for {
        root <- startDeployment(eventCallback, queryDependencies)
      } yield {
        mapek.knowledge ! SetClient(subscriberActor)
        mapek.knowledge ! SetTransitionMode(transitionConfig)
        mapek.knowledge ! SetDeploymentStatus(true)
        actors = root :: actors
        root
      }
    }

    protected override def startDeployment(implicit eventCallback: Option[EventCallback],
                                           queryDependencies: QueryDependencyMap
                                          ): Future[ActorRef] = deployOperatorGraphRec(query, true)

    protected override def deployOperator(operator: Query, parentOperators: (ActorRef, Query)*)
                                         (implicit eventCallback: Option[EventCallback],
                                          isRootOperator: Boolean,
                                          initialOperatorPlacement: Map[Query, Coordinates],
                                          queryDependencies: QueryDependencyMap
                                         ): Future[ActorRef] = {

      for { operatorActor <- createOperator(operator, HostInfo(cluster.selfMember, operator), false, None, parentOperators.map(_._1):_*)}
        yield {
          //println(s"deploy: created $operator with created callback $createdCallback: $operatorActor")
          mapek.knowledge ! AddOperator((operator, operatorActor))
          operatorActor
        }
    }
  }

  def createTestGraph(query: Query, publishers: Map[String, ActorRef], subscriberActor: ActorRef): ActorRef = {

    val wrapper = system.actorOf(Props(new Actor {
      implicit val cluster: Cluster = Cluster(context.system)
      try {
        val graphFactory: TestQueryGraph = new TestQueryGraph(query, subscriberActor, TransitionConfig(), publishers, None, None)
        log.debug("Created TestQueryGraph")
        rootOperator = Await.result(graphFactory.createAndStart(None), FiniteDuration(15, TimeUnit.SECONDS))
        log.debug(s"instantiated query $query with root operator $rootOperator")
        subscriber.send(rootOperator, Subscribe(subscriber.ref, ClientDummyQuery()))
        subscriber.send(rootOperator, StartExecution(MobilityTolerantAlgorithm.name))

      } catch {
        case e: Throwable => println(e.getMessage + "\n" + e.getStackTrace.mkString("\n"))
      }

      override def receive: Receive = {
        case msg => rootOperator.forward(msg)
      }
      override def postStop(): Unit = {
        rootOperator ! PoisonPill
        super.postStop()
      }
    }), "GraphWrapper")
    actors = wrapper :: actors
    wrapper
  }


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

  override def beforeAll(): Unit = {
    super.beforeAll()
    system.actorOf(Props(classOf[TaskManagerActor]), "TaskManager")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    subscriber = TestProbe()
  }

  override def afterEach(): Unit = {
    actors.foreach(system.stop)
    actors = List()
    system.stop(subscriber.ref)
    super.afterEach()
  }

  override def afterAll(): Unit = {
    system.terminate()
  }

  test(" extractOperators() must return a map of all operators and their assorted parent and child operators that exist in a query") {
      val query =
        stream[Boolean]("A")
          .and(stream[Boolean]("B"))
          .and(stream[Boolean]("C").where(_ == true))

      val result = Queries.extractOperatorsAndThroughputEstimates(query)
      assert(result.nonEmpty)
      assert(result.size == 10, "result must contain all 6 operators, 3 publisher dummy operators, and 1 ClientDummyOperator")
      assert(result.count(o => o._1.isInstanceOf[Stream1[_]]) == 3, "result must contain 3 stream operators")
      assert(result.count(o => o._1.isInstanceOf[Filter1[_]]) == 1, "result must contain one filter operator")
      assert(result.count(o => o._1.isInstanceOf[Conjunction21[_, _, _]]) == 1,
        "result must contain one conjunction21 operator")
      assert(result.count(o => o._1.isInstanceOf[Conjunction11[_, _]]) == 1,
        "result must contain one conjunction11 operator")
      assert(result.filter(o => o._1.isInstanceOf[Stream1[_]]).forall(o =>
        o._2._1.parents.isDefined && o._2._1.parents.get.nonEmpty && o._2._1.parents.get.head.isInstanceOf[PublisherDummyQuery]),
        "stream operators must have dummy operator parents")
      assert(result.exists(o => o._1.isInstanceOf[Conjunction21[_, _, _]] && o._2._1.child.isDefined && o._2._1.child.get.isInstanceOf[ClientDummyQuery]),
        "conjunction21 operator is the root of the query and has no children")
      assert(result.exists(o => o._1.isInstanceOf[Conjunction21[_, _, _]] &&
        o._2._1.parents.get.exists(p => p.isInstanceOf[Filter1[_]]) &&
        o._2._1.parents.get.exists(p => p.isInstanceOf[Conjunction11[_, _]])),
        "conjunction21 operator has filter and conjunction11 as parents")
      assert(result.exists(o => o._1.isInstanceOf[ClientDummyQuery] && o._2._1.parents.isDefined && o._2._1.parents.get.head == query && o._2._1.child.isEmpty))
    }

    test("Query should be serializable when sent between actors") {
      val serialization = SerializationExtension(system)
      val query = Stream3[String, Int, Boolean]("STREAM3", Set())
      val serializedBytes = serialization.serialize(query).get
      val serializerId = serialization.findSerializerFor(query).identifier
      val manifest = Serializers.manifestFor(serialization.findSerializerFor(query), query)
      val deserialized = serialization.deserialize(serializedBytes, serializerId, manifest).get

      assert(deserialized.equals(query), "deserialized query must equal original")
      assert(deserialized.asInstanceOf[StreamQuery].types == Vector(typeOf[String].toString, typeOf[Int].toString, typeOf[Boolean].toString),
             "deserialized query must contain correct type name strings")
    }

    test("LeafNode - StreamNode - 1") {
      val a: ActorRef = createTestPublisher("A")
      implicit val query: Query1[String] = stream[String]("A")
      val graph: ActorRef = createTestGraph(query, Map("A" -> a), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event1("42")
      subscriber.expectMsg(Event1("42"))
    }

    test("LeafNode - StreamNode - 2") {
      val a: ActorRef = createTestPublisher("A")
      implicit val query: Query2[Int, Int] = stream[Int, Int]("A")
      val graph: ActorRef = createTestGraph(query, Map("A" -> a), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event2(42, 42)
      subscriber.expectMsg(Event2(42, 42))
    }

    test("LeafNode - StreamNode - 3") {
      val a: ActorRef = createTestPublisher("A")
      implicit val query: Query3[Long, Long, Long] = stream[Long, Long, Long]("A")
      val graph: ActorRef = createTestGraph(query, Map("A" -> a), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event3(42l, 42l, 42l)
      subscriber.expectMsg(Event3(42l, 42l, 42l))
    }

    test("LeafNode - StreamNode - 4") {
      val a: ActorRef = createTestPublisher("A")
      implicit val query: Query4[Float, Float, Float, Float] = stream[Float, Float, Float, Float]("A")
      val graph: ActorRef = createTestGraph(query, Map("A" -> a), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event4(42f, 42f, 42f, 42f)
      subscriber.expectMsg(Event4(42f, 42f, 42f, 42f))
    }

    test("LeafNode - StreamNode - 5") {
      val a: ActorRef = createTestPublisher("A")
      implicit val query: Query5[Double, Double, Double, Double, Double] = stream[Double, Double, Double, Double, Double]("A")
      val graph: ActorRef = createTestGraph(query, Map("A" -> a), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event5(42.0, 42.0, 42.0, 42.0, 42.0)
      subscriber.expectMsg(Event5(42.0, 42.0, 42.0, 42.0, 42.0))
    }

    test("LeafNode - StreamNode - 6") {
      val a: ActorRef = createTestPublisher("A")
      implicit val query: Query6[Boolean, Boolean, Boolean, Boolean, Boolean, Boolean] = stream[Boolean, Boolean, Boolean, Boolean, Boolean, Boolean]("A")
      val graph: ActorRef = createTestGraph(query, Map("A" -> a), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event6(true, true, true, true, true, true)
      subscriber.expectMsg(Event6(true, true, true, true, true, true))
    }

  test("LeafNode - SequenceNode - 1") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    implicit val query: Query4[Int, Int, String, String] =
      sequence(nStream[Int, Int]("A") -> nStream[String, String]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), subscriber.ref)
    subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
    a ! Event2(21, 42).updateDepartureTimestamp(1, defaultThroughput)
    Thread.sleep(50)
    b ! Event2("21", "42").updateDepartureTimestamp(1, defaultThroughput)
    subscriber.expectMsg(Event4(21, 42, "21", "42"))
  }

  test("LeafNode - SequenceNode - 2") {
    val a: ActorRef = createTestPublisher("A")
    val b: ActorRef = createTestPublisher("B")
    implicit val query: Query4[Int, Int, String, String] =
      sequence(nStream[Int, Int]("A") -> nStream[String, String]("B"))
    val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), subscriber.ref)
    subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
    a ! Event2(1, 1).updateDepartureTimestamp(1, defaultThroughput)
    Thread.sleep(50)
    a ! Event2(2, 2).updateDepartureTimestamp(1, defaultThroughput)
    Thread.sleep(50)
    a ! Event2(3, 3).updateDepartureTimestamp(1, defaultThroughput)
    Thread.sleep(50)
    b ! Event2("1", "1").updateDepartureTimestamp(1, defaultThroughput)
    Thread.sleep(50)
    b ! Event2("2", "2").updateDepartureTimestamp(1, defaultThroughput)
    Thread.sleep(50)
    b ! Event2("3", "3").updateDepartureTimestamp(1, defaultThroughput)
    subscriber.expectMsg(Event4(1, 1, "1", "1"))
  }

    test("UnaryNode - FilterNode - 1") {
      val a: ActorRef = createTestPublisher("A")
      implicit val query: Query2[Int, Int] =
        stream[Int, Int]("A")
          .where(_ >= _)
      val graph: ActorRef = createTestGraph(query, Map("A" -> a), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event2(41, 42)
      a ! Event2(42, 42)
      a ! Event2(43, 42)
      subscriber.expectMsg(Event2(42, 42))
      subscriber.expectMsg(Event2(43, 42))
    }

    test("UnaryNode - FilterNode - 2") {
      val a: ActorRef = createTestPublisher("A")
      implicit val query: Query2[Int, Int] =
        stream[Int, Int]("A")
          .where(_ <= _)
      val graph: ActorRef = createTestGraph(query, Map("A" -> a), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event2(41, 42)
      a ! Event2(42, 42)
      a ! Event2(43, 42)
      val x = subscriber.expectMsg(Event2(41, 42))
      val y = subscriber.expectMsg(Event2(42, 42))
      subscriber.expectNoMessage()
    }

    test("UnaryNode - FilterNode - 3") {
      val a: ActorRef = createTestPublisher("A")
      implicit val query: Query1[Long] =
        stream[Long]("A")
          .where(_ == 42l)
      val graph: ActorRef = createTestGraph(query, Map("A" -> a), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event1(41l)
      a ! Event1(42l)
      subscriber.expectMsg(Event1(42l))
    }

    test("UnaryNode - FilterNode - 4") {
      val a: ActorRef = createTestPublisher("A")
      implicit val query: Query1[Float] =
        stream[Float]("A")
          .where(_ > 41f)
      val graph: ActorRef = createTestGraph(query, Map("A" -> a), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event1(41f)
      a ! Event1(42f)
      subscriber.expectMsg(Event1(42f))
    }

    test("UnaryNode - FilterNode - 5") {
      val a: ActorRef = createTestPublisher("A")
      implicit val query: Query1[Double] =
        stream[Double]("A")
          .where(_ < 42.0)
      val graph: ActorRef = createTestGraph(query, Map("A" -> a), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event1(41.0)
      a ! Event1(42.0)
      subscriber.expectMsg(Event1(41.0))
    }

    test("UnaryNode - FilterNode - 6") {
      val a: ActorRef = createTestPublisher("A")
      implicit val query: Query =
        stream[Boolean]("A")
          .where(_ != true)
      val graph: ActorRef = createTestGraph(query, Map("A" -> a), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event1(true)
      a ! Event1(false)
      subscriber.expectMsg(Event1(false))
    }

    test("UnaryNode - DropElemNode - 1") {
      val a: ActorRef = createTestPublisher("A")
      implicit val query: Query1[Int] =
        stream[Int, Int]("A")
          .dropElem2()
      val graph: ActorRef = createTestGraph(query, Map("A" -> a), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event2(21, 42)
      a ! Event2(42, 21)
      subscriber.expectMsg(Event1(21))
      subscriber.expectMsg(Event1(42))
    }

    test("UnaryNode - DropElemNode - 2") {
      val a: ActorRef = createTestPublisher("A")
      implicit val query: Query2[String, String] =
        stream[String, String, String, String]("A")
          .dropElem1()
          .dropElem2()
      val graph: ActorRef = createTestGraph(query, Map("A" -> a), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event4("a", "b", "c", "d")
      a ! Event4("e", "f", "g", "h")
      subscriber.expectMsg(Event2("b", "d"))
      subscriber.expectMsg(Event2("f", "h"))
    }

    test("UnaryNode - SelfJoinNode - 1") {
      val a: ActorRef = createTestPublisher("A")
      implicit val query: Query4[String, String, String, String] =
        stream[String, String]("A")
          .selfJoin(tumblingWindow(3.instances), tumblingWindow(2.instances))
      val graph: ActorRef = createTestGraph(query, Map("A" -> a), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event2("a", "b")
      a ! Event2("c", "d")
      a ! Event2("e", "f")
      subscriber.expectMsg(Event4("a", "b", "a", "b"))
      subscriber.expectMsg(Event4("a", "b", "c", "d"))
      subscriber.expectMsg(Event4("c", "d", "a", "b"))
      subscriber.expectMsg(Event4("c", "d", "c", "d"))
      subscriber.expectMsg(Event4("e", "f", "a", "b"))
      subscriber.expectMsg(Event4("e", "f", "c", "d"))
    }

    test("UnaryNode - SelfJoinNode - 2") {
      val a: ActorRef = createTestPublisher("A")
      implicit val query: Query4[String, String, String, String] =
        stream[String, String]("A")
          .selfJoin(slidingWindow(3.instances), slidingWindow(2.instances))
      val graph: ActorRef = createTestGraph(query, Map("A" -> a), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event2("a", "b")
      a ! Event2("c", "d")
      a ! Event2("e", "f")
      subscriber.expectMsg(Event4("a", "b", "a", "b"))
      subscriber.expectMsg(Event4("c", "d", "a", "b"))
      subscriber.expectMsg(Event4("c", "d", "c", "d"))
      subscriber.expectMsg(Event4("a", "b", "c", "d"))
      subscriber.expectMsg(Event4("e", "f", "c", "d"))
      subscriber.expectMsg(Event4("e", "f", "e", "f"))
      subscriber.expectMsg(Event4("a", "b", "e", "f"))
      subscriber.expectMsg(Event4("c", "d", "e", "f"))
    }

    test("BinaryNode - JoinNode - 1") {
      val a: ActorRef = createTestPublisher("A")
      val b: ActorRef = createTestPublisher("B")
      val sq: Query2[Int, Int] = stream[Int, Int]("B")
      implicit val query: Query5[String, Boolean, String, Int, Int] =
        stream[String, Boolean, String]("A")
          .join(sq, tumblingWindow(3.instances), tumblingWindow(2.instances))

      val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event3("a", true, "b")
      a ! Event3("c", true, "d")
      a ! Event3("e", true, "f")
      a ! Event3("g", true, "h")
      a ! Event3("i", true, "j")
      Thread.sleep(50)
      b ! Event2(1, 2)
      b ! Event2(3, 4)
      b ! Event2(5, 6)
      b ! Event2(7, 8)
      subscriber.expectMsg(Event5("a", true, "b", 1, 2))
      subscriber.expectMsg(Event5("c", true, "d", 1, 2))
      subscriber.expectMsg(Event5("e", true, "f", 1, 2))
      subscriber.expectMsg(Event5("a", true, "b", 3, 4))
      subscriber.expectMsg(Event5("c", true, "d", 3, 4))
      subscriber.expectMsg(Event5("e", true, "f", 3, 4))
      subscriber.expectMsg(Event5("a", true, "b", 5, 6))
      subscriber.expectMsg(Event5("c", true, "d", 5, 6))
      subscriber.expectMsg(Event5("e", true, "f", 5, 6))
      subscriber.expectMsg(Event5("a", true, "b", 7, 8))
      subscriber.expectMsg(Event5("c", true, "d", 7, 8))
      subscriber.expectMsg(Event5("e", true, "f", 7, 8))
    }

    test("BinaryNode - JoinNode - 2") {
      val a: ActorRef = createTestPublisher("A")
      val b: ActorRef = createTestPublisher("B")
      val sq: Query2[Int, Int] = stream[Int, Int]("B")
      implicit val query: Query5[String, Boolean, String, Int, Int] =
        stream[String, Boolean, String]("A")
          .join(sq, tumblingWindow(3.instances), tumblingWindow(2.instances))
      val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      b ! Event2(1, 2)
      b ! Event2(3, 4)
      b ! Event2(5, 6)
      b ! Event2(7, 8)
      Thread.sleep(50)
      a ! Event3("a", true, "b")
      a ! Event3("c", true, "d")
      a ! Event3("e", true, "f")
      a ! Event3("g", true, "h")
      a ! Event3("i", true, "j")
      subscriber.expectMsg(Event5("a", true, "b", 5, 6))
      subscriber.expectMsg(Event5("a", true, "b", 7, 8))
      subscriber.expectMsg(Event5("c", true, "d", 5, 6))
      subscriber.expectMsg(Event5("c", true, "d", 7, 8))
      subscriber.expectMsg(Event5("e", true, "f", 5, 6))
      subscriber.expectMsg(Event5("e", true, "f", 7, 8))
    }

    test("BinaryNode - JoinNode - 3") {
      val a: ActorRef = createTestPublisher("A")
      val b: ActorRef = createTestPublisher("B")
      val sq: Query2[Int, Int] = stream[Int, Int]("B")
      implicit val query: Query5[String, Boolean, String, Int, Int] =
        stream[String, Boolean, String]("A")
          .join(sq, slidingWindow(3.instances), slidingWindow(2.instances))
      val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event3("a", true, "b")
      a ! Event3("c", true, "d")
      a ! Event3("e", true, "f")
      a ! Event3("g", true, "h")
      a ! Event3("i", true, "j")
      Thread.sleep(50)
      b ! Event2(1, 2)
      b ! Event2(3, 4)
      b ! Event2(5, 6)
      b ! Event2(7, 8)
      subscriber.expectMsg(Event5("e", true, "f", 1, 2))
      subscriber.expectMsg(Event5("g", true, "h", 1, 2))
      subscriber.expectMsg(Event5("i", true, "j", 1, 2))
      subscriber.expectMsg(Event5("e", true, "f", 3, 4))
      subscriber.expectMsg(Event5("g", true, "h", 3, 4))
      subscriber.expectMsg(Event5("i", true, "j", 3, 4))
      subscriber.expectMsg(Event5("e", true, "f", 5, 6))
      subscriber.expectMsg(Event5("g", true, "h", 5, 6))
      subscriber.expectMsg(Event5("i", true, "j", 5, 6))
      subscriber.expectMsg(Event5("e", true, "f", 7, 8))
      subscriber.expectMsg(Event5("g", true, "h", 7, 8))
      subscriber.expectMsg(Event5("i", true, "j", 7, 8))
    }

    test("BinaryNode - JoinNode - 4") {
      val a: ActorRef = createTestPublisher("A")
      val b: ActorRef = createTestPublisher("B")
      val sq: Query2[Int, Int] = stream[Int, Int]("B")
      implicit val query: Query5[String, Boolean, String, Int, Int] =
        stream[String, Boolean, String]("A")
          .join(sq, slidingWindow(3.instances), slidingWindow(2.instances))
      val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      b ! Event2(1, 2)
      b ! Event2(3, 4)
      b ! Event2(5, 6)
      b ! Event2(7, 8)
      Thread.sleep(50)
      a ! Event3("a", true, "b")
      a ! Event3("c", true, "d")
      a ! Event3("e", true, "f")
      a ! Event3("g", true, "h")
      a ! Event3("i", true, "j")
      subscriber.expectMsg(Event5("a", true, "b", 5, 6))
      subscriber.expectMsg(Event5("a", true, "b", 7, 8))
      subscriber.expectMsg(Event5("c", true, "d", 5, 6))
      subscriber.expectMsg(Event5("c", true, "d", 7, 8))
      subscriber.expectMsg(Event5("e", true, "f", 5, 6))
      subscriber.expectMsg(Event5("e", true, "f", 7, 8))
      subscriber.expectMsg(Event5("g", true, "h", 5, 6))
      subscriber.expectMsg(Event5("g", true, "h", 7, 8))
      subscriber.expectMsg(Event5("i", true, "j", 5, 6))
      subscriber.expectMsg(Event5("i", true, "j", 7, 8))
    }

    test("Binary Node - ConjunctionNode - 1") {
      val a: ActorRef = createTestPublisher("A")
      val b: ActorRef = createTestPublisher("B")
      implicit val query: Query2[Int, Float] =
        stream[Int]("A")
          .and(stream[Float]("B"))
      val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), subscriber.ref)
      val starts = subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      log.info(s"STARTED $starts")
      a ! Event1(21)
      b ! Event1(21.0f)
      Thread.sleep(50)
      a ! Event1(42)
      b ! Event1(42.0f)
      subscriber.expectMsg(Event2(21, 21.0f))
      subscriber.expectMsg(Event2(42, 42.0f))
    }

    test("Binary Node - ConjunctionNode - 2") {
      val a: ActorRef = createTestPublisher("A")
      val b: ActorRef = createTestPublisher("B")
      implicit val query: Query2[Int, Float] =
        stream[Int]("A")
          .and(stream[Float]("B"))
      val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event1(21)
      a ! Event1(42)
      Thread.sleep(50)
      b ! Event1(21.0f)
      b ! Event1(42.0f)
      subscriber.expectMsg(Event2(21, 21.0f))
    }

    test("Binary Node - DisjunctionNode - 1") {
      val a: ActorRef = createTestPublisher("A")
      val b: ActorRef = createTestPublisher("B")
      implicit val query: Query2[Either[Int, String], Either[Int, String]] =
        stream[Int, Int]("A")
          .or(stream[String, String]("B"))
      val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event2(21, 42)
      Thread.sleep(50)
      b ! Event2("21", "42")
      subscriber.expectMsg(Event2(Left(21), Left(42)))
      subscriber.expectMsg(Event2(Right("21"), Right("42")))
    }

    test("Binary Node - DisjunctionNode - 2") {
      val a: ActorRef = createTestPublisher("A")
      val b: ActorRef = createTestPublisher("B")
      val c: ActorRef = createTestPublisher("C")
      implicit val query:
        Query3[Either[Either[Int, String], Boolean],
          Either[Either[Int, String], Boolean],
          Either[Unit, Boolean]] =
        stream[Int, Int]("A")
          .or(stream[String, String]("B"))
          .or(stream[Boolean, Boolean, Boolean]("C"))
      val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b, "C" -> c), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event2(21, 42)
      Thread.sleep(50)
      b ! Event2("21", "42")
      Thread.sleep(50)
      c ! Event3(true, false, true)
      subscriber.expectMsg(Event3(Left(Left(21)), Left(Left(42)), Left(())))
      subscriber.expectMsg(Event3(Left(Right("21")), Left(Right("42")), Left(())))
      subscriber.expectMsg(Event3(Right(true), Right(false), Right(true)))
    }

    test("Nested - SP operators") {
      val a: ActorRef = createTestPublisher("A")
      val b: ActorRef = createTestPublisher("B")
      val c: ActorRef = createTestPublisher("C")
      val sq1: Query2[String, String] = stream[String, String]("A")
      val sq2: Query2[Int, Int] = stream[Int, Int]("B")
      val sq3: Query1[String] = stream[String]("C")
      val sq4: Query4[String, String, Int, Int] =
        sq1.join(sq2, tumblingWindow(3.instances), tumblingWindow(2.instances))
      val sq5: Query2[String, String] =
        sq3.selfJoin(tumblingWindow(3.instances), tumblingWindow(2.instances))
      val sq6: Query6[String, String, Int, Int, String, String] =
        sq4.join(sq5, tumblingWindow(1.instances), tumblingWindow(4.instances))
      val sq7: Query6[String, String, Int, Int, String, String] =
        sq6.where((_, _, e3, e4, _, _) => e3 < e4)
      implicit val query: Query2[String, String] =
        sq7
          .dropElem2()
          .dropElem2()
          .dropElem2()
          .dropElem2()
      val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b, "C" -> c), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      b ! Event2(1, 2)
      b ! Event2(3, 4)
      b ! Event2(5, 6)
      b ! Event2(7, 8)
      Thread.sleep(50)
      a ! Event2("a", "b")
      a ! Event2("c", "d")
      a ! Event2("e", "f")
      a ! Event2("g", "h")
      a ! Event2("i", "j")
      Thread.sleep(50)
      c ! Event1("a")
      c ! Event1("b")
      c ! Event1("c")
      subscriber.expectMsg(Event2("e", "a"))
      subscriber.expectMsg(Event2("e", "b"))
      subscriber.expectMsg(Event2("e", "a"))
      subscriber.expectMsg(Event2("e", "b"))
    }

    test("Nested - CEP operators") {
      val a: ActorRef = createTestPublisher("A")
      val b: ActorRef = createTestPublisher("B")
      val c: ActorRef = createTestPublisher("C")
      implicit val query: Query2[Either[Int, Float], Either[Float, Boolean]] =
        stream[Int]("A")
          .and(stream[Float]("B"))
          .or(sequence(nStream[Float]("B") -> nStream[Boolean]("C")))
      val graph: ActorRef = createTestGraph(query, Map("A" -> a, "B" -> b, "C" -> c), subscriber.ref)
      subscriber.expectMsgAllOf(subAckMsg, AcknowledgeStart())
      a ! Event1(21)
      a ! Event1(42)
      Thread.sleep(50)
      b ! Event1(21.0f)
      b ! Event1(42.0f)
      Thread.sleep(50)
      c ! Event1(true)
      subscriber.expectMsg(Event2(Left(21), Left(21.0f)))
      subscriber.expectMsg(Event2(Right(21.0f), Right(true)))
    }

}
