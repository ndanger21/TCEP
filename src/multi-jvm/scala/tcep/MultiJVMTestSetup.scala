package tcep

import akka.actor

import java.util.concurrent.{Executors, TimeUnit}
import akka.actor.{Actor, ActorRef, ActorSelection, Address, PoisonPill, Props, RootActorPath}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.{ImplicitSender, TestProbe}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.{Coordinates, DistVivaldiActor}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.slf4j.{Logger, LoggerFactory}
import tcep.data.Events.Event1
import tcep.data.Queries.{ClientDummyQuery, Filter1, Query, Stream1}
import tcep.graph.QueryGraph
import tcep.graph.nodes.traits.Node.Subscribe
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.transition.StartExecution
import tcep.machinenodes.helper.actors.{SetPublisherActorRefs, SetPublisherActorRefsACK, StartVivaldiUpdates, TaskManagerActor}
import tcep.placement.PlacementStrategy
import tcep.prediction.PredictionHelper.Throughput
import tcep.publishers.Publisher.StartStreams
import tcep.publishers.RegularPublisher
import tcep.utils.TCEPUtils

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * basic setup class for tests that need a Cluster
  * run using sbt multi-jvm:test or sbt multi-jvm:testOnly *classprefix*
  */
abstract class MultiJVMTestSetup(numNodes: Int = 5) extends MultiNodeSpec(config = TCEPMultiNodeConfig)
  with WordSpecLike with Matchers with BeforeAndAfterAll with ImplicitSender {

  import TCEPMultiNodeConfig._

  val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit var cluster: Cluster = _

  def pNames(idx: Int): String = {
    val p1Addr = node(publisher1).address
    val p2Addr = node(publisher2).address
    val pNames = Vector(s"P:${p1Addr.host.get}:${p1Addr.port.get}", s"P:${p2Addr.host.get}:${p2Addr.port.get}")
    pNames(idx)
  }

  var publishers: Map[String, ActorRef] = Map()
  var clientProbe: TestProbe = _
  implicit var creatorAddress: Address = _
  implicit lazy val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  val errorMargin = 0.05
  val eventIntervalMicros: Long = 500e3.toLong // 500ms
  implicit var baseEventRates: Map[String, Throughput] = Map() // 2 per second

  override def initialParticipants = numNodes

  override def beforeAll() = {
    multiNodeSpecBeforeAll()
    cluster = Cluster(system)
    creatorAddress = cluster.selfAddress

    runOn(client) {
      system.actorOf(Props(classOf[TaskManagerActor]), "TaskManager")
      DistVivaldiActor.createVivIfNotExists(system)
      clientProbe = TestProbe("client")
      println("Client created: " + node(client))
    }
    runOn(publisher1) {
      // taskmanager for bandwidth measurements, not for hosting operators
      system.actorOf(Props(classOf[TaskManagerActor]), "TaskManager")
      val address = node(publisher1).address
      val pub1 = system.actorOf(Props(RegularPublisher(eventIntervalMicros, id => Event1(id))), s"P:${address.host.get}:${address.port.get}")
      DistVivaldiActor.createVivIfNotExists(system)
      println("created publisher1 " + pub1)
    }
    runOn(publisher2) {
      // taskmanager for bandwidth measurements, not for hosting operators
      system.actorOf(Props(classOf[TaskManagerActor]), "TaskManager")
      val address = node(publisher2).address
      val pub2 = system.actorOf(Props(RegularPublisher(eventIntervalMicros, id => Event1(id))), s"P:${address.host.get}:${address.port.get}")
      DistVivaldiActor.createVivIfNotExists(system)
      println("created publisher2 " + pub2)
    }
    if (numNodes > 3) {
      runOn(node1) {
        system.actorOf(Props(classOf[TaskManagerActor]), "TaskManager")
        DistVivaldiActor.createVivIfNotExists(system)
        println("created node1: " + node(node1))
      }
      runOn(node2) {
        system.actorOf(Props(classOf[TaskManagerActor]), "TaskManager")
        DistVivaldiActor.createVivIfNotExists(system)
        println("created node2: " + node(node2))
      }
    }
    cluster join node(client).address

    var upMembers = 0
    do {
      upMembers = cluster.state.members.count(_.status == MemberStatus.up)
    } while (upMembers < initialParticipants)
    testConductor.enter("wait for startup")

    val p1Addr = node(publisher1).address
    val p2Addr = node(publisher2).address

    val publisherRef1 = getPublisherOn(cluster.state.members.find(_.address == p1Addr).head, pNames(0)).get
    val publisherRef2 = getPublisherOn(cluster.state.members.find(_.address == p2Addr).head, pNames(1)).get
    publishers = Map(pNames(0) -> publisherRef1, pNames(1) -> publisherRef2) // need to override publisher map here because all nodes have hostname localhost during test
    baseEventRates = List(0, 1).map(pNames).map(_ -> Throughput(1, FiniteDuration(eventIntervalMicros, TimeUnit.MICROSECONDS))).toMap

    runOn(client) {
      println(cluster.state.members.map(m => m + " " + m.roles).mkString("\n"))
      println(s"publishers: \n${publishers.mkString("\n")}")
      println(s"publisher event rates: \n${baseEventRates.mkString("\n")}")

      publishers.values.foreach(_ ! StartStreams())
      cluster.state.members.foreach(m => TCEPUtils.selectTaskManagerOn(cluster, m.address) ! SetPublisherActorRefs(publishers))
      for(i <- 0 until initialParticipants)
        expectMsg(SetPublisherActorRefsACK())
    }
    testConductor.enter("setup complete")
  }

  override def afterAll() = {
    multiNodeSpecAfterAll()
  }

  def startVivaldiCoordinateUpdates: Unit = cluster.state.members.foreach(
    m => TCEPUtils.selectDistVivaldiOn(cluster, m.address) ! StartVivaldiUpdates())

  def setCoordinates(clientCoords: Coordinates, publisher1Coords: Coordinates, publisher2Coords: Coordinates, host1: Coordinates, host2: Coordinates): Unit = {
    runOn(client) {
      DistVivaldiActor.localPos.coordinates = clientCoords
    }
    runOn(publisher1) {
      DistVivaldiActor.localPos.coordinates = publisher1Coords
    }
    runOn(publisher2) {
      DistVivaldiActor.localPos.coordinates = publisher2Coords
    }
    runOn(node1) {
      DistVivaldiActor.localPos.coordinates = host1
    }
    runOn(node2) {
      DistVivaldiActor.localPos.coordinates = host2
    }
    DistVivaldiActor.coordinatesMap = Map()

  }

  def setCoordinatesForPlacement(uut: PlacementStrategy, clientCoords: Coordinates, publisher1Coords: Coordinates, publisher2Coords: Coordinates, host1: Coordinates, host2: Coordinates): Unit = {
    setCoordinates(clientCoords, publisher1Coords, publisher2Coords, host1, host2)
    uut.memberCoordinates = Map()
    val coordUpdate = uut.updateCoordinateMap()
    Await.result(coordUpdate, new FiniteDuration(10, TimeUnit.SECONDS))

  }

  def getMembersWithRole(cluster: Cluster, role: String) = cluster.state.members.filter(m => m.hasRole(role))

  def getPublisherOn(node: Member, name: String)(implicit cluster: Cluster): Option[ActorRef] = {
    val actorName = node.address.toString + s"/user/$name"
    val actorSelection: ActorSelection = cluster.system.actorSelection(actorName)
    val actorResolution: Future[ActorRef] = actorSelection.resolveOne()(Timeout(15, TimeUnit.SECONDS))
    try {
      // using blocking variant here, otherwise return type would have to be future
      val result = Await.result(actorResolution, new FiniteDuration(15, TimeUnit.SECONDS))
      Some(result)
    } catch {
      case e: Throwable =>
        log.error(s"publisher $actorName on ${node} not found \n $e")
        None
    }
  }

  def getTestBDP(mapping: Map[Query, (Member, Coordinates)], client: Coordinates, publisher: Coordinates): Double = {
    val stream = mapping.find(e => e._1.isInstanceOf[Stream1[Int]]).get._1
    val filter = mapping.find(e => e._1.isInstanceOf[Filter1[Int]]).get._1
    val latency1 = publisher.distance(mapping(stream)._2)
    val latency2 = mapping(stream)._2.distance(mapping(filter)._2)
    val latency3 = client.distance(mapping(filter)._2)
    val bw = ConfigFactory.load().getDouble("constants.default-data-rate")
    latency1 * bw + latency2 * bw + latency3 * bw
  }

  def createTestGraph(query: Query, publishers: Map[String, ActorRef], subscriber: TestProbe, placementStrategy: PlacementStrategy, mapek: String = "requirementBased")
                     (implicit cluster: Cluster): ActorRef = {
    try {
      system.actorOf(Props(new Actor {
        var graphFactory: QueryGraph = _
        var rootOperator: ActorRef = _
        override def preStart(): Unit = {
          graphFactory = new QueryGraph(query, TransitionConfig(), publishers, Some(placementStrategy), None, subscriber.ref, mapek)
          rootOperator = Await.result(graphFactory.createAndStart(), FiniteDuration(15, TimeUnit.SECONDS))
          //println(s"instantiated query $query with root operator $rootOperator")
          subscriber.send(rootOperator, Subscribe(subscriber.ref, ClientDummyQuery()))
          subscriber.send(rootOperator, StartExecution(placementStrategy.name))
          //println(s"started TestQueryGraph $graphFactory")
        }

        override def receive: Receive = {
          case GetOperatorMap => sender() ! graphFactory.deployedOperators
          case msg => rootOperator.forward(msg)
        }

        override def postStop(): Unit = {
          rootOperator ! PoisonPill
          super.postStop()
        }
      }), "GraphWrapper")
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        throw e
    }
  }

  def getBrokerQosMonitor(broker: Address)(implicit cluster: Cluster): actor.ActorSelection = cluster.system.actorSelection(RootActorPath(broker) / "user" / "TaskManager*" / "BrokerQosMonitor*")

  object GetOperatorMap
}