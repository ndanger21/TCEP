package tcep

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSelection, Address, Props}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.{ImplicitSender, TestProbe}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.{Coordinates, DistVivaldiActor}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.slf4j.{Logger, LoggerFactory}
import tcep.data.Events.Event1
import tcep.data.Queries.{Filter1, Query, Stream1}
import tcep.machinenodes.helper.actors.{StartVivaldiUpdates, TaskManagerActor}
import tcep.placement.PlacementStrategy
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
  var pNames: Vector[String] = Vector()
  var publishers: Map[String, ActorRef] = Map()
  var clientProbe: TestProbe = _
  implicit var creatorAddress: Address = _
  implicit var ec: ExecutionContext = _
  val errorMargin = 0.05
  val eventIntervalMicros: Long = 500e3.toLong // 500ms
  implicit val baseEventRate: Double = 1e6 / eventIntervalMicros // 2

  override def initialParticipants = numNodes

  override def beforeAll() = {
    multiNodeSpecBeforeAll()
    cluster = Cluster(system)
    creatorAddress = cluster.selfAddress
    ec = system.dispatcher

    runOn(client) {
      system.actorOf(Props(classOf[TaskManagerActor], baseEventRate), "TaskManager")
      DistVivaldiActor.createVivIfNotExists(system)
      clientProbe = TestProbe("client")
      println("Client created: " + node(client))
    }
    runOn(publisher1) {
      // taskmanager for bandwidth measurements, not for hosting operators
      system.actorOf(Props(classOf[TaskManagerActor], baseEventRate), "TaskManager")
      val address = node(publisher1).address
      val pub1 = system.actorOf(Props(RegularPublisher(eventIntervalMicros, id => Event1(id))), s"P:${address.host.get}:${address.port.get}")
      DistVivaldiActor.createVivIfNotExists(system)
      println("created publisher1 " + pub1)
    }
    runOn(publisher2) {
      // taskmanager for bandwidth measurements, not for hosting operators
      system.actorOf(Props(classOf[TaskManagerActor], baseEventRate), "TaskManager")
      val address = node(publisher2).address
      val pub2 = system.actorOf(Props(RegularPublisher(eventIntervalMicros, id => Event1(id))), s"P:${address.host.get}:${address.port.get}")
      DistVivaldiActor.createVivIfNotExists(system)
      println("created publisher2 " + pub2)
    }
    if(numNodes > 3) {
      runOn(node1) {
        system.actorOf(Props(classOf[TaskManagerActor], baseEventRate), "TaskManager")
        DistVivaldiActor.createVivIfNotExists(system)
        println("created node1: " + node(node1))
      }
      runOn(node2) {
        system.actorOf(Props(classOf[TaskManagerActor], baseEventRate), "TaskManager")
        DistVivaldiActor.createVivIfNotExists(system)
        println("created node2: " + node(node2))
      }
    }
    cluster join node(client).address

    var upMembers = 0
    do {
      upMembers = cluster.state.members.count(_.status == MemberStatus.up)
    } while(upMembers < initialParticipants)
    testConductor.enter("wait for startup")

    val p1Addr = node(publisher1).address
    val p2Addr = node(publisher2).address
    pNames = Vector(s"P:${p1Addr.host.get}:${p1Addr.port.get}", s"P:${p2Addr.host.get}:${p2Addr.port.get}")
    val publisherRef1 = getPublisherOn(cluster.state.members.find(_.address == p1Addr).head, pNames(0)).get
    val publisherRef2 = getPublisherOn(cluster.state.members.find(_.address == p2Addr).head, pNames(1)).get
    publishers = Map(pNames(0) -> publisherRef1, pNames(1) -> publisherRef2) // need to override publisher map here because all nodes have hostname localhost during test

    runOn(client) {
      println(cluster.state.members.map(m => m + " " + m.roles).mkString("\n"))
      println(publishers)
      publishers.values.foreach(_ ! StartStreams())
    }
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

}
