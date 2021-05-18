package tcep.graph

import akka.actor.{ActorContext, ActorRef, Props}
import akka.cluster.Cluster
import akka.pattern.ask
import akka.util.Timeout
import org.discovery.vivaldi.Coordinates
import org.slf4j.LoggerFactory
import tcep.data.Events.Event
import tcep.data.Queries._
import tcep.factories.NodeFactory
import tcep.graph.nodes._
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.transition.MAPEK._
import tcep.graph.transition._
import tcep.machinenodes.consumers.Consumer.SetQosMonitors
import tcep.placement.sbon.PietzuchAlgorithm
import tcep.placement.{HostInfo, PlacementStrategy, QueryDependencies, SpringRelaxationLike}
import tcep.simulation.tcep.GUIConnector
import tcep.utils.SpecialStats

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Created by raheel
  * on 15/08/2017.
  *
  * Extracts the Operator Graph from Base Query
  */
class QueryGraph(query: Query,
                 transitionConfig: TransitionConfig,
                 publishers: Map[String, ActorRef],
                 startingPlacementStrategy: Option[PlacementStrategy],
                 createdCallback: Option[CreatedCallback],
                 consumer: ActorRef,
                 mapekType: String = "requirementBased")
                (implicit val context: ActorContext,
                 implicit val cluster: Cluster,
                 implicit val baseEventRate: Double,
                 implicit val fixedSimulationProperties: Map[Symbol, Int] = Map(),
                 implicit val pimPaths: (String, String) = ("", "")
                 ) {

  lazy val blockingIoDispatcher: ExecutionContext = cluster.system.dispatchers.lookup("blocking-io-dispatcher")
  implicit val timeout = Timeout(15 seconds)
  implicit val ec = context.system.dispatcher
  val log = LoggerFactory.getLogger(getClass)

  var mapek: MAPEK = MAPEK.createMAPEK(mapekType, context, query, transitionConfig, startingPlacementStrategy, consumer, fixedSimulationProperties, pimPaths)
  val placementStrategy: PlacementStrategy = PlacementStrategy.getStrategyByName(Await.result(
    mapek.knowledge ? GetPlacementStrategyName, timeout.duration).asInstanceOf[String])
  var clientNode: ActorRef = _
  val brokerQoSMonitor: ActorRef = Await.result(context.system.actorSelection(context.system./("TaskManager")./("BrokerQosMonitor")).resolveOne(), timeout.duration)

  def createAndStart(eventCallback: Option[EventCallback] = None): ActorRef = {
    log.info(s"Creating and starting new QueryGraph with placement ${
      if (startingPlacementStrategy.isDefined) startingPlacementStrategy.get.name else "default (depends on MAPEK implementation)"} and publishers \n ${publishers.mkString("\n")}")
    val queryDependencies = extractOperators(query, baseEventRate)
    val root = startDeployment(eventCallback, queryDependencies)
    consumer ! SetQosMonitors
    clientNode = context.system.actorOf(Props(classOf[ClientNode], root, mapek, consumer, transitionConfig, queryDependencies(query)._4),
                                        s"ClientNode-${UUID.randomUUID.toString}")
    mapek.knowledge ! SetClient(clientNode)
    mapek.knowledge ! SetTransitionMode(transitionConfig)
    mapek.knowledge ! SetDeploymentStatus(true)
    Thread.sleep(100) // wait a bit here to avoid glitch where last addOperator msg arrives at knowledge AFTER
    // StartExecution msg is sent
    mapek.knowledge ! NotifyOperators(StartExecution(startingPlacementStrategy.getOrElse(PietzuchAlgorithm).name))
    log.info(s"started query ${ query } \n in mode ${ transitionConfig } with PlacementAlgorithm ${placementStrategy.name}")
    root
  }

  protected def startDeployment(implicit eventCallback: Option[EventCallback],
                                queryDependencies: mutable.LinkedHashMap[Query, (QueryDependencies, EventRateEstimate, EventSizeEstimate, EventBandwidthEstimate)]
                               ): ActorRef = {

    val startTime = System.currentTimeMillis()
    val res = for {init <- placementStrategy.initialize( )} yield {
      SpecialStats.log(s"$this", "placement", s"starting initial virtual placement")
      val deploymentComplete: Future[ActorRef] = if (placementStrategy.hasInitialPlacementRoutine()) {
        // some placement algorithms calculate an initial placement with global knowledge for all operators,
        // instead of calculating the optimal node one after another
        val initialOperatorPlacementRequest = placementStrategy.asInstanceOf[SpringRelaxationLike]
                                                               .initialVirtualOperatorPlacement(query, publishers)
        initialOperatorPlacementRequest.onComplete {
          case Success(value) => SpecialStats.log(s"$this", "placement",s"initial deployment virtual placement took ${System.currentTimeMillis() - startTime}ms")
          case Failure(exception) => SpecialStats.log(s"$this", "placement",s"initial deployment virtual placement failed after" + s" ${System.currentTimeMillis() - startTime}ms, cause: \n $exception")
        }
        for {initialOperatorPlacement <- initialOperatorPlacementRequest
             deployment <- deployOperatorGraphRec(query, true)(eventCallback, queryDependencies, initialOperatorPlacement)} yield {
          deployment
        }
      } else {
        deployOperatorGraphRec(query, true)
      }
      deploymentComplete
    }
    // block here to wait until deployment is finished
    val rootOperator = Await.result(res.flatten, new FiniteDuration(120, TimeUnit.SECONDS))
    SpecialStats.log(s"$this", "placement", s"initial deployment took ${ System.currentTimeMillis() - startTime }ms")
    rootOperator
  }

  protected def deployOperatorGraphRec(currentOperator: Query, isRootOperator: Boolean = false)
                                      (implicit eventCallback: Option[EventCallback], queryDependencies: mutable.LinkedHashMap[Query, (QueryDependencies, EventRateEstimate, EventSizeEstimate, EventBandwidthEstimate)],
                                       initialPlacement: Map[Query, Coordinates] = Map()): Future[ActorRef] = {

    implicit val _isRoot: Boolean = isRootOperator
    val rootOperator: Future[ActorRef] = currentOperator match {
      case op: StreamQuery => deployOperator(op, (publishers(op.publisherName), PublisherDummyQuery(op.publisherName)))

      case op: SequenceQuery => deployOperator(op, (publishers(op.s1.publisherName), PublisherDummyQuery(op.s1.publisherName)), (publishers(op.s2.publisherName), PublisherDummyQuery(op.s2.publisherName)))

      case op: UnaryQuery => deployOperatorGraphRec(op.sq) map { parentDeployment => deployOperator(op, (parentDeployment, op.sq)) } flatten

      case op: BinaryQuery =>
        // declare outside for comprehension so that futures start in parallel
        val parent1Deployment = deployOperatorGraphRec(op.sq1)
        val parent2Deployment = deployOperatorGraphRec(op.sq2)
        val deployment: Future[Future[ActorRef]] = for {parent1 <- parent1Deployment
                                                        parent2 <- parent2Deployment} yield {
          deployOperator(op, (parent1, op.sq1), (parent2, op.sq2))
        }
        deployment.flatten

      case other => throw new RuntimeException(s"unknown query type! $other")
    }
    rootOperator
  }

  protected def deployOperator(operator: Query, parentOperators: (ActorRef, Query)*)
                              (implicit eventCallback: Option[EventCallback],
                               isRootOperator: Boolean,
                               initialOperatorPlacement: Map[Query, Coordinates],
                               queryDependencies: mutable.LinkedHashMap[Query, (QueryDependencies, EventRateEstimate, EventSizeEstimate, EventBandwidthEstimate)]
                              ): Future[ActorRef] = {
    // child is not deployed yet -> None as placeholder
    val dependencies = Dependencies(parentOperators.toMap, Map(None -> queryDependencies(operator)._1.child.get))
    val reliabilityReqPresent = (query.requirements collect { case r: ReliabilityRequirement => r }).nonEmpty
    SpecialStats.log(s"$this", "placement", s"deploying operator $operator in mode $transitionConfig with placement strategy ${placementStrategy.name}")

    val deployment: Future[(ActorRef, HostInfo)] = {
      if (placementStrategy.hasInitialPlacementRoutine() && initialOperatorPlacement.contains(operator)) {
        for {hostInfo <- placementStrategy.asInstanceOf[SpringRelaxationLike]
                                          .findHost(initialOperatorPlacement(operator), Map(), operator, dependencies, queryDependencies.mapValues(_._4).toMap)
             deployedOperator <- createOperator(operator, hostInfo, false, None, parentOperators.map(_._1): _ *) } yield {
          if (reliabilityReqPresent) { // for now, start duplicate on self (just like relaxation does in this case, see findOptimalNodes())
            val backupDeployment = createOperator(operator, HostInfo(cluster.selfMember, operator, hostInfo.operatorMetrics) , true,
                                                  Some(deployedOperator), parentOperators.map(_._1): _*)
            backupDeployment.foreach { backup => mapek.knowledge ! AddBackupOperator(backup) }
          }
          (deployedOperator, hostInfo)
        }

      } else { // no initial placement or operator is missing
          for {hostInfo <- placementStrategy.findOptimalNode(operator, dependencies, HostInfo(cluster.selfMember, operator))
               deployedOperator <- createOperator(operator, hostInfo, false, None, parentOperators.map(_._1 ): _ *)} yield {
            if (reliabilityReqPresent) {
            // for now, start duplicate on self (just like relaxation does in this case)
            val backupDeployment = createOperator(operator, HostInfo(cluster.selfMember, operator, hostInfo.operatorMetrics), true, None, parentOperators.map(_._1): _*)
            backupDeployment.foreach { backup => mapek.knowledge ! AddBackupOperator(backup) }
            }
            (deployedOperator, hostInfo)
          }
      }
    }
    deployment map { opAndHostInfo => {
      SpecialStats.log(s"$this", "placement", s"deployed ${ opAndHostInfo._1 } on; ${opAndHostInfo._2.member} ; with " +
        s"hostInfo ${opAndHostInfo._2.operatorMetrics}; parents: $parentOperators; path.name: ${parentOperators.map(_._1).head.path.name}")
      mapek.knowledge ! AddOperator(opAndHostInfo._1)

      GUIConnector.sendInitialOperator(opAndHostInfo._2.member.address, placementStrategy.name,
                                       opAndHostInfo._1.path.name, s"$transitionConfig", parentOperators.map(_._1),
                                       opAndHostInfo._2, isRootOperator)(selfAddress = cluster.selfAddress, ec)
      opAndHostInfo._1
    }}
  }

  protected def createOperator(operator: Query,
                               hostInfo: HostInfo,
                               backupMode: Boolean,
                               mainNode: Option[ActorRef],
                               parentOperators: ActorRef*)
                              (implicit eventCallback: Option[EventCallback], isRootOperator: Boolean, baseEventRate: Double): Future[ActorRef] = {
    val operatorType = NodeFactory.getOperatorTypeFromQuery(operator)
    val props = Props(operatorType, transitionConfig, hostInfo, backupMode, mainNode, operator, createdCallback,
                      eventCallback, isRootOperator, baseEventRate, parentOperators)
    NodeFactory.createOperator(cluster, context, hostInfo, props, brokerQoSMonitor)
  }

  def stop(): Unit = {
    mapek.stop()
    clientNode ! ShutDown()
  }

  def getPlacementStrategy(): Future[String] = {
    (mapek.knowledge ? GetPlacementStrategyName).mapTo[String]
  }

  def addDemand(demand: Seq[Requirement]): Unit = {
    log.info("Requirements changed. Notifying Monitor")
    mapek.monitor ! AddRequirement(demand)
  }

  def removeDemand(demand: Seq[Requirement]): Unit = {
    log.info("Requirements changed. Notifying Monitor")
    mapek.monitor ! RemoveRequirement(demand)
  }

  def manualTransition(algorithmName: String): Unit = {
    log.info(s"Manual transition request to algorithm $algorithmName")
    mapek.planner ! ManualTransition(algorithmName)
  }
}

//Closures are not serializable so callbacks would need to be wrapped in a class
abstract class CreatedCallback() {

  def apply(): Any
}

abstract class EventCallback() {

  def apply(event: Event): Any
}