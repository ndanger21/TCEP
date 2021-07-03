package tcep.graph.nodes

import akka.actor.{ActorLogging, ActorRef, PoisonPill, Props, Timers}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.DistVivaldiActor
import tcep.ClusterActor
import tcep.data.Events
import tcep.data.Events._
import tcep.data.Queries.ClientDummyQuery
import tcep.graph.nodes.traits.Node.{OperatorMigrationNotice, Subscribe}
import tcep.graph.nodes.traits.{TransitionConfig, TransitionModeNames}
import tcep.graph.qos.OperatorQosMonitor
import tcep.graph.qos.OperatorQosMonitor.UpdateEventRateOut
import tcep.graph.transition.MAPEK.{SetLastTransitionStats, SetTransitionStatus, UpdateLatency}
import tcep.graph.transition.{MAPEK, SuccessorStart, TransferredState, TransitionRequest}
import tcep.machinenodes.consumers.Consumer.SetStatus
import tcep.machinenodes.helper.actors.{ACK, PlacementMessage}
import tcep.machinenodes.qos.BrokerQoSMonitor.{CPULoad, CPULoadUpdateTick, CPULoadUpdateTickKey, GetCPULoad}
import tcep.placement.{HostInfo, OperatorMetrics}
import tcep.prediction.PredictionHelper.Throughput
import tcep.publishers.Publisher.AcknowledgeSubscription
import tcep.simulation.tcep.GUIConnector
import tcep.utils.{SizeEstimator, TCEPUtils}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

/**
  * Receives the final output of the query
  *
  **/

class ClientNode(var rootOperator: ActorRef, mapek: MAPEK, var consumer: ActorRef = null, transitionConfig: TransitionConfig, rootOperatorBandwidthEstimate: Double) extends ClusterActor with ActorLogging with Timers {
  val samplingInterval: FiniteDuration = FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.MILLISECONDS)
  var hostInfo: HostInfo = _
  var transitionStartTime: Long = _   //Not in transition
  var transitionStatus: Int = 0
  var guiBDPUpdateSent = false
  implicit val timeout = Timeout(5 seconds)
  var eventRateOut: Throughput = Throughput(0, FiniteDuration(1, TimeUnit.SECONDS))
  var eventSizeOut: Long = 0
  val operatorQoSMonitor: ActorRef = context.actorOf(Props(classOf[OperatorQosMonitor], ClientDummyQuery(), self, mapek.monitor), "operatorQosMonitor")
  var currentCPULoad = 0.0d

  override def receive: Receive = {
    case CPULoadUpdateTick => TCEPUtils.selectTaskManagerOn(cluster, cluster.selfAddress) ! GetCPULoad
    case CPULoad(load) => currentCPULoad = load
    case UpdateEventRateOut(rate) => eventRateOut = rate
    case TransitionRequest(strategy, requester, stats, placement) => {
      sender() ! ACK()
      if(transitionStatus == 0) {
        log.info(s"Transiting system to ${strategy}")
        transitionLog(s"transiting to ${strategy}")
        mapek.knowledge ! SetTransitionStatus(1)
        transitionStartTime = System.currentTimeMillis()
        TCEPUtils.guaranteedDelivery(context, rootOperator, TransitionRequest(strategy, self, stats, placement), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))(blockingIoDispatcher)
      }
    }

    case TransferredState(_, replacement, oldParent, stats, _, _) => {
      sender() ! ACK()
      // log transition time of parent
      val transitionStart: Long = stats.transitionTimesPerOperator.getOrElse(oldParent, 0)
      val transitionDuration = System.currentTimeMillis() - transitionStart
      val opmap = stats.transitionTimesPerOperator.updated(oldParent, transitionDuration)
      log.info(s"parent $oldParent \n transition start: $transitionStart, transitionDuration: ${transitionDuration} , operator stats: \n ${stats.transitionTimesPerOperator.mkString("\n")}")
      val updatedStats =  updateTransitionStats(stats, oldParent, transferredStateSize(oldParent), updatedOpMap = Some(opmap) )
      mapek.knowledge ! SetTransitionStatus(0)
      mapek.knowledge ! SetLastTransitionStats(updatedStats)
      if(transitionConfig.transitionStrategy == TransitionModeNames.NaiveStopMoveStart)
        replacement ! SuccessorStart

      if(transitionStatus != 0) {
        val timetaken = System.currentTimeMillis() - transitionStartTime
        log.info(s"replacing operator node after $timetaken ms \n $rootOperator \n with replacement $replacement")
        for { rootPos <- TCEPUtils.getCoordinatesOfNode(cluster, rootOperator.path.address) } yield {
          val bdp = 0.001 * rootOperatorBandwidthEstimate * DistVivaldiActor.localPos.coordinates.distance(rootPos) // dist in [ms], bandwidth in [Bytes / s]
          replaceOperator(replacement, bdp)
        transitionLog(s"transition complete after $timetaken ms (from $oldParent to $replacement \n\n")
          GUIConnector.sendTransitionTimeUpdate(timetaken.toDouble / 1000)
        }
      }
    }

    case OperatorMigrationNotice(oldOperator, newOperator) => { // received from migrating parent (oldOperator)
      this.replaceOperator(newOperator, 1.0 ) // migration is not used atm
      log.info(s"received operator migration notice from $oldOperator, \n new operator is $newOperator")
    }

    case event: Event if sender().equals(rootOperator) && hostInfo != null => {
      event.updateArrivalTimestamp()
      //SpecialStats.log(s"$this", "clientNodeEvents", s"received event $event from ${sender()}")
      //Events.printEvent(event, log)
      //val arrival = System.nanoTime()
      val e2eLatency = System.currentTimeMillis() - event.monitoringData.creationTimestamp
      if(!guiBDPUpdateSent) {
        GUIConnector.sendBDPUpdate(event.monitoringData.networkUsage.sum, DistVivaldiActor.getLatencyValues())(cluster.selfAddress, blockingIoDispatcher) // this is the total BDP of the entire graph
        guiBDPUpdateSent = true
        eventSizeOut = SizeEstimator.estimate(event)
        log.info(s"$this", s"hostInfo after update: ${hostInfo.operatorMetrics}")
      }
      Events.updateMonitoringData(log, event, hostInfo, currentCPULoad, eventRateOut, eventSizeOut)

      consumer ! event
      operatorQoSMonitor ! event
      //monitors.foreach(monitor => monitor.onEventEmit(event, transitionStatus))
      //val now = System.nanoTime()
      mapek.knowledge ! UpdateLatency(e2eLatency)

    }

    case event: Event if !sender().equals(rootOperator) =>
      log.info(s"received event from unknown sender ${sender().path.name} my parent is ${rootOperator.path.name}")
      //Events.printEvent(event, log)

    case SetTransitionStatus(status) =>
      this.transitionStatus = status
      this.consumer ! SetStatus(status)

    case ShutDown() => {
      rootOperator ! ShutDown()
      log.info(s"Stopping self as received shut down message from ${sender().path.name}, forwarding it to $rootOperator")
      self ! PoisonPill
      //this.consumer ! PoisonPill
    }

    case _ =>
  }

  /**
    * replace parent operator reference so that new events are recognized correctly
    * @param replacement replacement ActorRef
    * @return
    */
  private def replaceOperator(replacement: ActorRef, bdpToOperator: Double) = {
      hostInfo = HostInfo(this.cluster.selfMember, ClientDummyQuery(), OperatorMetrics(Map(rootOperator.path.address -> bdpToOperator)))
      this.rootOperator = replacement
      guiBDPUpdateSent = false // total (accumulated) bdp of the entire operator graph is updated when the first event arrives
    }


  override def preStart(): Unit = {
    super.preStart()
    timers.startTimerWithFixedDelay(CPULoadUpdateTickKey, CPULoadUpdateTick, samplingInterval)
    implicit val ec = blockingIoDispatcher
      log.info(s"Subscribing for events from ${rootOperator.path.name}")
    // subscribe to root operator and update HostInfo with BDP to it
    for {
      ack <- (rootOperator ? Subscribe(self, ClientDummyQuery())).mapTo[AcknowledgeSubscription]
      rootPos <- {
        log.info(s"subscribed for events from rootoperator ${rootOperator} ${ack.acknowledgingParent}")
        TCEPUtils.getCoordinatesOfNode(cluster, rootOperator.path.address)(ec)
      }
    } yield {
      val dist = rootPos.distance(DistVivaldiActor.localPos.coordinates)
      val bdpToOperator = dist * rootOperatorBandwidthEstimate * 0.001 // dist in [ms], bandwidth in [Bytes / s]
      hostInfo = HostInfo(this.cluster.selfMember, ClientDummyQuery(), OperatorMetrics(Map(rootOperator.path.address -> bdpToOperator)))
      log.info(s"bdp on last hop: ${bdpToOperator} Bytes")
    }
  }


  override def postStop(): Unit = {
    super.postStop()
    log.info("Stopping self")
  }

}

case class ShutDown() extends PlacementMessage