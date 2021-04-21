package tcep.graph.transition.mapek.learnon

import akka.actor.{ActorRef, Address, Cancellable}
import akka.cluster.ClusterEvent.{MemberJoined, MemberLeft}
import akka.cluster.{Member, MemberStatus}
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.Coordinates
import tcep.graph.transition.MAPEK._
import tcep.graph.transition.mapek.contrast.ContrastMAPEK.{GetOperatorTreeDepth, MonitoringDataUpdate}
import tcep.graph.transition.mapek.contrast.ContrastMonitor
import tcep.graph.transition.mapek.lightweight.LightweightMAPEK.GetConsumer
import tcep.graph.transition.{ChangeInNetwork, MonitorComponent}
import tcep.machinenodes.consumers.Consumer.{AllRecords, GetAllRecords}
import tcep.machinenodes.helper.actors.{GetNetworkHopsMap, NetworkHopsMap}
import tcep.utils.TCEPUtils
import tcep.utils.TCEPUtils.{getTaskManagerOfMember, makeMapFuture}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Failure, Success}

class LearnOnMonitor(mapek: LearnOnMAPEK, consumer: ActorRef, fixedSimulationProperties: Map[Symbol, Int]) extends ContrastMonitor(mapek, consumer, fixedSimulationProperties ) {
  /**
    * implementation is identical to ContrastMonitor
    */
}
