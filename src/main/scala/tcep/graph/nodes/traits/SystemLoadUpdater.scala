package tcep.graph.nodes.traits

import akka.actor.{Actor, ActorLogging, Timers}
import akka.pattern.pipe
import com.typesafe.config.ConfigFactory
import tcep.machinenodes.helper.actors.{MeasurementMessage, Message}
import tcep.simulation.adaptive.cep.SystemLoad

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/**
  * trait for actors that need to access the system load value periodically
  * updates every second
  */
trait SystemLoadUpdater extends Actor with Timers with ActorLogging {

  private val blockingIoDispatcher: ExecutionContext = context.system.dispatchers.lookup("blocking-io-dispatcher")
  var currentLoad: Double = 0.0
  val samplingInterval: FiniteDuration = FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.MILLISECONDS)

  override def preStart(): Unit = {
    super.preStart()
    timers.startTimerWithFixedDelay(CPULoadUpdateTickKey, CPULoadUpdateTick, samplingInterval)
  }

  override def receive: Receive = {
    case CPULoadUpdateTick =>
      //val start = System.nanoTime()
      implicit val ec = blockingIoDispatcher
      Future(CPULoadUpdate(SystemLoad.getSystemLoad)) pipeTo self
      //val now = System.nanoTime()
      //log.info(s"currentLoad update took ${now - start}ns")
    case CPULoadUpdate(load) => currentLoad = load
  }

  private case class CPULoadUpdate(load: Double) extends MeasurementMessage
  private case object CPULoadUpdateTick
  private case object CPULoadUpdateTickKey
}
