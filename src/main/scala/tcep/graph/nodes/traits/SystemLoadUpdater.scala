package tcep.graph.nodes.traits

import akka.actor.{Actor, ActorLogging, Timers}
import akka.pattern.pipe
import com.typesafe.config.ConfigFactory
import tcep.machinenodes.qos.BrokerQoSMonitor.{CPULoadUpdate, CPULoadUpdateTick, CPULoadUpdateTickKey}
import tcep.simulation.adaptive.cep.SystemLoad

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/**
  * trait for actors that need to access the system load value periodically
  * updates every second
  */
class SystemLoadUpdater extends Actor with Timers with ActorLogging {

  var currentLoad: Double = 0.0
  val samplingInterval: FiniteDuration = FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.MILLISECONDS)
  //val singleThreadDispatcher: ExecutionContext = scala.concurrent.ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  lazy val blockingIoDispatcher: ExecutionContext = context.system.dispatchers.lookup("blocking-io-dispatcher")
  override def preStart(): Unit = {
    super.preStart()
    timers.startTimerWithFixedDelay(CPULoadUpdateTickKey, CPULoadUpdateTick, samplingInterval) // use fixed delay here to avoid starting multiple futures at once
  }

  override def receive: Receive = {
    case CPULoadUpdateTick =>
      log.debug("received CPULoadUpdateTick")
      implicit val ec = blockingIoDispatcher
      val update = Future {
        val start = System.nanoTime()
        val load = CPULoadUpdate(SystemLoad.getSystemLoad(samplingInterval)(self))
        val now = System.nanoTime()
        log.debug("currentLoad update took {}ms", (now - start) / 1e6)
        load
      }
      update.pipeTo(self)

    case load: CPULoadUpdate =>
      context.parent ! load
  }

}
