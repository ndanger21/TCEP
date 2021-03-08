package tcep.graph.nodes.traits

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, Cancellable}
import tcep.simulation.adaptive.cep.SystemLoad

import scala.concurrent.duration.FiniteDuration

/**
  * trait for actors that need to access the system load value periodically
  * updates every second
  */
trait SystemLoadUpdater extends Actor with ActorLogging {
  var currentLoad: Double = 0.0
  private var updateLoadTick: Cancellable = _
  private val blockingIoDispatcher = context.system.dispatchers.lookup("blocking-io-dispatcher")

  override def preStart(): Unit = {
    super.preStart()
    updateLoadTick = context.system.scheduler.scheduleWithFixedDelay(FiniteDuration(1, TimeUnit.SECONDS), FiniteDuration(1, TimeUnit.SECONDS))(() => {
      //val start = System.nanoTime()
      currentLoad = SystemLoad.getSystemLoad
      //val now = System.nanoTime()
      //log.info(s"currentLoad update took ${now - start}ns")
    })(blockingIoDispatcher)
  }

  override def postStop(): Unit = {
    updateLoadTick.cancel()
    super.postStop()
  }
}
