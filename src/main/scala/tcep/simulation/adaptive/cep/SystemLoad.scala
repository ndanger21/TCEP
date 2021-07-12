package tcep.simulation.adaptive.cep

import akka.actor.ActorRef
import org.slf4j.LoggerFactory

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicInteger
import javax.management.{Attribute, ObjectName}
import scala.concurrent.duration.FiniteDuration
import scala.language.postfixOps
import scala.sys.process._
/**
  * Created by Raheel on 05/07/2017.
  */
object SystemLoad {
  val log = LoggerFactory.getLogger(getClass)
  val cpuThreadCount: Int = Runtime.getRuntime.availableProcessors()

  @volatile
  var runningOperators: AtomicInteger = new AtomicInteger(0)
  val maxOperators = 100d

  def newOperatorAdded(): Unit = {
    runningOperators.incrementAndGet()
  }

  def operatorRemoved(): Unit = {
    runningOperators.decrementAndGet()
  }

  def isSystemOverloaded(currentLoad: Double, maxLoad: Double): Boolean = {
    currentLoad > maxLoad
  }

  def getSystemLoad(samplingInterval: FiniteDuration = FiniteDuration(1, "second"))(implicit caller: ActorRef): Double = {
    //getCpuUsageByJavaManagementFactory // sometimes unstable results, undefined on some vms
    //getCPUUsageByMpstat(samplingInterval) // provides more fine-grained results, but higher overhead -> can lead to thread starvation on machines with few (<=8 threads)
    getCpuUsageByUnixCommand
  }

  private def getCPUUsageByMpstat(samplingInterval: FiniteDuration): Double = {
    /**
    Linux 5.4.0-77-generic (niels-VBox) 	02.07.2021 	_x86_64_	(16 CPU)
    02:14:44     CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest  %gnice   %idle
    02:14:45     all   16,88    0,00    4,88    0,00    0,00    0,26    0,00    0,00    0,00   77,98
    Average:     all   16,88    0,00    4,88    0,00    0,00    0,26    0,00    0,00    0,00   77,98
      -> get average of %usr
    */
    val loadAvg = s"mpstat ${samplingInterval.toSeconds} 1".!!
    //log.debug("avg load is {}", loadAvg)
    loadAvg.split("\n")(4).split("\\s+")(2).replace(",", ".").toDouble / 100
  }

  // this takes about ~7-10ms!
  private def getCpuUsageByUnixCommand: Double = {
    val loadavg = "cat /proc/loadavg".!!;
    loadavg.split(" ")(0).toDouble / cpuThreadCount
  }

  private def getCpuUsageByJavaManagementFactory(implicit caller: ActorRef): Double = {
    val mbs = ManagementFactory.getPlatformMBeanServer
    val name = ObjectName.getInstance("java.lang:type=OperatingSystem")
    val list = mbs.getAttributes(name, Array("SystemCpuLoad"))
    log.info(s"JAVA MANAGEMENT CPU LOAD: $list, caller: $caller")
    if (!list.isEmpty) {
      list.get(0).asInstanceOf[Attribute].getValue.asInstanceOf[Double]
    } else {
      0
    }
  }
}
