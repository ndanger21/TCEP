package tcep.transition.mapek

import breeze.stats.meanAndVariance.MeanAndVariance
import org.cardygan.config.util.ConfigUtil
import org.scalatest.FunSuite
import tcep.data.Queries.Query2
import tcep.dsl.Dsl.{query1ToQuery1Helper, stream}
import tcep.graph.qos.OperatorQosMonitor.OperatorQoSMetrics
import tcep.graph.transition.mapek.DynamicCFM
import tcep.machinenodes.qos.BrokerQoSMonitor.{BrokerQosMetrics, IOMetrics}

class DynamicCFMTest extends FunSuite {
  val query: Query2[Int, Float] = stream[Int]("A").and(stream[Float]("B"))

  test("A DynamicCFM should be able to build a FM from a given Query") {
    val cfm = new DynamicCFM(query)
  }

  test("A DynamicCFM should return a correct Config object for a given context sample") {
    val cfm = new DynamicCFM(query)
    val sample = (OperatorQoSMetrics(
      eventSizeIn = List(1), eventSizeOut = 1,
      selectivity = 1,
      interArrivalLatency = MeanAndVariance(1, 1, 1),
      processingLatency = MeanAndVariance(1, 1, 1),
      networkToParentLatency = MeanAndVariance(1, 1, 1),
      endToEndLatency = MeanAndVariance(1, 1, 1),
      ioMetrics = IOMetrics()
    ), BrokerQosMetrics(cpuLoad = 1, cpuThreadCount = 1, deployedOperators = 1, IOMetrics = IOMetrics()))
    val config = cfm.getCurrentContextConfig(sample)
    ConfigUtil.checkConfig(config)
  }
}
