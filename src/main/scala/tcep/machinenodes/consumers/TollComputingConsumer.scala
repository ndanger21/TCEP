package tcep.machinenodes.consumers

import tcep.data.Queries
import tcep.data.Queries._
import tcep.dsl.Dsl._
import tcep.machinenodes.consumers.TollComputingConsumer.tollComputingQuery
import tcep.simulation.tcep.{LinearRoadDataNew, StreamDataType}

class TollComputingConsumer extends Consumer {

  val windowSize: Int = 60*3
  val requirement = latency < timespan(800.milliseconds) otherwise None

  override def preStart(): Unit = {
    super.preStart()
    log.info("Starting TollComputingConsumer with requirements: Latency < 5000ms!")
  }

  override def receive: Receive = super.receive

  override def queryFunction(): Queries.Query = tollComputingQuery(this.eventStreams, Set(requirement))
}

object TollComputingConsumer {
  def tollComputingQuery(eventStreams: Seq[Vector[Stream1[_ <: StreamDataType]]], requirements: Set[Requirement]): Query = {
    def streams = eventStreams(0).asInstanceOf[Vector[Stream1[LinearRoadDataNew]]].map(s => Stream1[LinearRoadDataNew](s.publisherName, s.requirements))
    def conjunctions = {
      val and01 = streams(0).and(streams(1))
      val and23 = streams(2).and(streams(3))
      val and45 = streams(4).and(streams(5))
      val and0123 = and01.and(and23)
      val and012345 = and0123.and(and45)
      and012345
    }
    def sectionAvgSpeeds(section: Int) = { //One less than actual sections because last cant generate toll
      val window = SlidingWindow6(conjunctions, Set(), windowSize=5*60, sectionFilter = Some(section))
      NewAverage1(window)
    }
    val observer = ObserveChange6(conjunctions)
    val avg01 = sectionAvgSpeeds(50).and(sectionAvgSpeeds(51))
    val join = avg01.join(observer, slidingWindow(1.seconds), slidingWindow(1.seconds), requirements.toSeq :_*)
    join

    /* OLD QUERY
    val and01 = streams(0).and(streams(1))
    val and23 = streams(2).and(streams(3))
    val and45 = streams(4).and(streams(5))
    val and0123 = and01.and(and23)
    val and012345 = and0123.and(and45)
    val sectionAvgSpeeds = 47 to 52 map (section => { //One less than actual sections because last cant generate toll
      val window = SlidingWindow6(and012345, Set(), windowSize=5*60, sectionFilter = Some(section))
      NewAverage1(window)
    })
    val observer = ObserveChange6(and012345)
    val avg01 = sectionAvgSpeeds(0).and(sectionAvgSpeeds(1))
    val avg23 = sectionAvgSpeeds(2).and(sectionAvgSpeeds(3))
    val avg0123 = avg01.and(avg23)
    val avg01234 = avg0123.and(sectionAvgSpeeds(4))
    val join = avg01234.join(observer, slidingWindow(1.seconds), slidingWindow(1.seconds), requirement)
    join
     */
  }
}
