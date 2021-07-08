package tcep.machinenodes.consumers
import akka.event.LoggingAdapter
import tcep.data.Queries
import tcep.data.Queries.{Stream1, _}
import tcep.data.Structures.MachineLoad
import tcep.dsl.Dsl._
import tcep.machinenodes.consumers.AnalysisConsumer.{adAnalysisQuery, loadStorageDatabase}
import tcep.simulation.tcep.{StreamDataType, YahooDataNew}

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.io.Source

class AnalysisConsumer extends Consumer {

  var storage : HashMap[Int, Int] = HashMap[Int, Int]()
  val latencyRequirement = latency < timespan(700.milliseconds) otherwise None
  val loadRequirement = load < MachineLoad(3.0d) otherwise None
  override def preStart(): Unit = {
    super.preStart()
    storage = loadStorageDatabase()(log)
  }

  override def receive: Receive = super.receive

  override def queryFunction(): Queries.Query = adAnalysisQuery(eventStreams, storage, Set(latencyRequirement, loadRequirement))
}

object AnalysisConsumer {
  def adAnalysisQuery(eventStreams: Seq[Vector[Stream1[_ <: StreamDataType]]], storage: HashMap[Int, Int], requirements: Set[Requirement]): Query = {
    val streams = eventStreams(0).asInstanceOf[Vector[Stream1[YahooDataNew]]]

    val and01 = streams(0).and(streams(1))
    val and23 = streams(2).and(streams(3))
    val and45 = streams(4).and(streams(5))
    val and67 = streams(6).and(streams(7))
    val and0123 = and01.and(and23)
    val and4567 = and45.and(and67)
    val db0123 = DatabaseJoin4(and0123, Set(), storage)
    val db4567 = DatabaseJoin4(and4567, Set(), storage)
    val dbAnd01234567 = db0123.and(db4567)
    val purchaseFilter = ShrinkFilter2(dbAnd01234567, Set(), (event: Any) => {
      event.asInstanceOf[YahooDataNew].eventType == 2
    }, emitAlways = Some(false))
    val stats = WindowStatistic1(purchaseFilter, requirements, 60)
    //val stats = WindowStatistic1(purchaseFilter, Set(latencyRequirement), 60)
    stats
  }

  def loadStorageDatabase()(implicit log: LoggingAdapter): HashMap[Int, Int] = {
    val dbFile = s"/app/event_traces/yahooJoins.csv"
    try {
      val storage = mutable.HashMap[Int, Int]()
      val bufferedSource = Source.fromFile(dbFile)
      var headerSkipped = false
      for (line <- bufferedSource.getLines()){
        if (!headerSkipped)
          headerSkipped = true
        else {
          val cols = line.split(",").map(_.trim)
          val adId = cols(0).toInt
          val campId = cols(1).toInt
          storage += (adId -> campId)
        }
      }
      bufferedSource.close()
      storage
    } catch {
      case e: Throwable =>
        log.error(s"error while creating database in consumer from file: $dbFile: {}", e)
        throw e
    }
  }
}
