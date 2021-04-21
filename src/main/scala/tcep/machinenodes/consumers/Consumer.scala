package tcep.machinenodes.consumers

import akka.actor.ActorLogging
import tcep.data.Events.Event
import tcep.data.Queries.{Query, Stream1}
import tcep.dsl.Dsl.Measurement
import tcep.graph.qos._
import tcep.machinenodes.consumers.Consumer._
import tcep.machinenodes.helper.actors.Message
import tcep.placement.vivaldi.VivaldiCoordinates
import tcep.simulation.tcep.{RecordAverageLoad, RecordFrequency, RecordLatency, RecordMessageHops, RecordMessageOverhead, RecordNetworkUsage, RecordProcessingNodes, RecordPublishingRate, RecordTransitionStatus}

abstract class Consumer extends VivaldiCoordinates with ActorLogging {

  lazy val query: Query = queryFunction()
  var allRecords: AllRecords = AllRecords()
  var monitorFactories: Array[MonitorFactory] = Array.empty[MonitorFactory]
  var monitors: Array[Monitor] = Array.empty[Monitor]
  var transitionStatus: Int = 0
  var eventStreams: Seq[Vector[Stream1[Any]]] = _

  override def postStop(): Unit = {
    log.info(s"stopping self $self")
    super.postStop()
  }

  override def receive: Receive = super.receive orElse {
    case GetQuery =>
      log.info("Creating Query")
      sender() ! this.query
      log.info(s"Query is: ${this.query.toString}")

    case GetAllRecords =>
      log.info(s"${sender()} asked for AllRecords, eventrate out/in: ${allRecords.recordPublishingRate.lastRateMeasurement} ${allRecords.recordFrequency.lastMeasurement}   load: ${allRecords.recordAverageLoad.lastLoadMeasurement}")
      sender() ! this.allRecords

    case GetMonitorFactories =>
      sender() ! this.monitorFactories

    case GetRequirementChange =>
      sender() ! Set.empty

    case event: Event =>
      log.debug(s"$self received EVENT: $event")
      this.monitors.foreach(monitor => monitor.onEventEmit(event, this.transitionStatus))

    case SetStatus(status) =>
      log.info(s"transition status changed to $status")
      this.transitionStatus = status

    case SetQosMonitors =>
      if(monitorFactories.isEmpty) {
        log.info(s"${self} Setting QoS Monitors")
        val latencyMonitorFactory = PathLatencyMonitorFactory(query, Some(allRecords.recordLatency))
        val hopsMonitorFactory = MessageHopsMonitorFactory(query, Some(allRecords.recordMessageHops), allRecords.recordProcessingNodes)
        val loadMonitorFactory = LoadMonitorFactory(query, Some(allRecords.recordAverageLoad))
        val frequencyMonitorFactory = AverageFrequencyMonitorFactory(query, Some(allRecords.recordFrequency))
        val recordTransitionStatusFactory = TransitionMonitorFactory(query, allRecords.recordTransitionStatus)
        val messageOverheadMonitorFactory = MessageOverheadMonitorFactory(query, allRecords.recordMessageOverhead)
        val networkUsageMonitorFactory = NetworkUsageMonitorFactory(query, allRecords.recordNetworkUsage)
        val publishingRateMonitorFactory = PublishingRateMonitorFactory(query, allRecords.recordPublishingRate)

        monitorFactories = Array(latencyMonitorFactory, hopsMonitorFactory,
          loadMonitorFactory, frequencyMonitorFactory, messageOverheadMonitorFactory, networkUsageMonitorFactory,
          recordTransitionStatusFactory, publishingRateMonitorFactory)
        this.monitors = monitorFactories.map(f => f.createNodeMonitor)
        //log.info(s"Monitors set: ${this.monitors.toString}")
        log.info(s"Setting QoS Monitors set. AllRecords are: ${this.allRecords.getValues}")
      }
    case SetStreams(streams) =>
      this.eventStreams = streams.asInstanceOf[Seq[Vector[Stream1[Any]]]]
      log.info("Event streams received")
  }

  def queryFunction(): Query
}

object Consumer {
  case object GetQuery
  case object GetAllRecords
  case object GetMonitorFactories
  case object GetRequirementChange
  case class SetStatus(status: Int)
  case object SetQosMonitors
  //case class SetStreams(streams: (Vector[Stream1[MobilityData]],Vector[Stream1[Int]]))
  case class SetStreams(streams: Seq[Any]) extends Message

  case class AllRecords(recordLatency: RecordLatency = RecordLatency(),
                        recordAverageLoad: RecordAverageLoad = RecordAverageLoad(),
                        recordMessageHops: RecordMessageHops = RecordMessageHops(),
                        recordFrequency: RecordFrequency = RecordFrequency(),
                        recordMessageOverhead: RecordMessageOverhead = RecordMessageOverhead(),
                        recordNetworkUsage: RecordNetworkUsage = RecordNetworkUsage(),
                        recordPublishingRate: RecordPublishingRate = RecordPublishingRate(),
                        recordTransitionStatus: Option[RecordTransitionStatus] = Some(RecordTransitionStatus()),
                        recordProcessingNodes: Option[RecordProcessingNodes] = Some(RecordProcessingNodes())) {
    def allDefined: Boolean =
      recordLatency.lastMeasurement.isDefined &&
        recordMessageHops.lastMeasurement.isDefined &&
        recordAverageLoad.lastLoadMeasurement.isDefined &&
        recordFrequency.lastMeasurement.isDefined &&
        recordMessageOverhead.lastEventOverheadMeasurement.isDefined &&
        recordMessageOverhead.lastPlacementOverheadMeasurement.isDefined &&
        recordNetworkUsage.lastUsageMeasurement.isDefined /*&&
    recordPublishingRate.lastRateMeasurement.isDefined*/
    /*
    SpecialStats.log(this.getClass.getSimpleName, "measurements", s"latency:${recordLatency.lastMeasurement.isDefined}")
    SpecialStats.log(this.getClass.getSimpleName, "measurements", s"message hops:${recordMessageHops.lastMeasurement.isDefined}")
    SpecialStats.log(this.getClass.getSimpleName, "measurements", s"average load:${recordAverageLoad.lastLoadMeasurement.isDefined}")
    SpecialStats.log(this.getClass.getSimpleName, "measurements", s"frequency :${recordFrequency.lastMeasurement.isDefined}")
    SpecialStats.log(this.getClass.getSimpleName, "measurements", s"event overhead :${recordOverhead.lastEventOverheadMeasurement.isDefined}")
    SpecialStats.log(this.getClass.getSimpleName, "measurements", s"event overhead :${recordOverhead.lastPlacementOverheadMeasurement.isDefined}")
    SpecialStats.log(this.getClass.getSimpleName, "measurements", s"network usage :${recordNetworkUsage.lastUsageMeasurement.isDefined}")
    */
    def getRecordsList: List[Measurement] = List(recordLatency, recordAverageLoad, recordMessageHops, recordFrequency, recordMessageOverhead, recordNetworkUsage, recordPublishingRate, recordTransitionStatus.get)
    def getValues = this.getRecordsList.map {
      case l: RecordLatency => l -> l.lastMeasurement
      case l: RecordAverageLoad => l -> l.lastLoadMeasurement
      case h: RecordMessageHops => h -> h.lastMeasurement
      case f: RecordFrequency => f -> f.lastMeasurement
      case o: RecordMessageOverhead => o -> (o.lastEventOverheadMeasurement, o.lastPlacementOverheadMeasurement)
      case n: RecordNetworkUsage => n -> n.lastUsageMeasurement
      case p: RecordPublishingRate => p -> p.lastRateMeasurement
      case t: RecordTransitionStatus => t -> t.lastMeasurement
      case pn: RecordProcessingNodes => pn -> pn.lastMeasurement
    }
  }
}

