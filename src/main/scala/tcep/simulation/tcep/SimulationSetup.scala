package tcep.simulation.tcep

import akka.actor.{ActorLogging, ActorRef, Address, Cancellable, CoordinatedShutdown, RootActorPath}
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.{Member, MemberStatus}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import tcep.data.Queries._
import tcep.data.Structures.MachineLoad
import tcep.dsl.Dsl._
import tcep.graph.QueryGraph
import tcep.graph.nodes.traits.{TransitionConfig, TransitionExecutionModes, TransitionModeNames}
import tcep.machinenodes.consumers.Consumer.SetStreams
import tcep.machinenodes.consumers.{AnalysisConsumer, TollComputingConsumer}
import tcep.machinenodes.helper.actors._
import tcep.placement.manets.StarksAlgorithm
import tcep.placement.mop.RizouAlgorithm
import tcep.placement.sbon.PietzuchAlgorithm
import tcep.placement.vivaldi.VivaldiCoordinates
import tcep.placement.{GlobalOptimalBDPAlgorithm, MobilityTolerantAlgorithm, RandomAlgorithm}
import tcep.prediction.PredictionHelper.Throughput
import tcep.publishers.Publisher.StartStreams
import tcep.publishers.RegularPublisher.GetEventsPerSecond
import tcep.utils.TCEPUtils

import java.io.File
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

class SimulationSetup(mode: Int, transitionMode: TransitionConfig, durationInMinutes: Option[Int] = None,
                      startingPlacementAlgorithm: String = PietzuchAlgorithm.name,
                      queryString: String = "AccidentDetection",
                      mapek: String = "requirementBased", requirementStr: String = "latency", loadTestMax: Int = 1,
                      overridePublisherPorts: Option[Set[Int]] = None
                     )(implicit val directory: Option[File], val combinedPIM: Boolean, fixedSimulationProperties: Map[Symbol, Int] = Map()
) extends VivaldiCoordinates with ActorLogging {

  val transitionTesting: Boolean = ConfigFactory.load().getBoolean("constants.transition-testing")
  val nSpeedPublishers = ConfigFactory.load().getInt("constants.number-of-speed-publisher-nodes")
  val nSections = ConfigFactory.load().getInt("constants.number-of-road-sections")
  val minNumberOfMembers = ConfigFactory.load().getInt("akka.cluster.min-nr-of-members")
  private def minNumberOfTaskManagers = minNumberOfMembers - cluster.state.members.count(_.hasRole("Consumer"))
  val publisherBasePort = ConfigFactory.load().getInt("constants.base-port") + 1
  val densityPublisherNodePort = publisherBasePort + nSpeedPublishers // base port to distinguish densityPublisher nodes from SpeedPublishers -> last publisher is density publisher
  val speedPublisherNodePorts = publisherBasePort until publisherBasePort + nSpeedPublishers
  // publisher naming convention: P:hostname:port, e.g. "P:p1:2502", or "P:localhost:2501" (for local testing)
  var publishers: Map[String, ActorRef] = Map.empty[String, ActorRef]
  implicit var publisherEventRates: Map[String, Throughput] = Map()
  val publisherPorts: Set[Int] = if(queryString == "AccidentDetection") densityPublisherNodePort :: speedPublisherNodePorts.toList toSet else speedPublisherNodePorts.toSet
  var consumers: Set[ActorRef] = Set()
  var taskManagerActorRefs: Map[Member, ActorRef] = Map()
  var coordinatesEstablished: Set[Address] = Set()
  var publisherActorBroadcastAcks: Set[Address] = Set()
  var simulationStarted = false
  val defaultDuration: Long = ConfigFactory.load().getLong("constants.simulation-time")
  val startTime = System.currentTimeMillis()
  val totalDuration = if(durationInMinutes.isDefined) FiniteDuration(durationInMinutes.get, TimeUnit.MINUTES) else FiniteDuration(defaultDuration, TimeUnit.MINUTES)
  val startDelay = new FiniteDuration(5, TimeUnit.SECONDS)
  val samplingInterval = new FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.MILLISECONDS)
  val requirementChangeDelay = new FiniteDuration(30, TimeUnit.MINUTES)
  val ws = slidingWindow(1.seconds) // join window size
  val latencyRequirement = latency < timespan(30.milliseconds) otherwise None
  val messageHopsRequirement = hops < 3 otherwise None
  val loadRequirement = load < MachineLoad(10.0d) otherwise None
  val frequencyRequirement = frequency > Frequency(200, samplingInterval.toSeconds.toInt) otherwise None
  var graphs: Map[Int, QueryGraph] = Map()
  //val query = ConfigFactory.load().getStringList("constants.query")
  // performance influence model paths for LearnOn
  implicit val pimPaths: (String, String) = if(!Set(Mode.MADRID_TRACES, Mode.LINEAR_ROAD, Mode.YAHOO_STREAMING).contains(mode)) {
    //("/performanceModels/combinedLatencyModel.log", "/performanceModels/combinedLoadModel.log") // just use this as default
    (ConfigFactory.load().getString("constants.mapek.naive-latency-madrid-log-pim-path"), ConfigFactory.load.getString("constants.mapek.naive-load-madrid-log-pim-path"))
  } else if (combinedPIM) {
    (ConfigFactory.load().getString("constants.mapek.latency-combined-pim-path"), ConfigFactory.load().getString("constants.mapek.load-combined-pim-path"))
  } else {
    if (ConfigFactory.load().getBoolean("constants.learnon.naive-models")) {
      mode match {
        case Mode.MADRID_TRACES =>
          (ConfigFactory.load().getString("constants.mapek.naive-latency-madrid-log-pim-path"), ConfigFactory.load.getString("constants.mapek.naive-load-madrid-log-pim-path"))
        case Mode.LINEAR_ROAD =>
          (ConfigFactory.load().getString("constants.mapek.naive-latency-linear-road-log-pim-path"), ConfigFactory.load.getString("constants.mapek.naive-load-linear-road-log-pim-path"))
        case Mode.YAHOO_STREAMING =>
          (ConfigFactory.load().getString("constants.mapek.naive-latency-yahoo-log-pim-path"), ConfigFactory.load.getString("constants.mapek.naive-load-yahoo-log-pim-path"))
      }
    } else {
      if (ConfigFactory.load().getBoolean("constants.learnon.log-prediction")) {
        mode match {
          case Mode.MADRID_TRACES =>
            (ConfigFactory.load().getString("constants.mapek.latency-madrid-log-pim-path"),ConfigFactory.load().getString("constants.mapek.load-madrid-pim-path"))//ConfigFactory.load().getString("constants.mapek.load-madrid-log-pim-path"))
          case Mode.LINEAR_ROAD =>
            (ConfigFactory.load().getString("constants.mapek.latency-linear-road-log-pim-path"), ConfigFactory.load().getString("constants.mapek.load-linear-road-pim-path"))//ConfigFactory.load().getString("constants.mapek.load-linear-road-log-pim-path"))
          case Mode.YAHOO_STREAMING =>
            (ConfigFactory.load().getString("constants.mapek.latency-yahoo-log-pim-path"), ConfigFactory.load().getString("constants.mapek.load-yahoo-pim-path"))//ConfigFactory.load().getString("constants.mapek.load-yahoo-log-pim-path"))
        }
      } else {
        mode match {
          case Mode.MADRID_TRACES =>
            (ConfigFactory.load().getString("constants.mapek.latency-madrid-pim-path"),ConfigFactory.load().getString("constants.mapek.load-madrid-pim-path"))
          case Mode.LINEAR_ROAD =>
            (ConfigFactory.load().getString("constants.mapek.latency-linear-road-pim-path"), ConfigFactory.load().getString("constants.mapek.load-linear-road-pim-path"))
          case Mode.YAHOO_STREAMING =>
            (ConfigFactory.load().getString("constants.mapek.latency-yahoo-pim-path"), ConfigFactory.load().getString("constants.mapek.load-yahoo-pim-path"))
        }
      }
    }
  }
  log.info(s"PIMPATHS are: ${pimPaths}")


  log.info(s"publisher ports: speed ($speedPublisherNodePorts) density: $densityPublisherNodePort \n $publisherPorts")
  lazy val speedPublishers: Vector[(String, ActorRef)] = publishers.toVector.filter(p => speedPublisherNodePorts.contains(p._2.path.address.port.getOrElse(-1)))
  def speedStreams(nSpeedStreamOperators: Int): Vector[Stream1[MobilityData]] = {
    log.info(s"speedPublishers: ${speedPublishers.size} for $nSpeedStreamOperators operators \n ${speedPublishers.mkString("\n")}")
    // enough speed publisher nodes for the query
    if(speedPublishers.size >= nSpeedStreamOperators)
      speedPublishers.map(p => Stream1[MobilityData](p._1, Set())).sortBy(_.publisherName)
    // fewer speed publisher nodes than needed for the query
    else if(speedPublishers.nonEmpty) {
      val streams = (0 until nSpeedStreamOperators).map(i => Stream1[MobilityData](speedPublishers(i % speedPublishers.size)._1, Set())).toVector
      log.warning(s"not enough speed publishers for all stream operators, reusing some (${speedPublishers.size} of $nSpeedStreamOperators (unique Stream operators: ${streams.size}")
      streams
    }
    else throw new IllegalArgumentException(s"no speed publishers found, publishers: \n $publishers")
  }
  def densityPublishers: Vector[(String, ActorRef)] = publishers.toVector.filter(_._2.path.address.port.getOrElse(-1) >= densityPublisherNodePort)
  def singlePublisherNodeDensityStream(section: Int): Stream1[SectionDensity] = {
    val sectionDensityPublisher = densityPublishers.find(_._2.path.name.split("-").last.contentEquals(section.toString))
      .getOrElse(throw new IllegalArgumentException(s"density publisher actor for section $section not found among $densityPublishers"))
    val dp = Stream1[SectionDensity](sectionDensityPublisher._1, Set())
    log.info(s"using density publisher actor ${sectionDensityPublisher._2 } for section $section from a single node")
    dp
  }

  def getStreams(streamOperatorCount: Int): Seq[Vector[Stream1[_ <: StreamDataType]]] = {
    log.info("Creating Streams!")
    val streams: Seq[Vector[Stream1[_ <: StreamDataType]]] = mode match {
      case Mode.MADRID_TRACES =>
        val dense = 0 to nSections-1 map (section => {
          this.singlePublisherNodeDensityStream(section)
        })
        //(this.speedStreams, dense.toVector)
        Seq(this.speedStreams(streamOperatorCount), dense.toVector)

      case Mode.LINEAR_ROAD =>
        val pubVector: Vector[Stream1[LinearRoadDataNew]] = if(publishers.size < streamOperatorCount)
          (0 until streamOperatorCount).map(i => Stream1[LinearRoadDataNew](publishers.toVector(i % publishers.size)._1, Set())).toVector
        else publishers.map(p => Stream1[LinearRoadDataNew](p._1, Set())).toVector
        Seq(pubVector.sortBy(_.publisherName))

      case Mode.YAHOO_STREAMING =>
        val pubVector: Vector[Stream1[YahooDataNew]] = if(publishers.size < streamOperatorCount)
          (0 until streamOperatorCount).map(i => Stream1[YahooDataNew](publishers.toVector(i % publishers.size)._1, Set())).toVector
        else publishers.map(p => Stream1[YahooDataNew](p._1, Set())).toVector
        Seq(pubVector.sortBy(_.publisherName))

      case _ =>
        if(queryString == "AccidentDetection") {
          val dense = 0 to nSections - 1 map (section => {
            this.singlePublisherNodeDensityStream(section)
          })
          Seq(this.speedStreams(streamOperatorCount), dense.toVector)
        } else {
          Seq(this.speedStreams(streamOperatorCount))
        }
    }
    log.info(s"Streams are: ${streams}")
    streams
  }

  override def receive: Receive = {
    super.receive orElse {
      case VivaldiCoordinatesEstablished() =>
        sender() ! ACK()
        coordinatesEstablished = coordinatesEstablished.+(sender().path.address)

      case SetPublisherActorRefsACK() =>
        publisherActorBroadcastAcks = publisherActorBroadcastAcks.+(sender().path.address)
        //if(publisherActorBroadcastAcks.size >= minNumberOfTaskManagers)
          log.info(s"${sender().path.address} received the publisher actorRefs")

      case TaskManagerFound(member, ref) =>
        log.info(s"found taskManager on node $member, ${taskManagerActorRefs.size} of $minNumberOfTaskManagers")
        taskManagerActorRefs = taskManagerActorRefs.updated(member, ref)
        if(taskManagerActorRefs.size >= minNumberOfTaskManagers) {
          taskManagerActorRefs.foreach(_._2 ! AllTaskManagerActors(taskManagerActorRefs))
        }

      case s: CurrentClusterState => this.currentClusterState(s)
      case _ =>
    }
  }


  // accident detection query: low speed, high density in section A, high speed, low density in following road section B
  def accidentQuery(requirements: Requirement*) = {

    val sections = 0 to 1 map(section => { // careful when increasing number of subtrees (21 operators per tree) -> too many operators make transitions with MDCEP unreliable
      val avgSpeed = averageSpeedPerSectionQuery(section)
      val density = singlePublisherNodeDensityStream(section)
      //val and = avgSpeed.join(density, slidingWindow(1 instances), slidingWindow(1 instances))
      val and = avgSpeed.and(density)
      val filter = and.where(
        (_: MobilityData, _: SectionDensity) => true /*(sA: Double, dA: Double) => sA <= 60 && dA >= 10*/ // replace with always true to keep events (and measurements) coming in
      )
      filter
    })
    val and01 = sections(0).and(sections(1), requirements: _*)
    // and12 = sections(1).and(sections(2), requirements: _*)
    and01
  }

  // average speed per section (average operator uses only values matching section)
  def averageSpeedPerSectionQuery(section: Int, requirements: Requirement*) = {
    val nSpeedStreamOperators = 8
    val streams = this.speedStreams(nSpeedStreamOperators)
    if(streams.size < nSpeedStreamOperators) log.warning(s"simulation should have $nSpeedStreamOperators publishers, actual: ${streams.size}; attempting to re-use publishers to generate $nSpeedStreamOperators streams")
    val and01 = streams(0).and(streams(1))
    val and23 = streams(2).and(streams(3))
    val and0123 = and01.and(and23)
    val averageA = Average4(and0123, sectionFilter = Some(section))

    val and67 = streams(4).and(streams(5))
    val and89 = streams(6).and(streams(7))
    val and6789 = and67.and(and89)
    val averageB = Average4(and6789, sectionFilter = Some(section))

    val andAB = averageA.and(averageB)
    val averageAB = Average2(andAB, Set(requirements: _*), sectionFilter = Some(section))
    averageAB
  }

  // average speed of all publishers
  def averageSpeedQuery(requirements: Requirement*) = {
    val nSpeedStreamOperators = 8
    val streams = this.speedStreams(nSpeedStreamOperators)
    if(streams.size < nSpeedStreamOperators) log.warning(s"simulation should have $nSpeedStreamOperators publishers, actual: ${streams.size}; attempting to re-use publishers to generate $nSpeedStreamOperators streams")
    val and01 = streams(0).and(streams(1))
    val and23 = streams(2).and(streams(3))
    val and0123 = and01.and(and23)
    val averageA = Average4(and0123)

    val and67 = streams(4).and(streams(5))
    val and89 = streams(6).and(streams(7))
    val and6789 = and67.and(and89)
    val averageB = Average4(and6789)

    val andAB = averageA.and(averageB)
    val averageAB = Average2(andAB, Set(requirements: _*))
    averageAB
  }

  override def preStart(): Unit = {
    super.preStart()
    context.system.scheduler.scheduleOnce(new FiniteDuration(5, TimeUnit.SECONDS))(checkAndRunQuery)
  }

  override def currentClusterState(state: CurrentClusterState): Unit = {
    super.currentClusterState(state)
    log.info("received currentClusterState, extracting publishers")
    state.members.filter(x => x.status == MemberStatus.Up && x.hasRole("Publisher")) foreach extractProducers
  }

  override def memberUp(member: Member): Unit = {
    super.memberUp(member)
    if (member.hasRole("Publisher")) extractProducers(member)
    else {
      if (member.hasRole("Consumer")) {
        implicit val timeout = Timeout(20, TimeUnit.SECONDS)
        0 until loadTestMax foreach { i =>
          val subref = for {
            actorRef <- context.actorSelection(RootActorPath(member.address) / "user" / s"consumer$i").resolveOne()
          } yield {
            actorRef
          }
          subref.onComplete {
            case Success(actorRef) =>
              log.info(s"SUBSCRIBER found: ${actorRef.path}")
              saveConsumer(actorRef)
            case Failure(exception) =>
              log.info(s"Failed to get SUBSCRIBER cause of $exception")
          }
        }
      }
      else
        log.info(s"found member $member with role: ${member.roles}")
    }
  }

  // finds publisher actors (speed and density) on publisher nodes and saves their ActorRef
  def extractProducers(member: Member): Unit = {
    log.info(s"Found publisher node ${member}")
    implicit val timeout = Timeout(10, TimeUnit.SECONDS)
    if(member.address.port.getOrElse(-1) >= densityPublisherNodePort) { // densityPublisher node -> multiple publisher actors
      0 until nSections foreach { i =>
        if (!publishers.keys.exists(_.contains(s"densityPublisher-$i-"))) {
          val publisherActor = for {
            ref <- context.actorSelection(RootActorPath(member.address) / "user" / s"P:*-densityPublisher-$i-*").resolveOne()
            publisherEventRate <- (ref ? GetEventsPerSecond).mapTo[Throughput]
          } yield (ref, publisherEventRate)
          publisherActor.onComplete {
            case Success(info) =>
              val name = info._1.toString.split("/").last.split("#").head
              log.info(s"saving density publisher ${info} as $name")
              this.savePublisher(name -> info)
            case Failure(exception) =>
              log.warning(s"failed to resolve publisher actor on member $member, due to ${exception.getMessage}, retrying")
              extractProducers(member)
          }
        }
      }
    } else {
        val resolvePublisher = for {
          publisherActorRef <- context.actorSelection(RootActorPath(member.address) / "user" / "P:*").resolveOne() // speedPublisher node -> only one publisher actor
          publisherEventRate <- (publisherActorRef ? GetEventsPerSecond).mapTo[Throughput]
        } yield {
          (publisherActorRef, publisherEventRate)
        }
        resolvePublisher.onComplete {
          case Success(publisherInfo) =>
            val name = publisherInfo._1.toString.split("/").last.split("#").head
            log.info(s"saving publisher ${publisherInfo} as $name")
            this.savePublisher(name -> publisherInfo)
            log.info(s"current publishers: \n ${publishers.keys}")
          case Failure(exception) =>
            log.warning(s"failed to resolve publisher actor on member $member, due to ${exception.getMessage}, retrying")
            extractProducers(member)
        }
      }
  }
  // avoid insert being lost when both resolve futures complete at the same time
  def savePublisher(publisher: (String, (ActorRef, Throughput))): Unit = {
    publishers.synchronized { publishers += publisher._1 -> publisher._2._1 }
    publisherEventRates.synchronized { publisherEventRates += publisher._1 -> publisher._2._2}
  }
  def saveConsumer(consumer: ActorRef): Unit = consumers.synchronized { consumers = consumers + consumer }

  //If
  // 1. all publishers required in the query are available and the minimum number of members is up
  // 2. all link bandwidths are measured
  // 3. all nodes have established their coordinates
  // ... then run the simulation
  def checkAndRunQuery(): Unit = {
    try {
      val timeSinceStart = System.currentTimeMillis() - startTime
      val upMembers = cluster.state.members.filter(m => m.status == MemberStatus.up && !m.hasRole("VivaldiRef"))
      val foundPublisherPorts = publishers.values.map(_.path.address.port.getOrElse(-1)).toSet
      log.info(s"checking if ready, time since init: ${timeSinceStart}ms," +
        s" taskManagers found: ${taskManagerActorRefs.size} of $minNumberOfTaskManagers," +
        s" upMembers: (${upMembers.size} of $minNumberOfMembers), (joining or other state: ${cluster.state.members.size}), " +
        s" publishers: ${publishers.keySet.size} of ${nSpeedPublishers + nSections}, missing publisher port numbers: ${publisherPorts.diff(foundPublisherPorts)}" +
        s" coordinates established: ${coordinatesEstablished.size} of $minNumberOfTaskManagers " +
        s" publisherActorBroadcastsAcks complete: ${publisherActorBroadcastAcks.size} of $minNumberOfTaskManagers" +
        s" num of subscribers: ${consumers.size} of $loadTestMax" +
        s" started: $simulationStarted")

      if(taskManagerActorRefs.size < minNumberOfTaskManagers) {
        upMembers.filter(!_.hasRole("Consumer")).diff(taskManagerActorRefs.keys.toSet)
          .map(m => {
            log.info(s"retrieving Taskmanager of $m")
            TCEPUtils.getTaskManagerOfMember(cluster, m)
              .map(taskM => self ! TaskManagerFound(m, taskM))
          })
      }

      cluster.state.members.foreach(m => TCEPUtils.selectDistVivaldiOn(cluster, m.address) ! StartVivaldiUpdates())
      // broadcast publisher actorRefs to all nodes so that when transitioning, not every placement algorithm instance has to retrieve them for themselves
      // distinguish publishers by ports since they're easier to generalize and set up for mininet, geni and testing than hostnames
      if(publisherPorts.subsetOf(foundPublisherPorts) && taskManagerActorRefs.size >= minNumberOfTaskManagers && publishers.size >= nSpeedPublishers)
        upMembers.foreach(m => if(!m.hasRole("Consumer")) TCEPUtils.selectTaskManagerOn(cluster, m.address) ! SetPublisherActorRefs(publishers))

      // publishers found (speed publisher nodes with 1 speed publisher actors, 1 density publisher node with nSections densityPublisher actors
      if (publisherPorts.subsetOf(foundPublisherPorts) && publishers.size >= nSpeedPublishers && upMembers.size >= minNumberOfMembers && // nodes up
        taskManagerActorRefs.size >= minNumberOfTaskManagers && // taskManager ActorRefs have been found and broadcast to all other taskManagers
        coordinatesEstablished.size >= minNumberOfTaskManagers && // all nodes have established their coordinates
        publisherActorBroadcastAcks.size >= minNumberOfTaskManagers &&
        consumers.size >= loadTestMax && !simulationStarted) {
        simulationStarted = true
        log.info(s"telling publishers to start streams...")
        publishers.foreach(p => p._2 ! StartStreams())
        this.consumers.foreach(c => TCEPUtils.guaranteedDelivery(context, c, SetStreams(this.getStreams(12))))
        this.executeSimulation()

      } else {
        context.system.scheduler.scheduleOnce(new FiniteDuration(5, TimeUnit.SECONDS))(checkAndRunQuery)
      }
    } catch {
      case e: Throwable => log.error(e, "failed to start simulation")
    }
  }

  /**
    * run a simulation with the specified starting parameters
    * @param i                  simulation run number
    * @param placementStrategy  starting placement strategy
    * @param transitionConfig     transition mode (MFGS or SMS) to use if requirements change
    * @param initialRequirements query to be executed
    * @param finishedCallback   callback to apply when simulation is finished
    * @param requirementChanges  requirement to change to after requirementChangeDelay
    * @param splcDataCollection boolean to collect additional data for CONTRAST MAPEK implementation (machine learning data)
    * @return queryGraph of the running simulation
    */
  def runSimulation(i: Int, placementStrategy: String, transitionConfig: TransitionConfig, finishedCallback: () => Any,
                    initialRequirements: Set[Requirement], requirementChanges: Option[Set[Requirement]] = None,
                    splcDataCollection: Boolean = false): Unit = {
      log.info("run simulation entered")
      val query = queryString match {
      case "Stream" => stream[MobilityData](speedPublishers(0)._1, initialRequirements.toSeq: _*)
      case "Filter" => stream[MobilityData](speedPublishers(0)._1, initialRequirements.toSeq: _*).where(_ => true)
      case "Conjunction" => stream[MobilityData](speedPublishers(0)._1).and(stream[MobilityData](speedPublishers(1)._1), initialRequirements.toSeq: _*)
      case "Disjunction" => stream[MobilityData](speedPublishers(0)._1).or(stream[MobilityData](speedPublishers(1)._1), initialRequirements.toSeq: _*)
      case "Join" => stream[MobilityData](speedPublishers(0)._1).join(stream[MobilityData](speedPublishers(1)._1), slidingWindow(1.seconds), slidingWindow(1.seconds)).where((_, _) => true, initialRequirements.toSeq: _*)
      case "SelfJoin" => stream[MobilityData](speedPublishers(0)._1).selfJoin(slidingWindow(1.seconds), slidingWindow(1.seconds), initialRequirements.toSeq: _*)
      case "AccidentDetection" => accidentQuery(initialRequirements.toSeq: _*)
      case "AdAnalysis" => AnalysisConsumer.adAnalysisQuery(getStreams(8), AnalysisConsumer.loadStorageDatabase()(log), initialRequirements)
      case "LinearRoad" => TollComputingConsumer.tollComputingQuery(getStreams(6), initialRequirements)
    }
    log.info("query received")
        //accidentQuery(initialRequirements.toSeq: _*)
      //val query = Average2(speedStreams(0).and(speedStreams(1)), initialRequirements)
      //val query = averageSpeedQuery(initialRequirements.toSeq: _*)
      def debugQuery(parent: Query1[MobilityData] = speedStreams(1)(0), depth: Int = 200, count: Int = 0): Query1[MobilityData] = {
        if(count < depth) debugQuery(parent.where(_ => true), depth, count + 1)
        else parent.where(_ => true, initialRequirements.toSeq :_*)
      }
      def debugQueryTree(depth: Int = 3, count: Int = 0): Average2[MobilityData] = {
        if(count < depth) Average2(debugQueryTree(depth, count + 1).and(debugQueryTree(depth, count + 1)))
        else averageSpeedQuery() // 18 operators
      }

      val sim = if (splcDataCollection || this.mapek == "CONSTRAST" || this.mapek == "LearnOn") {
        new SPLCDataCollectionSimulation(
          name = s"${transitionMode}-${placementStrategy}-${queryString}-${mapek}-${requirementStr}-$i",
          query, transitionConfig, publishers, consumers.toVector(i),
          Some(placementStrategy), this.mapek)
        } else {
            new Simulation(name = s"${transitionMode}-${placementStrategy}-${queryString}-${mapek}-${requirementStr}-$i",
              query, transitionConfig, publishers, consumers.toVector(i), Some(placementStrategy), this.mapek)
        }

    val percentage: Double = (i + 1 / loadTestMax.toDouble) * 100.0
    log.info(s"starting $transitionMode ${placementStrategy} algorithm simulation number $i (progress: $percentage) \n with requirements \n $initialRequirements \n and with requirementChanges \n $requirementChanges \n mapek $mapek and query $query")
    val graph = Await.result(sim.startSimulation(queryString, startDelay, samplingInterval, totalDuration)(finishedCallback), FiniteDuration(120, TimeUnit.SECONDS)) // (start simulation time, interval, end time (s))
    graphs = graphs.+(i -> graph)
    context.system.scheduler.scheduleOnce(totalDuration)(this.shutdown())
    val firstDelay = requirementChangeDelay
    val firstReqChange: Option[Cancellable] = if (requirementChanges.isDefined && requirementChanges.get.nonEmpty) {
        log.info(s"scheduling first requirement change after $firstDelay for graph ${graphs(i)}")
        Some(context.system.scheduler.scheduleOnce(firstDelay)(changeReqTask(i, initialRequirements.toSeq, requirementChanges.get.toSeq)))
        //log.info(s"scheduling second requirement change after ${requirementChangeDelay.mul(2)} for graph ${graphs(i-1)}")
        //context.system.scheduler.scheduleOnce(requirementChangeDelay.mul(2))(changeReqTask(i, requirementChanges.get.toSeq, initialRequirements.toSeq))
    } else None

    // iterates over all available algorithms (-> 1 transition every requirementChangeDelay)
    if (transitionTesting) {
      if (firstReqChange.isDefined) firstReqChange.get.cancel()
      val allAlgorithms = ConfigFactory.load().getStringList("benchmark.general.algorithms").asScala
      //val allAlgorithms = List(PietzuchAlgorithm.name, GlobalOptimalBDPAlgorithm.name)
      var mult = 1
      val repetitions: Double = (totalDuration.div(requirementChangeDelay)) / allAlgorithms.size
      for (repeat <- 0 until repetitions.toInt) {
        allAlgorithms.foreach(a => {
          val t = if(repeat == 0 && a == allAlgorithms.head) FiniteDuration(1, TimeUnit.MINUTES).plus(requirementChangeDelay)
          else requirementChangeDelay.mul(mult)
          context.system.scheduler.scheduleOnce(t)(explicitlyChangeAlgorithmTask(i, a))
          mult += 1
          log.info(s"scheduling manual algorithm change to $a after $t ")
        })
      }
    }
  }

  def changeReqTask(i: Int, oldReq: Seq[Requirement], newReq: Seq[Requirement]): Unit = {
    log.info(s"running scheduled req change for graph $i from oldReq: $oldReq to newReq: $newReq")
    graphs(i).removeDemand(oldReq)
    graphs(i).addDemand(newReq)
  }

  def explicitlyChangeAlgorithmTask(i: Int, algorithm: String): Unit = {
    log.info(s"executing scheduled explicit change algorithm of graph $i to $algorithm")
    graphs(i).manualTransition(algorithm)
  }

  // interactive simulation mode: select query and algorithm via GUI at runtime
  def testGUI(i: Int, j: Int, windowSize: Int): Unit = {
    try {
      var sims: mutable.MutableList[Simulation] = mutable.MutableList()
      var graphs: mutable.MutableList[QueryGraph] = mutable.MutableList()
      var currentTransitionMode: String = null
      @volatile var currentStrategyName: String = null
      var strategyNameUpdate: Cancellable = null
      var currentQueryName: String = null
      var currentMapekName: String = null
      @volatile var simulationStarted = false
      val allReqs = Set(latencyRequirement, loadRequirement, messageHopsRequirement)

      GUIConnector.sendMembers(cluster)

      val startSimulationRequest: (String, String, List[String], String, String) => Unit = (transitionMode: String, strategyName: String, optimizationCriteria: List[String], query: String, mapekName: String) => {
        if (simulationStarted) {
          return
        }
        simulationStarted = true
        val mode = transitionMode match {
          case "MFGS" => TransitionModeNames.MFGS
          case "SMS" => TransitionModeNames.SMS
          case "NMS" => TransitionModeNames.NaiveMovingState
          case "NSMS" => TransitionModeNames.NaiveStopMoveStart
        }
        var currentQuery: Query = null

        val newReqs =
          if (optimizationCriteria != null) optimizationCriteria.map(reqName => allReqs.find(_.name == reqName).getOrElse(throw new IllegalArgumentException(s"unknown requirement name: ${reqName}")))
          else {
            Seq(latencyRequirement, messageHopsRequirement)
          }
        log.info(s"-----------------------------")
        log.info(s"strategy name: ${strategyName}")
        log.info(s"-----------------------------")

        //globalOptimalBDPQuery = stream[Int](pNames(0)).join(stream[Int](pNames(1)), slidingWindow(windowSize.seconds), slidingWindow(windowSize.seconds)).where((_, _) => true, newReqs:_*)
        currentQuery = query match {
          case "Stream" => stream[MobilityData](speedPublishers(0)._1, newReqs: _*)
          case "Filter" => stream[MobilityData](speedPublishers(0)._1, newReqs: _*).where(_ => true)
          case "Conjunction" => stream[MobilityData](speedPublishers(0)._1).and(stream[MobilityData](speedPublishers(1)._1), newReqs: _*)
          case "Disjunction" => stream[MobilityData](speedPublishers(0)._1).or(stream[MobilityData](speedPublishers(1)._1), newReqs: _*)
          case "Join" => stream[MobilityData](speedPublishers(0)._1).join(stream[MobilityData](speedPublishers(1)._1), slidingWindow(windowSize.seconds), slidingWindow(windowSize.seconds)).where((_, _) => true, newReqs: _*)
          case "SelfJoin" => stream[MobilityData](speedPublishers(0)._1).selfJoin(slidingWindow(windowSize.seconds), slidingWindow(windowSize.seconds), newReqs: _*)
          case "AccidentDetection" => accidentQuery(newReqs: _*)
        }

        val percentage: Double = (i + 1 / loadTestMax.toDouble) * 100.0

        val sim = if (mapekName == "CONSTRAST" || mapekName == "LearnOn") {
          new SPLCDataCollectionSimulation(
            name = transitionMode,
            currentQuery, TransitionConfig(mode, TransitionExecutionModes.CONCURRENT_MODE),
            publishers, consumers.toVector(i),
            Some(strategyName), mapekName)
        } else {
          new Simulation(transitionMode, currentQuery,
            TransitionConfig(mode, TransitionExecutionModes.CONCURRENT_MODE),
            publishers, consumers.toVector(i),
            Some(strategyName),
            mapekName)
        }

        sims += sim

        //start at 20th second, and keep recording data for 5 minutes
        val graph = Await.result(sim.startSimulation(query, startDelay, samplingInterval, FiniteDuration.apply(0, TimeUnit.SECONDS))(null), FiniteDuration(15, TimeUnit.SECONDS))
        graphs += graph

        currentTransitionMode = transitionMode
        currentQueryName = query
        currentMapekName = mapekName
        log.info(s"scheduling update for currentStrategyName of graph $i")
        if(i == 0) { // schedule this only for the first graph so the gui shows only the name of the first graph if multiple are present
          strategyNameUpdate = cluster.system.scheduler.scheduleAtFixedRate(FiniteDuration(0, TimeUnit.SECONDS), FiniteDuration(1, TimeUnit.SECONDS))(() => {
            for {stratName <- graph.getPlacementStrategy()} yield {
              currentStrategyName = stratName
              log.info(s"updated currentStrategyName $currentStrategyName")
            }
          })
        }
      }

      val transitionRequest = (optimizationCriteria: List[String]) => {
        log.info("Received new optimization criteria " + optimizationCriteria.toString())
        //optimizationCriteria.foreach(op => {
        graphs.foreach(graph => {
          val newReqs = optimizationCriteria.map(reqName => allReqs.find(_.name == reqName).getOrElse(throw new IllegalArgumentException(s"unknown requirement name: ${reqName}")))
          graph.removeDemand(allReqs.toSeq)
          graph.addDemand(newReqs)
        })
      }

      val manualTransitionRequest = (algorithmName: String) => {
        log.info("Received new manual transition request " + algorithmName)
        graphs.foreach(graph => {
          graph.manualTransition(algorithmName)
        })
      }

      val stop = () => {
        sims.foreach(sim => {
          sim.stopSimulation()
        })
        graphs.foreach(graph => {
          graph.stop()
        })
        graphs = mutable.MutableList()
        sims = mutable.MutableList()
        if(strategyNameUpdate != null) strategyNameUpdate.cancel()
        currentStrategyName = null
        currentTransitionMode = null
        currentQueryName = null
        simulationStarted = false
      }

      val status = () => {
        var strategyName = "none"
        var transitionMode = "none"
        var query = "none"
        var mapek = "none"
        if (currentStrategyName != null) {
          strategyName = currentStrategyName
        }
        if (currentTransitionMode != null) {
          transitionMode = currentTransitionMode
        }
        if (currentQueryName != null) {
          query = currentQueryName
        }
        if (currentMapekName != null)
          mapek = currentMapekName

        val response = Map(
          "placementStrategy" -> strategyName,
          "transitionMode" -> transitionMode,
          "query" -> query,
          "mapek" -> mapek
        )
        log.info(s"status response: $response")
        response
      }

      val server = new TCEPSocket(this.context.system)
      server.startServer(startSimulationRequest, transitionRequest, manualTransitionRequest, stop, status)
      log.info(s"started GUI server $server for interactive simulation")
    } catch {
      case e: Throwable =>
        log.error(e, "testGUI crashed, restarting...")
        testGUI(i, j, windowSize)
    }
  }


  def executeSimulation(): Unit = try {

    val reqChange = requirementStr match {
      case "latency" => latencyRequirement
      case "load" => loadRequirement
      case "hops" => messageHopsRequirement
      case "throughput" => frequencyRequirement
      case _ =>
        log.warning(s"unknown initial requirement supplied as argument: $requirementStr, defaulting to latencyReq")
        latencyRequirement
    }

    mode match {

      case Mode.TEST_RELAXATION => for (i <- Range(0, loadTestMax))
        this.runSimulation(i, PietzuchAlgorithm.name, transitionMode,
          () => log.info(s"Starks algorithm Simulation Ended for $i"),
          Set(latencyRequirement))

      case Mode.TEST_STARKS => for (i <- Range(0, loadTestMax))
        this.runSimulation(i, StarksAlgorithm.name, transitionMode,
          () => log.info(s"Starks algorithm Simulation Ended for $i"),
          Set(messageHopsRequirement))

      case Mode.TEST_RIZOU => for (i <- Range(0, loadTestMax))
        this.runSimulation(i, RizouAlgorithm.name, transitionMode,
          () => log.info(s"Starks algorithm Simulation Ended for $i"),
          Set(loadRequirement)) // use load requirement to make it "selectable" for RequirementBasedMAPEK/BenchmarkingNode

      case Mode.TEST_PC => for (i <- Range (0, loadTestMax) )
        this.runSimulation (i,  MobilityTolerantAlgorithm.name, transitionMode,
          () => log.info (s"Producer Consumer algorithm Simulation Ended for $i"),
          Set(latencyRequirement), Some(Set()))

      case Mode.TEST_OPTIMAL => for (i <- Range (0, loadTestMax) )
        this.runSimulation (i, GlobalOptimalBDPAlgorithm.name, transitionMode,
          () => log.info (s"Producer Consumer algorithm Simulation Ended for $i"),
          Set(latencyRequirement), Some(Set()))

      case Mode.TEST_RANDOM => for (i <- Range (0, loadTestMax) )
        this.runSimulation (i, RandomAlgorithm.name, transitionMode,
          () => log.info (s"Producer Consumer algorithm Simulation Ended for $i"),
          Set(latencyRequirement), Some(Set()))

      case Mode.TEST_SMS =>
        this.runSimulation(0, startingPlacementAlgorithm, TransitionConfig(TransitionModeNames.SMS, TransitionExecutionModes.CONCURRENT_MODE),
          () => log.info(s"SMS transition $startingPlacementAlgorithm algorithm Simulation ended"),
          Set(latencyRequirement), Some(Set(messageHopsRequirement)))

      case Mode.TEST_MFGS =>
        this.runSimulation(0, startingPlacementAlgorithm, TransitionConfig(TransitionModeNames.MFGS, TransitionExecutionModes.SEQUENTIAL_MODE),
          () => log.info(s"MFGS transition $startingPlacementAlgorithm algorithm Simulation ended"),
          Set(latencyRequirement), Some(Set(reqChange)))

      case Mode.SPLC_DATACOLLECTION =>
        val used_mapek = ConfigFactory.load().getString("constants.mapek.type")
        this.runSimulation(0, startingPlacementAlgorithm, transitionMode,
          () => log.info(s"$transitionMode $startingPlacementAlgorithm algorithm Simulation with SPLC data collection enabled ended"),
          Set(latencyRequirement), Some(Set(latencyRequirement, frequencyRequirement)), true)

      case Mode.TEST_GUI => this.testGUI(0, 0, 1)
      case Mode.DO_NOTHING => log.info("simulation ended!")
      case Mode.TEST_LIGHTWEIGHT =>
        this.runSimulation(0, startingPlacementAlgorithm, transitionMode,
          () => log.info(s"Lightweight transitions simulation ended"),
          Set(latencyRequirement), Some(Set(reqChange)))

      case Mode.MADRID_TRACES =>
        this.runSimulation(0, startingPlacementAlgorithm, transitionMode,
          () => log.info("MadridTrace simulation ended"),
          Set(latencyRequirement), Some(Set()))

      case Mode.LINEAR_ROAD =>
        this.runSimulation(0, startingPlacementAlgorithm, transitionMode,
          () => log.info("LinearRoad simulation ended"),
          Set(), Some(Set(frequencyRequirement)), true)

      case Mode.YAHOO_STREAMING =>
        this.runSimulation(0, startingPlacementAlgorithm, transitionMode,
          () => log.info("YahooStreaming simulation ended"),
          Set(latencyRequirement, frequencyRequirement), Some(Set(latencyRequirement, frequencyRequirement)), true)
    }

  } catch {
    case e: Throwable => log.error(e, "failed to run simulation")
  }

  def shutdown() = {
    import scala.sys.process._
    ("pkill -f ntpd").!
    CoordinatedShutdown.get(cluster.system).run(CoordinatedShutdown.ClusterDowningReason)
    this.cluster.system.terminate()

  }

}

case class RecordLatency() extends LatencyMeasurement {
  var lastMeasurement: Option[java.time.Duration] = Option.empty

  override def apply(latency: java.time.Duration): Any = {
    lastMeasurement = Some(latency)
  }
}

case class RecordMessageHops() extends MessageHopsMeasurement {
  var lastMeasurement: Option[Int] = Option.empty

  override def apply(overhead: Int): Any = {
    lastMeasurement = Some(overhead)
  }
}

case class RecordAverageLoad() extends LoadMeasurement {
  var lastLoadMeasurement: Option[Double] = Option.empty

  def apply(load: Double): Any = {
    lastLoadMeasurement = Some(load)
  }
}

case class RecordFrequency() extends FrequencyMeasurement {
  var lastMeasurement: Option[Int] = Option.empty

  override def apply(frequency: Int): Any = {
    lastMeasurement = Some(frequency)
  }
}

case class RecordTransitionStatus() extends TransitionMeasurement {
  var lastMeasurement: Option[Int] = Option.empty

  override def apply(status: Int): Any = {
    lastMeasurement = Some(status)
  }
}

case class RecordMessageOverhead() extends MessageOverheadMeasurement {
  var lastEventOverheadMeasurement: Option[Long] = Option.empty
  var lastPlacementOverheadMeasurement: Option[Long] = Option.empty

  override def apply(eventOverhead: Long, placementOverhead: Long): Any = {
    lastEventOverheadMeasurement = Some(eventOverhead)
    lastPlacementOverheadMeasurement = Some(placementOverhead)
  }
}

case class RecordNetworkUsage() extends NetworkUsageMeasurement {
  var lastUsageMeasurement: Option[Double] = Option.empty

  override def apply(status: Double): Any = {
    lastUsageMeasurement = Some(status)
  }
}

case class RecordPublishingRate() extends PublishingRateMeasurement {
  var lastRateMeasurement: Option[Double] = Option.empty

  override def apply(status: Double): Any = {
    lastRateMeasurement = Some(status)
  }
}


case class RecordProcessingNodes() {
  var lastMeasurement: Option[List[Address]] = None

  def apply(status: List[Address]): Any = {
    lastMeasurement = Some(status)
  }
}


trait StreamDataType
case class MobilityData(publisherSection: Int, speed: Double) extends StreamDataType
case class SectionDensity(density: Int) extends StreamDataType
case class LinearRoadDataNew(vehicleId: Int, section: Int, density: Int, speed: Double, var change: Boolean = false, var dataWindow: Option[List[Double]] = None, var avgSpeed: Option[Double] = None) extends StreamDataType
case class YahooDataNew(adId: Int, eventType: Int, var campaignId: Option[Int] = None) extends StreamDataType
case class StatisticData(id: Int, performance: Double) extends StreamDataType

case object SetMissingBandwidthMeasurementDefaults