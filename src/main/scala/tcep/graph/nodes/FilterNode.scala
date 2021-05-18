package tcep.graph.nodes

import akka.actor.ActorRef
import tcep.data.Events._
import tcep.data.Queries._
import tcep.graph.nodes.traits._
import tcep.graph.{CreatedCallback, EventCallback, QueryGraph}
import tcep.placement.HostInfo
import tcep.simulation.tcep.SimulationRunner.logger

import java.io.{File, PrintStream}

/**
  * Handling of [[tcep.data.Queries.FilterQuery]] is done by FilterNode.
  *
  * @see [[QueryGraph]]
  **/

case class FilterNode(transitionConfig: TransitionConfig,
                      hostInfo: HostInfo,
                      backupMode: Boolean,
                      mainNode: Option[ActorRef],
                      query: FilterQuery,
                      createdCallback: Option[CreatedCallback],
                      eventCallback: Option[EventCallback],
                      isRootOperator: Boolean,
                      publisherEventRate: Double,
                      _parentActor: Seq[ActorRef]
                     ) extends UnaryNode(_parentActor) {

  val directory = if(new File("./logs").isDirectory) Some(new File("./logs")) else {
    logger.info("Invalid directory path")
    None
  }

  val csvWriter = directory map { directory => new PrintStream(new File(directory, s"filter-messages.csv"))
  } getOrElse java.lang.System.out
  csvWriter.println("time\tfiltered\tvalue1\tvalue2")

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event =>
      event.updateArrivalTimestamp()
      val s = sender()
      if (parentActor.contains(s)) {
        if (query.cond(event))
          emitEvent(event, eventCallback)
      } else log.info(s"received event $event from $s, \n parent: \n $parentActor")

    case unhandledMessage => log.info(s"${self.path.name} received msg ${unhandledMessage.getClass} from ${sender()}")
  }

}

