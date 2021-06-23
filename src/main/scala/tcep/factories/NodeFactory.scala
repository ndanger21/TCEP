package tcep.factories

import akka.actor.{ActorContext, ActorRef, Props}
import akka.cluster.Cluster
import org.slf4j.LoggerFactory
import tcep.data.Queries._
import tcep.graph.nodes._
import tcep.graph.transition.MAPEK.AddOperator
import tcep.machinenodes.helper.actors.{CreateRemoteOperator, RemoteOperatorCreated}
import tcep.placement.HostInfo
import tcep.utils.TCEPUtils

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object NodeFactory {
  val log = LoggerFactory.getLogger(getClass)

  def getOperatorTypeFromQuery(query: Query): Class[_] = {
    query match {
      case aq: AverageQuery =>              classOf[AverageNode]
      case sq: StreamQuery =>               classOf[StreamNode]
      case sq: SequenceQuery =>             classOf[SequenceNode]
      case sq: SelfJoinQuery =>             classOf[SelfJoinNode]
      case jq: JoinQuery =>                 classOf[JoinNode]
      case dq: DisjunctionQuery =>          classOf[DisjunctionNode]
      case cq: ConjunctionQuery =>          classOf[ConjunctionNode]
      case dq: DropElemQuery =>             classOf[DropElemNode]
      case fq: FilterQuery =>               classOf[FilterNode]
      case oc: ObserveChangeQuery =>        classOf[ObserveChangeNode]
      case sw: SlidingWindowQuery =>        classOf[SlidingWindowNode]
      case navg: NewAverageQuery =>         classOf[NewAverageNode]
      case db: DatabaseJoinQuery =>         classOf[DatabaseJoinNode]
      case cn: ConverterQuery =>            classOf[ConverterNode]
      case sf: ShrinkingFilterQuery =>      classOf[ShrinkingFilterNode]
      case winStat: WindowStatisticQuery => classOf[WindowStatisticNode]
      case _ => throw new IllegalArgumentException(s"cannot create unknown operator type: $query")
    }
  }

  def createOperator(targetHost: HostInfo,
                     props: Props,
                     brokerNodeQosMonitor: ActorRef
                    )(implicit ec: ExecutionContext, cluster: Cluster, context: ActorContext): Future[ActorRef] = {
    if (cluster.selfMember == targetHost.member)
      Future {
        try {
          val name = s"${targetHost.operator.toString.split("\\(", 2).head}${UUID.randomUUID().toString}"
          val ref = cluster.system.actorOf(props.withMailbox("prio-mailbox"), name)
          brokerNodeQosMonitor ! AddOperator((targetHost.operator, ref))
          log.info(s"created operator on self: $ref")
          ref
        } catch {
          case e: Throwable =>
            log.error(s"failed to create operator $targetHost", e)
            log.error(s"props: $props")
            throw e
        }
      }
    else for {
      hostTaskManager <- TCEPUtils.getTaskManagerOfMember(cluster, targetHost.member)
      request <- TCEPUtils.guaranteedDelivery(context, hostTaskManager, CreateRemoteOperator(targetHost, props)).mapTo[RemoteOperatorCreated]
    } yield {
      log.info(s"created operator on other cluster node: ${request.ref}")
      request.ref
    }
  }
}
