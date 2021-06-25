package tcep.graph.nodes.traits

import akka.actor.{ActorRef, Address, PoisonPill}
import tcep.data.Events._
import tcep.data.Queries._
import tcep.graph.nodes.ShutDown
import tcep.graph.nodes.traits.Node.{OperatorMigrationNotice, Subscribe}
import tcep.graph.transition.{TransferredState, TransitionRequest, TransitionStats}
import tcep.publishers.Publisher.AcknowledgeSubscription
import tcep.utils.TCEPUtils

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

/**
  * Handling of [[tcep.data.Queries.BinaryQuery]] is done by BinaryNode
  **/
trait BinaryNode extends Node {
  override val query: BinaryQuery
  var parentNode1: ActorRef
  var parentNode2: ActorRef
  //can have multiple active parents due to SMS mode (we could receive messages from previous parent instances during transition)
  val p1List: ListBuffer[ActorRef] = ListBuffer()
  val p2List: ListBuffer[ActorRef] = ListBuffer()

  private var parent1TransitInProgress = false
  private var parent2TransitInProgress = false
  private var selfTransitionStarted = false
  private var transitionRequestor: ActorRef = _
  private var transitionStartTime: Long = _
  private var transitionStatsAcc: TransitionStats = TransitionStats()

  override def preStart(): Unit = {
    super.preStart()
    p1List += parentNode1
    p2List += parentNode2
  }

  private def updateParent1(oldParent: ActorRef, newParent: ActorRef): Unit = {
    p1List += newParent
    p1List -= oldParent
    parentNode1 = newParent
    updateParentOperatorMap(oldParent, newParent)
  }

  private def updateParent2(oldParent: ActorRef, newParent: ActorRef): Unit = {
    p2List += newParent
    p2List -= oldParent
    parentNode2 = newParent
    updateParentOperatorMap(oldParent, newParent)
  }

  // child receives control back from parent that has completed their transition
  def handleTransferredState(algorithm: String, newParent: ActorRef, oldParent: ActorRef, stats: TransitionStats, placement: Option[Map[Query, Address]]): Unit = {
    implicit val ec: ExecutionContext = blockingIoDispatcher
    if(!selfTransitionStarted) {
      transitionConfig.transitionExecutionMode match {
        case TransitionExecutionModes.CONCURRENT_MODE =>
          if (p1List.contains(oldParent)) {
            updateParent1(oldParent, newParent)
            parent1TransitInProgress = false
          } else if (p2List.contains(oldParent)) {
            updateParent2(oldParent, newParent)
            parent2TransitInProgress = false
          } else {
            transitionLog(s"received TransferState msg from non-parent: ${oldParent}; parents: ${p1List.map(_.path.name)} ${p2List.map(_.path.name)}")
            log.error(new IllegalStateException(s"received TransferState msg from non-parent: ${oldParent}; \n parents: \n $p1List \n $p2List"), "TRANSITION ERROR")
          }

        case TransitionExecutionModes.SEQUENTIAL_MODE =>
          if (p1List.contains(oldParent)) {
            updateParent1(oldParent, newParent)
            parent1TransitInProgress = false
            TCEPUtils.guaranteedDelivery(context, p2List.last, TransitionRequest(algorithm, self, stats, placement), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))
            parent2TransitInProgress = true

          } else if (p2List.contains(oldParent)) {
            updateParent2(oldParent, newParent)
            parent2TransitInProgress = false
          } else {
            transitionLog(s"received TransferState msg from non-parent: ${oldParent}; parents: ${p1List.map(_.path.name)} ${p2List.map(_.path.name)}")
            log.error(new IllegalStateException(s"received TransferState msg from non-parent: ${oldParent}; \n parents: \n $p1List \n $p2List"), "TRANSITION ERROR")
          }
      }

      accumulateTransitionStats(stats)

      if (!parent1TransitInProgress && !parent2TransitInProgress) {
        selfTransitionStarted = true
        log.info(s"parents transition complete, executing own transition -\n new parents: ${parentNode1.toString()} ${parentNode2.toString()}")
        transitionLog(s"old parents transition to new parents with ${algorithm} complete, executing own transition")
        executeTransition(transitionRequestor, algorithm, transitionStatsAcc, placement)
      }
    }
  }

  override def childNodeReceive: Receive = {
    case DependenciesRequest => sender ! DependenciesResponse(Seq(p1List.last, p2List.last))

    //parent has transited to the new node
    case TransferredState(algorithm, newParent, oldParent, stats, lastOperator, placement) =>
      handleTransferredState(algorithm, newParent, oldParent, stats, placement)

    case OperatorMigrationNotice(oldOperator, newOperator) => { // received from migrating parent (oldParent)
      if(p1List.contains(oldOperator)) {
        updateParent1(oldOperator, newOperator)
      }
      if(p2List.contains(oldOperator)) {
        updateParent2(oldOperator, newOperator)
      }
      TCEPUtils.guaranteedDelivery(context, newOperator, Subscribe(self, query))(blockingIoDispatcher).mapTo[AcknowledgeSubscription]
      log.info(s"received operator migration notice from ${oldOperator}, \n new operator is $newOperator \n updated parents $p1List $p2List")
    }

    case ShutDown() => {
      p1List.last ! ShutDown()
      p2List.last ! ShutDown()
      self ! PoisonPill
    }
  }

  override def handleTransitionRequest(requester: ActorRef, algorithm: String, stats: TransitionStats, placement: Option[Map[Query, Address]]): Unit = {
    implicit val ec: ExecutionContext = blockingIoDispatcher
    transitionLog(s"asking old parents ${p1List.last} and ${p2List.last} to transit to new parents with ${algorithm}")
    transitionStartTime = System.currentTimeMillis()
    transitionRequestor = requester
    transitionStatsAcc = TransitionStats()
    transitionConfig.transitionExecutionMode match {
      case TransitionExecutionModes.CONCURRENT_MODE => {
        TCEPUtils.guaranteedDelivery(context, p1List.last, TransitionRequest(algorithm, self, stats, placement), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))
        TCEPUtils.guaranteedDelivery(context, p2List.last, TransitionRequest(algorithm, self, stats, placement), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))
        parent1TransitInProgress = true
        parent2TransitInProgress = true
      }
      case TransitionExecutionModes.SEQUENTIAL_MODE => {
        TCEPUtils.guaranteedDelivery(context, p1List.last, TransitionRequest(algorithm, self, stats, placement), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))
        parent1TransitInProgress = true
        parent2TransitInProgress = false
      }

      case default => throw new Error("Invalid Configuration. Stopping execution of program")
    }
  }

  // merge transition stats from parents for a binary operator
  def accumulateTransitionStats(stats: TransitionStats): TransitionStats = {
    transitionStatsAcc = TransitionStats(
      transitionStatsAcc.placementOverheadBytes + stats.placementOverheadBytes,
      transitionStatsAcc.transitionOverheadBytes + stats.transitionOverheadBytes,
      stats.transitionStartAtKnowledge,
      transitionStatsAcc.transitionTimesPerOperator ++ stats.transitionTimesPerOperator,
      math.max(transitionStatsAcc.transitionEndParent, stats.transitionEndParent)
    )
    transitionStatsAcc
  }

  override def getParentActors(): List[ActorRef] = List(p1List.last, p2List.last)

}
