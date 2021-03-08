package tcep.graph.nodes.traits

import akka.actor.{ActorRef, PoisonPill}
import tcep.data.Events._
import tcep.data.Queries._
import tcep.graph.nodes.ShutDown
import tcep.graph.nodes.traits.Node.{OperatorMigrationNotice, Subscribe}
import tcep.graph.transition.{TransferredState, TransitionRequest, TransitionStats}
import tcep.placement.PlacementStrategy
import tcep.publishers.Publisher.AcknowledgeSubscription
import tcep.utils.TCEPUtils

/**
  * Handling of [[tcep.data.Queries.UnaryQuery]] is done by UnaryNode
  **/
abstract class UnaryNode(var parentActor: Seq[ActorRef]) extends Node {

  override val query: UnaryQuery
  private var transitionRequestor: ActorRef = _
  private var selfTransitionStarted = false
  private var transitionStartTime: Long = _
  //can have multiple active unary parents due to SMS mode (we could receive messages from older parents)
  override def getParentActors(): List[ActorRef] = parentActor.toList

  override def preStart(): Unit = {
    super.preStart()
  }

  override def childNodeReceive: Receive = {

    case DependenciesRequest => sender() ! DependenciesResponse(parentActor)

    case TransferredState(placementAlgo, successor, oldParent, stats, lastOperator) =>
      if(!selfTransitionStarted) {
        selfTransitionStarted = true
        if (parentActor.contains(oldParent)) {
          parentActor = Seq(successor)
          updateParentOperatorMap(oldParent, successor)
        } else log.error(new IllegalStateException(s"received TransferState msg from non-parent: ${oldParent}; \n parent: \n $getParentActors()"), "TRANSITION ERROR")

        transitionLog(s"old parent transition to new parent with ${placementAlgo.name} complete, executing own transition")
        executeTransition(transitionRequestor, placementAlgo, stats) // stats are updated by MFGS/SMS receive of TransferredState
      }

    case OperatorMigrationNotice(oldOperator, newOperator) =>  // received from migrating parent
      if(parentActor.contains(oldOperator)) {
        parentActor = Seq(newOperator)
        updateParentOperatorMap(oldOperator, newOperator)
      }
      TCEPUtils.guaranteedDelivery(context, newOperator, Subscribe(self, getParentOperatorMap().head._2))(blockingIoDispatcher).mapTo[AcknowledgeSubscription]
      log.info(s"received operator migration notice from ${oldOperator}, \n new operator is $newOperator \n updated parent $parentActor")

    case ShutDown() =>
      parentActor.last ! ShutDown()
      self ! PoisonPill
  }


  override def handleTransitionRequest(requester: ActorRef, algorithm: PlacementStrategy, stats: TransitionStats): Unit = {
    log.info(s"Asking ${parentActor.last.path.name} to transit to algorithm ${algorithm.name}")
    transitionLog(s"asking old parent ${parentActor.last.path} to transit to new parent with ${algorithm.name}")
    transitionRequestor = requester
    transitionStartTime = System.currentTimeMillis()
    TCEPUtils.guaranteedDelivery(context, parentActor.last, TransitionRequest(algorithm, self, stats), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))(blockingIoDispatcher)
  }

}