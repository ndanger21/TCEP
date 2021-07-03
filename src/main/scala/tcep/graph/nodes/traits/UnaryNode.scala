package tcep.graph.nodes.traits

import akka.actor.{ActorRef, Address, PoisonPill}
import tcep.data.Events._
import tcep.data.Queries._
import tcep.graph.nodes.ShutDown
import tcep.graph.nodes.traits.Node.{OperatorMigrationNotice, Subscribe}
import tcep.graph.transition.{TransferredState, TransitionRequest, TransitionStats}
import tcep.publishers.Publisher.AcknowledgeSubscription
import tcep.utils.TCEPUtils

/**
  * Handling of [[tcep.data.Queries.UnaryQuery]] is done by UnaryNode
  **/
trait UnaryNode extends Node {
  val query: UnaryQuery
  private var transitionRequestor: ActorRef = _
  private var selfTransitionStarted = false
  private var transitionStartTime: Long = _
  //can have multiple active unary parents due to SMS mode (we could receive messages from older parents)
  override def getParentActors(): List[ActorRef] = np.parentActor.toList

  override def preStart(): Unit = {
    super.preStart()
  }

  override def childNodeReceive: Receive = {

    case DependenciesRequest => sender() ! DependenciesResponse(np.parentActor)

    case TransferredState(placementAlgo, successor, oldParent, stats, lastOperator, placement) =>
      if(!selfTransitionStarted) {
        selfTransitionStarted = true
        if (np.parentActor.contains(oldParent)) {
          np.parentActor = Seq(successor)
          updateParentOperatorMap(oldParent, successor)
        } else log.error(new IllegalStateException(s"received TransferState msg from non-parent: ${oldParent}; \n parent: \n $getParentActors()"), "TRANSITION ERROR")

        transitionLog(s"old parent transition to new parent with ${placementAlgo} complete, executing own transition")
        executeTransition(transitionRequestor, placementAlgo, stats, placement) // stats are updated by MFGS/SMS receive of TransferredState
      }

    case OperatorMigrationNotice(oldOperator, newOperator) =>  // received from migrating parent
      if(np.parentActor.contains(oldOperator)) {
        np.parentActor = Seq(newOperator)
        updateParentOperatorMap(oldOperator, newOperator)
      }
      TCEPUtils.guaranteedDelivery(context, newOperator, Subscribe(self, getParentOperatorMap().head._2))(blockingIoDispatcher).mapTo[AcknowledgeSubscription]
      log.info(s"received operator migration notice from ${oldOperator}, \n new operator is $newOperator \n updated parent $np.parentActor")

    case ShutDown() =>
      np.parentActor.last ! ShutDown()
      self ! PoisonPill
  }


  override def handleTransitionRequest(requester: ActorRef, algorithm: String, stats: TransitionStats, placement: Option[Map[Query, Address]]): Unit = {
    log.info(s"Asking ${np.parentActor.last.path.name} to transit to algorithm ${algorithm}")
    transitionLog(s"asking old parent ${np.parentActor.last.path} to transit to new parent with ${algorithm}")
    transitionRequestor = requester
    transitionStartTime = System.currentTimeMillis()
    TCEPUtils.guaranteedDelivery(context, np.parentActor.last, TransitionRequest(algorithm, self, stats, placement), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))(blockingIoDispatcher)
  }


}