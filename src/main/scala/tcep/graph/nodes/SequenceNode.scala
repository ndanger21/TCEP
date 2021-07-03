package tcep.graph.nodes

import akka.actor.{ActorLogging, ActorRef}
import akka.util.Timeout
import com.espertech.esper.client._
import tcep.data.Events._
import tcep.data.Queries._
import tcep.graph.QueryGraph
import tcep.graph.nodes.traits.Node.NodeProperties
import tcep.graph.nodes.traits._
import tcep.placement.HostInfo

import java.util.concurrent.TimeUnit

/**
  * Handling of [[tcep.data.Queries.SequenceQuery]] is done by SequenceNode.
  *
  * @see [[QueryGraph]]
  **/
//TODO either only allow publishers as parents (no check at the moment) or allow any kind of parent (need to implement transition logic similar to BinaryNode then)
case class SequenceNode(query: SequenceQuery, hostInfo: HostInfo, np: NodeProperties) extends LeafNode with EsperEngine with ActorLogging {

  override val esperServiceProviderUri: String = name
  var esperInitialized = false
  implicit val resolveTimeout = Timeout(60, TimeUnit.SECONDS)

  override def preStart(): Unit = {
    super.preStart()
    assert(np.parentActor.size == 2)
    val init = for {
      type1 <- addEventType("sq1", SequenceNode.createArrayOfNames(query.s1), SequenceNode.createArrayOfClasses(query.s1))(blockingIoDispatcher)
      type2 <- addEventType("sq2", SequenceNode.createArrayOfNames(query.s2), SequenceNode.createArrayOfClasses(query.s2))(blockingIoDispatcher)
      epStatement: EPStatement <- createEpStatement("select * from pattern [every (sq1=sq1 -> sq2=sq2)]")(blockingIoDispatcher)
    } yield {
      val updateListener: UpdateListener = (newEventBeans: Array[EventBean], _) => newEventBeans.foreach(eventBean => {
        try {
          val values1: Array[Any] = eventBean.get("sq1").asInstanceOf[Array[Any]]
          val values2: Array[Any] = eventBean.get("sq2").asInstanceOf[Array[Any]]
          val values = values1.tail ++ values2.tail
          val monitoringData1 = values1.head.asInstanceOf[MonitoringData]
          val monitoringData2 = values2.head.asInstanceOf[MonitoringData]
          val event: Event = values.length match {
            case 2 =>
              val res = Event2(values(0), values(1))
              mergeMonitoringData(res, monitoringData1, monitoringData2, log)
            case 3 =>
              val res = Event3(values(0), values(1), values(2))
              mergeMonitoringData(res, monitoringData1, monitoringData2, log)
            case 4 =>
              val res = Event4(values(0), values(1), values(2), values(3))
              mergeMonitoringData(res, monitoringData1, monitoringData2, log)
            case 5 =>
              val res = Event5(values(0), values(1), values(2), values(3), values(4))
              mergeMonitoringData(res, monitoringData1, monitoringData2, log)
            case 6 =>
              val res = Event6(values(0), values(1), values(2), values(3), values(4), values(5))
              mergeMonitoringData(res, monitoringData1, monitoringData2, log)
          }
          emitEvent(event, np.eventCallback)

        } catch {
          case e: Throwable =>
            log.error(e, "error while processing events in Sequence operator")
        }
      }
      )
      epStatement.addListener(updateListener)
      esperInitialized = true
    }
    //Await.result(init, FiniteDuration(5, TimeUnit.SECONDS)) // block here to wait until esper is initialized
    log.debug("CREATED SEQUENCE OP")
  }

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event if sender().equals(np.parentActor.head) && esperInitialized =>
      log.debug("RECEIVED EVENT {}", event)
      event.updateArrivalTimestamp()
      event match {
      case Event1(e1) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
    }
    case event: Event if sender().equals(np.parentActor.last) && esperInitialized =>
      log.debug("RECEIVED EVENT {}", event)
      event.updateArrivalTimestamp()
      event match {
      case Event1(e1) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
    }
    case unhandledMessage =>
  }

  override def getParentActors(): List[ActorRef] = np.parentActor.toList

  override def postStop(): Unit = {
    destroyServiceProvider()
    super.postStop()
  }

}

object SequenceNode {
  def createArrayOfNames(noReqStream: NStream): Array[String] = noReqStream match {
    case _: NStream1[_] => Array("e1")
    case _: NStream2[_, _] => Array("e1", "e2")
    case _: NStream3[_, _, _] => Array("e1", "e2", "e3")
    case _: NStream4[_, _, _, _] => Array("e1", "e2", "e3", "e4")
    case _: NStream5[_, _, _, _, _] => Array("e1", "e2", "e3", "e4", "e5")
  }

  def createArrayOfClasses(noReqStream: NStream): Array[Class[_]] = {
    val clazz: Class[_] = classOf[AnyRef]
    noReqStream match {
      case _: NStream1[_] => Array(clazz)
      case _: NStream2[_, _] => Array(clazz, clazz)
      case _: NStream3[_, _, _] => Array(clazz, clazz, clazz)
      case _: NStream4[_, _, _, _] => Array(clazz, clazz, clazz, clazz)
      case _: NStream5[_, _, _, _, _] => Array(clazz, clazz, clazz, clazz, clazz)
    }
  }
}
