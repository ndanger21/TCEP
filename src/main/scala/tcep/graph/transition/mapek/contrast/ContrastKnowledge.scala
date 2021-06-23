package tcep.graph.transition.mapek.contrast

import tcep.data.Queries._
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.transition.mapek.contrast.ContrastMAPEK.{GetCFM, GetContextData, GetOperatorTreeDepth, MonitoringDataUpdate}
import tcep.graph.transition.{KnowledgeComponent, MAPEK}
import tcep.placement.PlacementStrategy

/**
  * Created by Niels on 18.02.2018.
  * K of the MAPEK loop
  * central sharing point for information regarding system and current context
  * @param mapek reference to the running MAPEK instance
  */
class ContrastKnowledge(mapek: MAPEK, query: Query, transitionConfig: TransitionConfig, currentPlacementStrategy: PlacementStrategy)
  extends KnowledgeComponent(query, transitionConfig, currentPlacementStrategy) {

  val operatorTreeDepth: Int = calculateOperatorTreeDepth(query)
  var contextData: Map[String, AnyVal] = Map()

  val cfm: CFM = new CFM()

  override def preStart() = {
    super.preStart()
    cfm.exportCFMAsXML("logs/")
    log.info(s"starting ContrastMAPEK as actor ${self} with dispatcher ${context.dispatcher}")
  }

  override def receive: Receive = super.receive orElse {

    case GetOperatorTreeDepth => sender() ! this.operatorTreeDepth
    case GetCFM => sender() ! this.cfm
    case GetContextData => sender() ! this.contextData
    case MonitoringDataUpdate(updateData) => this.updateContextData(updateData)
  }

  def updateContextData(data: Map[String, AnyVal]): Unit = {
    for ((k, v) <- data) this.contextData += (k -> v)
  }

  private def calculateOperatorTreeDepth(q: Query): Int = {
    q match {
      case query: StreamQuery => 1
      case query: SequenceQuery => 1
      case query: UnaryQuery => 1 + calculateOperatorTreeDepth(query.sq)
      case query: BinaryQuery => 1 + Math.max(calculateOperatorTreeDepth(query.sq1), calculateOperatorTreeDepth(query.sq2))
    }
  }

}