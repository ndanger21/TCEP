package tcep.graph.nodes.traits

import com.espertech.esper.client._
import tcep.data.Queries._

import scala.concurrent.{ExecutionContext, Future}

/**
  * The actors extending EsperEngine are essentially equipped with their own instance of the EP engine
  * Esper [3], which they use to perform the respective operation they represent. This approach has two
  * advantages. At first, by relying on Esper’s implementation of more complex operators, e.g., join,
  * a (possibly incorrect) implementation of such operators does not have to be provided. Moreover,
  * resolving the semantic ambiguity of the sequence as well as the and operator is also taken care of
  * by Esper’s implementation.
  *
  * @see http://www.espertech.com/products/esper.php (visited on 03/16/2017).
  **/

//TODO: Explore Siddhi engine
trait EsperEngine {

  val esperServiceProviderUri: String

  val configuration = new Configuration

  // Using `lazy val`s here is inspired by Frank Sauer's template `akka-with-esper`:
  // https://www.lightbend.com/activator/template/akka-with-esper
  lazy val serviceProvider: EPServiceProvider =
  EPServiceProviderManager.getProvider(esperServiceProviderUri, configuration)

  lazy val runtime: EPRuntime = serviceProvider.getEPRuntime

  lazy val administrator: EPAdministrator = serviceProvider.getEPAdministrator
  // these functions are wrapped in futures to be run on the blocking dispatcher since Esper calls Thread.sleep during initialisation and destruction, which is undesirable on an Actor's default dispatcher
  def addEventType(eventTypeName: String, elementNames: Array[String], elementClasses: Array[Class[_]])(implicit executionContext: ExecutionContext): Future[Unit] = Future {
    configuration.addEventType(eventTypeName, elementNames, elementClasses.asInstanceOf[Array[AnyRef]])
  }

  def createEpStatement(eplString: String)(implicit executionContext: ExecutionContext): Future[EPStatement] = Future {
    administrator.createEPL(eplString)
  }

  def sendEvent(eventTypeName: String, eventAsArray: Array[AnyRef])(implicit executionContext: ExecutionContext): Unit = {
    runtime.sendEvent(eventAsArray, eventTypeName)
  }

  def destroyServiceProvider(): Unit = {
    serviceProvider.removeAllServiceStateListeners()
    serviceProvider.removeAllStatementStateListeners()
    serviceProvider.destroy()
  }

}

object EsperEngine {

  def createArrayOfNames(query: Query): Array[String] = query match {
    case _: Query1[_] => Array("e1")
    case _: Query2[_, _] => Array("e1", "e2")
    case _: Query3[_, _, _] => Array("e1", "e2", "e3")
    case _: Query4[_, _, _, _] => Array("e1", "e2", "e3", "e4")
    case _: Query5[_, _, _, _, _] => Array("e1", "e2", "e3", "e4", "e5")
    case _: Query6[_, _, _, _, _, _] => Array("e1", "e2", "e3", "e4", "e5", "e6")
    case _ => throw new RuntimeException(s"cannot create names for $query")
  }

  def createArrayOfClasses(query: Query): Array[Class[_]] = {
    val clazz: Class[_] = classOf[AnyRef]
    query match {
      case _: Query1[_] => Array(clazz)
      case _: Query2[_, _] => Array(clazz, clazz)
      case _: Query3[_, _, _] => Array(clazz, clazz, clazz)
      case _: Query4[_, _, _, _] => Array(clazz, clazz, clazz, clazz)
      case _: Query5[_, _, _, _, _] => Array(clazz, clazz, clazz, clazz, clazz)
      case _: Query6[_, _, _, _, _, _] => Array(clazz, clazz, clazz, clazz, clazz, clazz)
      case _ => throw new RuntimeException(s"cannot create classes for $query")
    }
  }

  def toAnyRef(any: Any): AnyRef = {
    // Yep, an `AnyVal` can safely be cast to `AnyRef`:
    // https://stackoverflow.com/questions/25931611/why-anyval-can-be-converted-into-anyref-at-run-time-in-scala
    any.asInstanceOf[AnyRef]
  }

}
