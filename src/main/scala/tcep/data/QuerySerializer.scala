/*
package tcep.data

import java.nio.charset.StandardCharsets

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import tcep.data.Queries.{Requirement, Stream1, Stream2, Stream3, Stream4, Stream5, Stream6, StreamQuery}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Type, TypeTag, WeakTypeTag, typeOf}
//import akka.serialization.Serializer
import tcep.data.Queries.{BinaryQuery, Query, Query1}

class QuerySerializer extends Serializer[StreamQuery] {

  /*
  override def identifier: Int = 1000000
  override def includeManifest: Boolean = true
  val sep = "!"

  override def toBinary(o: AnyRef): Array[Byte] = {
    o match {
      case b: BinaryQuery =>
        val typeString = b.types.mkString(sep)
        val classManifest = b.getClass.getDeclaredField("sq1").getAnnotatedType
         //getBytes(StandardCharsets.UTF_8)
    }
  }

  override def fromBinary(bytes: Array[Byte],
                          manifest: Option[Class[_]]): AnyRef = {
    val typeSymbolString = new String(bytes, StandardCharsets.UTF_8)
    val typeSymbolNames = typeSymbolString.split(sep)
    typeSymbolNames
  }

   */

  private val sep = "!"
  override def write(kryo: Kryo,
                     output: Output,
                     query: StreamQuery): Unit = {
    output.writeString(query.publisherName)
    kryo.writeObject(output, query.requirements)
    output.writeString(query.types.mkString(sep))
  }

  override def read(kryo: Kryo,
                    input: Input,
                    clazz: Class[StreamQuery]): StreamQuery = {
    val publisherName = input.readString()
    val requirements = kryo.readObject(input, classOf[Set[Requirement]])
    val types = input.readString().split(sep).toVector
    types.length match {
      case 1 => Stream1(publisherName, requirements)
      case 2 => Stream2(publisherName, requirements)
      case 3 => Stream3(publisherName, requirements)
      case 4 => Stream4(publisherName, requirements)
      case 5 => Stream5(publisherName, requirements)
      case 6 => Stream6(publisherName, requirements)
    }
  }
}*/
/*
class TypeTagSerializer extends Serializer[TypeTag[_]] {

  override def write(kryo: Kryo,
                     output: Output,
                     tag: TypeTag[_]): Unit = {
    kryo.writeObject(output, tag.tpe)
  }

  override def read(kryo: Kryo,
                    input: Input,
                    tagTypeClass: Class[TypeTag[_]]): TypeTag[_] = {
    kryo.readObject(input, tagTypeClass)
  }
}
*/