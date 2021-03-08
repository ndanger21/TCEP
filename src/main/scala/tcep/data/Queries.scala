package tcep.data

import java.time.Duration

import akka.actor.Address
import tcep.data.Events._
import tcep.data.Structures.MachineLoad
import tcep.dsl.Dsl.{Frequency, FrequencyMeasurement, LatencyMeasurement, LoadMeasurement, MessageHopsMeasurement}
import tcep.machinenodes.helper.actors.MySerializable
import tcep.placement.QueryDependencies
import tcep.simulation.tcep.MobilityData
import tcep.utils.SizeEstimator

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.reflect.runtime.universe.{TypeTag, typeOf}

/**
  * Helpers object for defining/representing queries in terms of case class.
  * */
object Queries {

  sealed trait NStream { val publisherName: String }
  case class NStream1[A]                (publisherName: String)(implicit tagA: TypeTag[A]) extends NStream
  case class NStream2[A, B]             (publisherName: String)(implicit tagA: TypeTag[A], tagB: TypeTag[B]) extends NStream
  case class NStream3[A, B, C]          (publisherName: String)(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C]) extends NStream
  case class NStream4[A, B, C, D]       (publisherName: String)(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends NStream
  case class NStream5[A, B, C, D, E]    (publisherName: String)(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends NStream

  sealed trait Window
  case class SlidingInstances  (instances: Int) extends Window
  case class TumblingInstances (instances: Int) extends Window
  case class SlidingTime       (time: Int)   extends Window
  case class TumblingTime      (time: Int)   extends Window

  sealed trait Operator
  case object Equal        extends Operator
  case object NotEqual     extends Operator
  case object Greater      extends Operator
  case object GreaterEqual extends Operator
  case object Smaller      extends Operator
  case object SmallerEqual extends Operator

  sealed abstract class Requirement(val name: String)
  case class LatencyRequirement   (operator: Operator, latency: Duration, otherwise: Option[LatencyMeasurement], override val name: String = LatencyRequirement.name)      extends Requirement(name)
  object LatencyRequirement { val name = "latency" }

  case class FrequencyRequirement (operator: Operator, frequency: Frequency, otherwise: Option[FrequencyMeasurement], override val name: String = FrequencyRequirement.name) extends Requirement(name)
  object FrequencyRequirement { val name = "frequency" }

  case class LoadRequirement      (operator: Operator, machineLoad: MachineLoad, otherwise: Option[LoadMeasurement], override val name: String = LoadRequirement.name)            extends Requirement(name)
  object LoadRequirement { val name = "machineLoad" }

  case class MessageHopsRequirement(operator: Operator, requirement: Int, otherwise: Option[MessageHopsMeasurement], override val name: String = MessageHopsRequirement.name)            extends Requirement(name)
  object MessageHopsRequirement { val name = "messageHops" }

  case class ReliabilityRequirement(override val name: String = ReliabilityRequirement.name) extends Requirement(name)
  object ReliabilityRequirement { val name = "reliability" }

  def pullRequirements(q: Query, accumulatedReq: List[Requirement]): Set[Requirement] = q match {
    case query: UnaryQuery => q.requirements ++ pullRequirements(query.sq, accumulatedReq)
    case query: BinaryQuery =>
      val child1 = pullRequirements(query.sq1, q.requirements.toList ++ accumulatedReq)
      val child2 = pullRequirements(query.sq2, q.requirements.toList ++ accumulatedReq)
      q.requirements ++ child1 ++ child2
    case query: Query => q.requirements ++ accumulatedReq
  }

  sealed trait Query extends MySerializable {
    val types: Vector[String]
    val requirements: Set[Requirement]
    def estimateEventSize: Long
    @transient implicit val dummyAddr: Address = Address("tcp", "tcep", "dummy", 2000)

    override def toString(): String = {
      this match {
        case bq: BinaryQuery => s"${ bq.getClass.getSimpleName }(${bq.sq1}, ${bq.sq2})"
        case uq: UnaryQuery => s"${ uq.getClass.getSimpleName }(${uq.sq})"
        case sq: StreamQuery => s"${ sq.getClass.getSimpleName }(${ sq.publisherName })"
        case seq: SequenceQuery => s"${ seq.getClass.getSimpleName }(${ seq.s1.publisherName } -> ${ seq.s2.publisherName })"
        case dummyQuery: PublisherDummyQuery => s"${ dummyQuery.getClass.getSimpleName }(${dummyQuery.p})"
        case dummyQuery: ClientDummyQuery => s"${ dummyQuery.getClass.getSimpleName }"
        case _ => this.toString()
      }
    }

    protected def getSampleValue(tpeString: String): Any = {
      tpeString match {
        case t if t == typeOf[Boolean].toString => true
        case t if t == typeOf[Int].toString => 0
        case t if t == typeOf[Long].toString => 1L
        case t if t == typeOf[String].toString => "DefaultString"
        case t if t == typeOf[Float].toString => 1.0f
        case t if t == typeOf[Double].toString => 1.0d
        case t if t == typeOf[MobilityData].toString => MobilityData(0, 0.0d)
        // no idea how else to handle disjunction operator here; extend as needed
        case t if t == typeOf[Either[Int, String]] .toString=> Right("DefaultString") // use the largest of the other possible values
        case t if t == typeOf[Either[String, Int]].toString => Left("DefaultString")
        case t if t == typeOf[Either[Int, Float]].toString => Right(1.0f)
        case t if t == typeOf[Either[Float, Boolean]].toString => Left(1.0f)
        case t if t == typeOf[Either[Either[Int, String], Boolean]].toString => Left(getSampleValue(typeOf[Either[Int, String]].toString))
        case t if t == typeOf[Either[Unit, Boolean]].toString => Right(true)
        case other => throw new NotImplementedError(s"could not find a sample value for $other, must be implemented")
      }
    }
  }

  type EventRateEstimate = Double
  type EventSizeEstimate = Long
  type EventBandwidthEstimate = Double
  def extractOperators(query: Query, baseEventRate: Double): mutable.LinkedHashMap[Query, (QueryDependencies, EventRateEstimate, EventSizeEstimate, EventBandwidthEstimate)] = {
    /**
      * recursively extracts the operators and their dependent child and parent operators
      * as well as their estimated output event rate and size (depending on parent operators), and used bandwidth in [Bytes / s]
      *
      * @param operator current operator
      * @param child child operator
      * @return the sorted map of all operators and their dependencies, with the root operator as the second to last map entry
      */
    def extractOperatorsRec(operator: Query, child: Option[Query] = Some(ClientDummyQuery())
                           ): mutable.LinkedHashMap[Query, (QueryDependencies, EventRateEstimate, EventSizeEstimate, EventBandwidthEstimate)] = {
      operator match {
        case b: BinaryQuery =>
          val left = extractOperatorsRec(b.sq1, Some(b))
          val right = extractOperatorsRec(b.sq2, Some(b))
          val estimatedEventRate = estimateEventRate(operator, (left ++ right).toMap)
          val estimatedEventSize = b.estimateEventSize
          left ++= right += b -> (QueryDependencies(Some(List(b.sq1, b.sq2)), child), estimatedEventRate, estimatedEventSize, estimatedEventRate * estimatedEventSize)

        case u: UnaryQuery =>
          val parentRes = extractOperatorsRec(u.sq, Some(u))
          val estimatedEventRate = parentRes(u.sq)._2
          val estimatedEventSize = u.estimateEventSize
          parentRes += u -> (QueryDependencies(Some(List(u.sq)), child), estimatedEventRate, estimatedEventSize, estimatedEventSize * estimatedEventRate)

        case s: StreamQuery => // stream nodes have no parent operators, they depend on publishers
          val estimatedEventSize = s.estimateEventSize
          val streamOp = s -> (QueryDependencies(Some(List(PublisherDummyQuery(s.publisherName))), child), baseEventRate, estimatedEventSize, baseEventRate * estimatedEventSize)
          val publisherDummy = PublisherDummyQuery(s.publisherName) -> (QueryDependencies(None, Some(s)), baseEventRate, estimatedEventSize, baseEventRate * estimatedEventSize)
          mutable.LinkedHashMap(publisherDummy, streamOp)

        case seq: SequenceQuery =>
          mutable.LinkedHashMap(seq -> (QueryDependencies(
            Some(List(PublisherDummyQuery(seq.s1.publisherName), PublisherDummyQuery(seq.s2.publisherName))), child),
            baseEventRate, seq.estimateEventSize, baseEventRate * seq.estimateEventSize))

        case _ => throw new RuntimeException(s"cannot extract dependency operators from $query")
      }
    }

    val res = extractOperatorsRec(query)
    val rootOpEntry = res(query)
    res += ClientDummyQuery() -> (QueryDependencies(Some(List(query)), None), rootOpEntry._2, rootOpEntry._3, rootOpEntry._4)
    res
  }

  def estimateEventRate(operator: Query,
                        parentOperators: Map[Query, (QueryDependencies, EventRateEstimate, EventSizeEstimate, EventBandwidthEstimate)]): Double = {

    // the window size in seconds
    // TODO must treat sliding and tumbling windows separately! see esper doc
    def getWindowSize(w: Window, rate: Double): Double = w match {
      case SlidingInstances(instances) => instances / rate
      case SlidingTime(time) => time
      case TumblingInstances(instances) => instances / rate
      case TumblingTime(time) => time
    }
    def calculateEventRateFromWindows(w1: Window, eventRate1: Double, w2: Window, eventRate2: Double): Double = {
      val (w1Size, w2Size) = (getWindowSize(w1, eventRate1), getWindowSize(w2, eventRate2))
      val window1Volume = w1Size * eventRate1
      val window2Volume = w1Size * eventRate2 // amount of events within window duration
      val eventsTotal = window1Volume * window2Volume * 2 // e.g. @10[Events/s]: 5 * 10 * 5 * 10 = 2500/5 = 500; multiply by 2 since we get this from both parents
      eventsTotal / math.max(w1Size, w2Size) // events/second
    }

    val eventRateToChild: Double = operator match {
      // event rate depends on window size and event rate of parents
      case j: JoinQuery => calculateEventRateFromWindows(j.w1, parentOperators(j.sq1)._2, j.w2, parentOperators(j.sq2)._2)
      case sj: SelfJoinQuery => calculateEventRateFromWindows(sj.w1, parentOperators(sj.sq)._2, sj.w2, parentOperators(sj.sq)._2)
      // an event is sent whenever all parents have sent an event -> slowest parent event rate
      case c: ConjunctionQuery => parentOperators.minBy(_._2._2)._2._2
      // an event is sent whenever either parent sends
      case d: DisjunctionQuery => parentOperators(d.sq1)._2 + parentOperators(d.sq2)._2
      // otherwise use the highest parent event rate
      case _ => parentOperators.maxBy(_._2._2)._2._2
    }
    eventRateToChild
  }

  def estimateOutputBandwidths(operator: Query, baseEventRate: Double): Map[Query, Double] = extractOperators(operator, baseEventRate).map(e => e._1 -> e._2._4).toMap

  sealed trait LeafQuery   extends Query
  sealed trait UnaryQuery  extends Query { val sq: Query }
  sealed trait BinaryQuery extends Query { val sq1: Query; val sq2: Query }
  sealed trait StreamQuery      extends LeafQuery   { val publisherName: String }
  sealed trait SequenceQuery    extends LeafQuery   { val s1: NStream; val s2: NStream }
  sealed trait FilterQuery      extends UnaryQuery  { val cond: Event => Boolean }
  sealed trait DropElemQuery    extends UnaryQuery
  sealed trait SelfJoinQuery    extends UnaryQuery  { val w1: Window; val w2: Window }
  sealed trait AverageQuery     extends UnaryQuery  { val sectionFilter: Option[Int] = None }
  sealed trait ObserveChangeQuery extends UnaryQuery
  sealed trait SlidingWindowQuery extends UnaryQuery { val sectionFilter: Option[Int] = None; val windowSize: Int; val stepSize: Int }
  sealed trait NewAverageQuery extends UnaryQuery
  sealed trait DatabaseJoinQuery extends UnaryQuery { val db: HashMap[Int,Int]}
  sealed trait ConverterQuery extends UnaryQuery
  sealed trait WindowStatisticQuery extends UnaryQuery {val windowSize: Int}
  sealed trait ShrinkingFilterQuery extends UnaryQuery { val cond: Any => Boolean; val emitAlways: Option[Boolean] = None }
  sealed trait JoinQuery        extends BinaryQuery { val w1: Window; val w2: Window }
  sealed trait ConjunctionQuery extends BinaryQuery
  sealed trait DisjunctionQuery extends BinaryQuery

  // TypeTags are used here to make type information about the processed event type available at runtime;
  // this is necessary since scala erases all type information at compile time.
  // The 'types' field contains only the name strings of the types instead of the entire Type,
  // because Kryo throws a ConcurrentModificationException when attempting to serialize Type directly (and the name is sufficient for our uses, i.e. getSampleValue())
  abstract class Query1[A](implicit tagA: TypeTag[A]) extends Query {
    val types = Vector(tagA.tpe.toString)
    def estimateEventSize: Long = SizeEstimator.estimate(Event1(getSampleValue(types.head)))
  }
  abstract class Query2[A, B](implicit tagA: TypeTag[A], tagB: TypeTag[B])             extends Query {
    val types = Vector(tagA.tpe.toString, tagB.tpe.toString)
    def estimateEventSize: Long = SizeEstimator.estimate(Event2(getSampleValue(types(0)), getSampleValue(types(1))))
  }
  abstract class Query3[A, B, C](implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C])          extends Query {
    val types = Vector(tagA.tpe.toString, tagB.tpe.toString, tagC.tpe.toString)
    def estimateEventSize: Long = SizeEstimator.estimate(Event3(getSampleValue(types(0)), getSampleValue(types(1)), getSampleValue(types(2))))
  }
  abstract class Query4[A, B, C, D](implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D])       extends Query {
    val types = Vector(tagA.tpe.toString, tagB.tpe.toString, tagC.tpe.toString, tagD.tpe.toString)
    def estimateEventSize: Long = SizeEstimator.estimate(Event4(getSampleValue(types(0)), getSampleValue(types(1)), getSampleValue(types(2)), getSampleValue(types(3))))
  }
  abstract class Query5[A, B, C, D, E](implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E])       extends Query {
    val types = Vector(tagA.tpe.toString, tagB.tpe.toString, tagC.tpe.toString, tagD.tpe.toString, tagE.tpe.toString)
    def estimateEventSize: Long = SizeEstimator.estimate(Event5(getSampleValue(types(0)), getSampleValue(types(1)), getSampleValue(types(2)), getSampleValue(types(3)), getSampleValue(types(4))))
  }
  abstract class Query6[A, B, C, D, E, F](implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F])       extends Query {
    val types = Vector(tagA.tpe.toString, tagB.tpe.toString, tagC.tpe.toString, tagD.tpe.toString, tagE.tpe.toString, tagF.tpe.toString)
    def estimateEventSize: Long = SizeEstimator.estimate(Event6(getSampleValue(types(0)), getSampleValue(types(1)), getSampleValue(types(2)), getSampleValue(types(3)), getSampleValue(types(4)), getSampleValue(types(5))))
  }

  case class Stream1[A]                (publisherName: String, requirements: Set[Requirement])(implicit tagA: TypeTag[A]) extends Query1[A] with StreamQuery
  case class Stream2[A, B]             (publisherName: String, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B]) extends Query2[A, B]             with StreamQuery
  case class Stream3[A, B, C]          (publisherName: String, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C]) extends Query3[A, B, C]          with StreamQuery
  case class Stream4[A, B, C, D]       (publisherName: String, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query4[A, B, C, D]       with StreamQuery
  case class Stream5[A, B, C, D, E]    (publisherName: String, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query5[A, B, C, D, E]    with StreamQuery
  case class Stream6[A, B, C, D, E, F] (publisherName: String, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with StreamQuery

  case class Sequence11[A, B]             (s1: NStream1[A],             s2: NStream1[B],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B]) extends Query2[A, B]             with SequenceQuery
  case class Sequence12[A, B, C]          (s1: NStream1[A],             s2: NStream2[B, C],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C]) extends Query3[A, B, C]          with SequenceQuery
  case class Sequence21[A, B, C]          (s1: NStream2[A, B],          s2: NStream1[C],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C]) extends Query3[A, B, C]          with SequenceQuery
  case class Sequence13[A, B, C, D]       (s1: NStream1[A],             s2: NStream3[B, C, D],       requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query4[A, B, C, D]       with SequenceQuery
  case class Sequence22[A, B, C, D]       (s1: NStream2[A, B],          s2: NStream2[C, D],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query4[A, B, C, D]       with SequenceQuery
  case class Sequence31[A, B, C, D]       (s1: NStream3[A, B, C],       s2: NStream1[D],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query4[A, B, C, D]       with SequenceQuery
  case class Sequence14[A, B, C, D, E]    (s1: NStream1[A],             s2: NStream4[B, C, D, E],    requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query5[A, B, C, D, E]    with SequenceQuery
  case class Sequence23[A, B, C, D, E]    (s1: NStream2[A, B],          s2: NStream3[C, D, E],       requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query5[A, B, C, D, E]    with SequenceQuery
  case class Sequence32[A, B, C, D, E]    (s1: NStream3[A, B, C],       s2: NStream2[D, E],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query5[A, B, C, D, E]    with SequenceQuery
  case class Sequence41[A, B, C, D, E]    (s1: NStream4[A, B, C, D],    s2: NStream1[E],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query5[A, B, C, D, E]    with SequenceQuery
  case class Sequence15[A, B, C, D, E, F] (s1: NStream1[A],             s2: NStream5[B, C, D, E, F], requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with SequenceQuery
  case class Sequence24[A, B, C, D, E, F] (s1: NStream2[A, B],          s2: NStream4[C, D, E, F],    requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with SequenceQuery
  case class Sequence33[A, B, C, D, E, F] (s1: NStream3[A, B, C],       s2: NStream3[D, E, F],       requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with SequenceQuery
  case class Sequence42[A, B, C, D, E, F] (s1: NStream4[A, B, C, D],    s2: NStream2[E, F],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with SequenceQuery
  case class Sequence51[A, B, C, D, E, F] (s1: NStream5[A, B, C, D, E], s2: NStream1[F],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with SequenceQuery

  case class Filter1[A]                (sq: Query1[A],                cond: Event => Boolean, requirements: Set[Requirement])(implicit tagA: TypeTag[A]) extends Query1[A]                with FilterQuery
  case class Filter2[A, B]             (sq: Query2[A, B],             cond: Event => Boolean, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B]) extends Query2[A, B]             with FilterQuery
  case class Filter3[A, B, C]          (sq: Query3[A, B, C],          cond: Event => Boolean, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C]) extends Query3[A, B, C]          with FilterQuery
  case class Filter4[A, B, C, D]       (sq: Query4[A, B, C, D],       cond: Event => Boolean, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query4[A, B, C, D]       with FilterQuery
  case class Filter5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    cond: Event => Boolean, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query5[A, B, C, D, E]    with FilterQuery
  case class Filter6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], cond: Event => Boolean, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with FilterQuery

  case class DropElem1Of2[A, B]             (sq: Query2[A, B],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B]) extends Query1[B]                               with DropElemQuery
  case class DropElem2Of2[A, B]             (sq: Query2[A, B],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B]) extends Query1[A]                               with DropElemQuery
  case class DropElem1Of3[A, B, C]          (sq: Query3[A, B, C],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C]) extends Query2[B, C]          with DropElemQuery
  case class DropElem2Of3[A, B, C]          (sq: Query3[A, B, C],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C]) extends Query2[A, C]          with DropElemQuery
  case class DropElem3Of3[A, B, C]          (sq: Query3[A, B, C],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C]) extends Query2[A, B]          with DropElemQuery
  case class DropElem1Of4[A, B, C, D]       (sq: Query4[A, B, C, D],       requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query3[B, C, D]       with DropElemQuery
  case class DropElem2Of4[A, B, C, D]       (sq: Query4[A, B, C, D],       requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query3[A, C, D]       with DropElemQuery
  case class DropElem3Of4[A, B, C, D]       (sq: Query4[A, B, C, D],       requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query3[A, B, D]       with DropElemQuery
  case class DropElem4Of4[A, B, C, D]       (sq: Query4[A, B, C, D],       requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query3[A, B, C]       with DropElemQuery
  case class DropElem1Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query4[B, C, D, E]    with DropElemQuery
  case class DropElem2Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query4[A, C, D, E]    with DropElemQuery
  case class DropElem3Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query4[A, B, D, E]    with DropElemQuery
  case class DropElem4Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query4[A, B, C, E]    with DropElemQuery
  case class DropElem5Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query4[A, B, C, D]    with DropElemQuery
  case class DropElem1Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query5[B, C, D, E, F] with DropElemQuery
  case class DropElem2Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query5[A, C, D, E, F] with DropElemQuery
  case class DropElem3Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query5[A, B, D, E, F] with DropElemQuery
  case class DropElem4Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query5[A, B, C, E, F] with DropElemQuery
  case class DropElem5Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query5[A, B, C, D, F] with DropElemQuery
  case class DropElem6Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query5[A, B, C, D, E] with DropElemQuery

  case class SelfJoin11[A]       (sq: Query1[A],       w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A]) extends Query2[A, A]             with SelfJoinQuery
  case class SelfJoin22[A, B]    (sq: Query2[A, B],    w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B]) extends Query4[A, B, A, B]       with SelfJoinQuery
  case class SelfJoin33[A, B, C] (sq: Query3[A, B, C], w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C]) extends Query6[A, B, C, A, B, C] with SelfJoinQuery

  case class Join11[A, B]             (sq1: Query1[A],             sq2: Query1[B],             w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B]) extends Query2[A, B]             with JoinQuery
  case class Join12[A, B, C]          (sq1: Query1[A],             sq2: Query2[B, C],          w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C]) extends Query3[A, B, C]          with JoinQuery
  case class Join21[A, B, C]          (sq1: Query2[A, B],          sq2: Query1[C],             w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C]) extends Query3[A, B, C]          with JoinQuery
  case class Join13[A, B, C, D]       (sq1: Query1[A],             sq2: Query3[B, C, D],       w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query4[A, B, C, D]       with JoinQuery
  case class Join22[A, B, C, D]       (sq1: Query2[A, B],          sq2: Query2[C, D],          w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query4[A, B, C, D]       with JoinQuery
  case class Join31[A, B, C, D]       (sq1: Query3[A, B, C],       sq2: Query1[D],             w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query4[A, B, C, D]       with JoinQuery
  case class Join14[A, B, C, D, E]    (sq1: Query1[A],             sq2: Query4[B, C, D, E],    w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query5[A, B, C, D, E]    with JoinQuery
  case class Join23[A, B, C, D, E]    (sq1: Query2[A, B],          sq2: Query3[C, D, E],       w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query5[A, B, C, D, E]    with JoinQuery
  case class Join32[A, B, C, D, E]    (sq1: Query3[A, B, C],       sq2: Query2[D, E],          w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query5[A, B, C, D, E]    with JoinQuery
  case class Join41[A, B, C, D, E]    (sq1: Query4[A, B, C, D],    sq2: Query1[E],             w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query5[A, B, C, D, E]    with JoinQuery
  case class Join15[A, B, C, D, E, F] (sq1: Query1[A],             sq2: Query5[B, C, D, E, F], w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with JoinQuery
  case class Join24[A, B, C, D, E, F] (sq1: Query2[A, B],          sq2: Query4[C, D, E, F],    w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with JoinQuery
  case class Join33[A, B, C, D, E, F] (sq1: Query3[A, B, C],       sq2: Query3[D, E, F],       w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with JoinQuery
  case class Join42[A, B, C, D, E, F] (sq1: Query4[A, B, C, D],    sq2: Query2[E, F],          w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with JoinQuery
  case class Join51[A, B, C, D, E, F] (sq1: Query5[A, B, C, D, E], sq2: Query1[F],             w1: Window, w2: Window, requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with JoinQuery

  case class Conjunction11[A, B]             (sq1: Query1[A],             sq2: Query1[B],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B]) extends Query2[A, B]             with ConjunctionQuery
  case class Conjunction12[A, B, C]          (sq1: Query1[A],             sq2: Query2[B, C],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C]) extends Query3[A, B, C]          with ConjunctionQuery
  case class Conjunction21[A, B, C]          (sq1: Query2[A, B],          sq2: Query1[C],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C]) extends Query3[A, B, C]          with ConjunctionQuery
  case class Conjunction13[A, B, C, D]       (sq1: Query1[A],             sq2: Query3[B, C, D],       requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query4[A, B, C, D]       with ConjunctionQuery
  case class Conjunction22[A, B, C, D]       (sq1: Query2[A, B],          sq2: Query2[C, D],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query4[A, B, C, D]       with ConjunctionQuery
  case class Conjunction31[A, B, C, D]       (sq1: Query3[A, B, C],       sq2: Query1[D],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query4[A, B, C, D]       with ConjunctionQuery
  case class Conjunction14[A, B, C, D, E]    (sq1: Query1[A],             sq2: Query4[B, C, D, E],    requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query5[A, B, C, D, E]    with ConjunctionQuery
  case class Conjunction23[A, B, C, D, E]    (sq1: Query2[A, B],          sq2: Query3[C, D, E],       requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query5[A, B, C, D, E]    with ConjunctionQuery
  case class Conjunction32[A, B, C, D, E]    (sq1: Query3[A, B, C],       sq2: Query2[D, E],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query5[A, B, C, D, E]    with ConjunctionQuery
  case class Conjunction41[A, B, C, D, E]    (sq1: Query4[A, B, C, D],    sq2: Query1[E],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query5[A, B, C, D, E]    with ConjunctionQuery
  case class Conjunction15[A, B, C, D, E, F] (sq1: Query1[A],             sq2: Query5[B, C, D, E, F], requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with ConjunctionQuery
  case class Conjunction24[A, B, C, D, E, F] (sq1: Query2[A, B],          sq2: Query4[C, D, E, F],    requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with ConjunctionQuery
  case class Conjunction33[A, B, C, D, E, F] (sq1: Query3[A, B, C],       sq2: Query3[D, E, F],       requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with ConjunctionQuery
  case class Conjunction42[A, B, C, D, E, F] (sq1: Query4[A, B, C, D],    sq2: Query2[E, F],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with ConjunctionQuery
  case class Conjunction51[A, B, C, D, E, F] (sq1: Query5[A, B, C, D, E], sq2: Query1[F],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query6[A, B, C, D, E, F] with ConjunctionQuery

  private type X = Unit
  case class Disjunction11[A, B]                               (sq1: Query1[A],                sq2: Query1[B],                requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B]) extends Query1[Either[A, B]]                                                                       with DisjunctionQuery
  case class Disjunction12[A, B, C]                            (sq1: Query1[A],                sq2: Query2[B, C],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C]) extends Query2[Either[A, B], Either[X, C]]                                                         with DisjunctionQuery
  case class Disjunction13[A, B, C, D]                         (sq1: Query1[A],                sq2: Query3[B, C, D],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query3[Either[A, B], Either[X, C], Either[X, D]]                                           with DisjunctionQuery
  case class Disjunction14[A, B, C, D, E]                      (sq1: Query1[A],                sq2: Query4[B, C, D, E],       requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query4[Either[A, B], Either[X, C], Either[X, D], Either[X, E]]                             with DisjunctionQuery
  case class Disjunction15[A, B, C, D, E, F]                   (sq1: Query1[A],                sq2: Query5[B, C, D, E, F],    requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query5[Either[A, B], Either[X, C], Either[X, D], Either[X, E], Either[X, F]]               with DisjunctionQuery
  case class Disjunction16[A, B, C, D, E, F, G]                (sq1: Query1[A],                sq2: Query6[B, C, D, E, F, G], requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G]) extends Query6[Either[A, B], Either[X, C], Either[X, D], Either[X, E], Either[X, F], Either[X, G]] with DisjunctionQuery
  case class Disjunction21[A, B, C]                            (sq1: Query2[A, B],             sq2: Query1[C],                requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C]) extends Query2[Either[A, C], Either[B, X]]                                                         with DisjunctionQuery
  case class Disjunction22[A, B, C, D]                         (sq1: Query2[A, B],             sq2: Query2[C, D],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query2[Either[A, C], Either[B, D]]                                                         with DisjunctionQuery
  case class Disjunction23[A, B, C, D, E]                      (sq1: Query2[A, B],             sq2: Query3[C, D, E],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query3[Either[A, C], Either[B, D], Either[X, E]]                                           with DisjunctionQuery
  case class Disjunction24[A, B, C, D, E, F]                   (sq1: Query2[A, B],             sq2: Query4[C, D, E, F],       requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query4[Either[A, C], Either[B, D], Either[X, E], Either[X, F]]                             with DisjunctionQuery
  case class Disjunction25[A, B, C, D, E, F, G]                (sq1: Query2[A, B],             sq2: Query5[C, D, E, F, G],    requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G]) extends Query5[Either[A, C], Either[B, D], Either[X, E], Either[X, F], Either[X, G]]               with DisjunctionQuery
  case class Disjunction26[A, B, C, D, E, F, G, H]             (sq1: Query2[A, B],             sq2: Query6[C, D, E, F, G, H], requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G], tagH: TypeTag[H]) extends Query6[Either[A, C], Either[B, D], Either[X, E], Either[X, F], Either[X, G], Either[X, H]] with DisjunctionQuery
  case class Disjunction31[A, B, C, D]                         (sq1: Query3[A, B, C],          sq2: Query1[D],                requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D]) extends Query3[Either[A, D], Either[B, X], Either[C, X]]                                           with DisjunctionQuery
  case class Disjunction32[A, B, C, D, E]                      (sq1: Query3[A, B, C],          sq2: Query2[D, E],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query3[Either[A, D], Either[B, E], Either[C, X]]                                           with DisjunctionQuery
  case class Disjunction33[A, B, C, D, E, F]                   (sq1: Query3[A, B, C],          sq2: Query3[D, E, F],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query3[Either[A, D], Either[B, E], Either[C, F]]                                           with DisjunctionQuery
  case class Disjunction34[A, B, C, D, E, F, G]                (sq1: Query3[A, B, C],          sq2: Query4[D, E, F, G],       requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G]) extends Query4[Either[A, D], Either[B, E], Either[C, F], Either[X, G]]                             with DisjunctionQuery
  case class Disjunction35[A, B, C, D, E, F, G, H]             (sq1: Query3[A, B, C],          sq2: Query5[D, E, F, G, H],    requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G], tagH: TypeTag[H]) extends Query5[Either[A, D], Either[B, E], Either[C, F], Either[X, G], Either[X, H]]               with DisjunctionQuery
  case class Disjunction36[A, B, C, D, E, F, G, H, I]          (sq1: Query3[A, B, C],          sq2: Query6[D, E, F, G, H, I], requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G], tagH: TypeTag[H], tagI: TypeTag[I]) extends Query6[Either[A, D], Either[B, E], Either[C, F], Either[X, G], Either[X, H], Either[X, I]] with DisjunctionQuery
  case class Disjunction41[A, B, C, D, E]                      (sq1: Query4[A, B, C, D],       sq2: Query1[E],                requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E]) extends Query4[Either[A, E], Either[B, X], Either[C, X], Either[D, X]]                             with DisjunctionQuery
  case class Disjunction42[A, B, C, D, E, F]                   (sq1: Query4[A, B, C, D],       sq2: Query2[E, F],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query4[Either[A, E], Either[B, F], Either[C, X], Either[D, X]]                             with DisjunctionQuery
  case class Disjunction43[A, B, C, D, E, F, G]                (sq1: Query4[A, B, C, D],       sq2: Query3[E, F, G],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G]) extends Query4[Either[A, E], Either[B, F], Either[C, G], Either[D, X]]                             with DisjunctionQuery
  case class Disjunction44[A, B, C, D, E, F, G, H]             (sq1: Query4[A, B, C, D],       sq2: Query4[E, F, G, H],       requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G], tagH: TypeTag[H]) extends Query4[Either[A, E], Either[B, F], Either[C, G], Either[D, H]]                             with DisjunctionQuery
  case class Disjunction45[A, B, C, D, E, F, G, H, I]          (sq1: Query4[A, B, C, D],       sq2: Query5[E, F, G, H, I],    requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G], tagH: TypeTag[H], tagI: TypeTag[I]) extends Query5[Either[A, E], Either[B, F], Either[C, G], Either[D, H], Either[X, I]]               with DisjunctionQuery
  case class Disjunction46[A, B, C, D, E, F, G, H, I, J]       (sq1: Query4[A, B, C, D],       sq2: Query6[E, F, G, H, I, J], requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G], tagH: TypeTag[H], tagI: TypeTag[I], tagJ: TypeTag[J]) extends Query6[Either[A, E], Either[B, F], Either[C, G], Either[D, H], Either[X, I], Either[X, J]] with DisjunctionQuery
  case class Disjunction51[A, B, C, D, E, F]                   (sq1: Query5[A, B, C, D, E],    sq2: Query1[F],                requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F]) extends Query5[Either[A, F], Either[B, X], Either[C, X], Either[D, X], Either[E, X]]               with DisjunctionQuery
  case class Disjunction52[A, B, C, D, E, F, G]                (sq1: Query5[A, B, C, D, E],    sq2: Query2[F, G],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G]) extends Query5[Either[A, F], Either[B, G], Either[C, X], Either[D, X], Either[E, X]]               with DisjunctionQuery
  case class Disjunction53[A, B, C, D, E, F, G, H]             (sq1: Query5[A, B, C, D, E],    sq2: Query3[F, G, H],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G], tagH: TypeTag[H]) extends Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, X], Either[E, X]]               with DisjunctionQuery
  case class Disjunction54[A, B, C, D, E, F, G, H, I]          (sq1: Query5[A, B, C, D, E],    sq2: Query4[F, G, H, I],       requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G], tagH: TypeTag[H], tagI: TypeTag[I]) extends Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, X]]               with DisjunctionQuery
  case class Disjunction55[A, B, C, D, E, F, G, H, I, J]       (sq1: Query5[A, B, C, D, E],    sq2: Query5[F, G, H, I, J],    requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G], tagH: TypeTag[H], tagI: TypeTag[I], tagJ: TypeTag[J]) extends Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J]]               with DisjunctionQuery
  case class Disjunction56[A, B, C, D, E, F, G, H, I, J, K]    (sq1: Query5[A, B, C, D, E],    sq2: Query6[F, G, H, I, J, K], requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G], tagH: TypeTag[H], tagI: TypeTag[I], tagJ: TypeTag[J], tagK: TypeTag[K]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[X, K]] with DisjunctionQuery
  case class Disjunction61[A, B, C, D, E, F, G]                (sq1: Query6[A, B, C, D, E, F], sq2: Query1[G],                requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G]) extends Query6[Either[A, F], Either[B, X], Either[C, X], Either[D, X], Either[E, X], Either[F, X]] with DisjunctionQuery
  case class Disjunction62[A, B, C, D, E, F, G, H]             (sq1: Query6[A, B, C, D, E, F], sq2: Query2[G, H],             requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G], tagH: TypeTag[H]) extends Query6[Either[A, F], Either[B, G], Either[C, X], Either[D, X], Either[E, X], Either[F, X]] with DisjunctionQuery
  case class Disjunction63[A, B, C, D, E, F, G, H, I]          (sq1: Query6[A, B, C, D, E, F], sq2: Query3[G, H, I],          requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G], tagH: TypeTag[H], tagI: TypeTag[I]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, X], Either[E, X], Either[F, X]] with DisjunctionQuery
  case class Disjunction64[A, B, C, D, E, F, G, H, I, J]       (sq1: Query6[A, B, C, D, E, F], sq2: Query4[G, H, I, J],       requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G], tagH: TypeTag[H], tagI: TypeTag[I], tagJ: TypeTag[J]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, X], Either[F, X]] with DisjunctionQuery
  case class Disjunction65[A, B, C, D, E, F, G, H, I, J, K]    (sq1: Query6[A, B, C, D, E, F], sq2: Query5[G, H, I, J, K],    requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G], tagH: TypeTag[H], tagI: TypeTag[I], tagJ: TypeTag[J], tagK: TypeTag[K]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[F, X]] with DisjunctionQuery
  case class Disjunction66[A, B, C, D, E, F, G, H, I, J, K, L] (sq1: Query6[A, B, C, D, E, F], sq2: Query6[G, H, I, J, K, L], requirements: Set[Requirement])(implicit tagA: TypeTag[A], tagB: TypeTag[B], tagC: TypeTag[C], tagD: TypeTag[D], tagE: TypeTag[E], tagF: TypeTag[F], tagG: TypeTag[G], tagH: TypeTag[H], tagI: TypeTag[I], tagJ: TypeTag[J], tagK: TypeTag[K], tagL: TypeTag[L]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[F, K]] with DisjunctionQuery

  case class ObserveChange1[A] (sq: Query1[A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with ObserveChangeQuery
  case class ObserveChange2[A] (sq: Query2[A, A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with ObserveChangeQuery
  case class ObserveChange3[A] (sq: Query3[A, A, A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with ObserveChangeQuery
  case class ObserveChange4[A] (sq: Query4[A, A, A, A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with ObserveChangeQuery
  case class ObserveChange5[A] (sq: Query5[A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with ObserveChangeQuery
  case class ObserveChange6[A] (sq: Query6[A, A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with ObserveChangeQuery

  case class SlidingWindow1[A] (sq: Query1[A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None, override val windowSize: Int, override val stepSize: Int = 1)(implicit tagA: TypeTag[A]) extends Query1[A] with SlidingWindowQuery
  case class SlidingWindow2[A] (sq: Query2[A, A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None, override val windowSize: Int, override val stepSize: Int = 1)(implicit tagA: TypeTag[A]) extends Query1[A] with SlidingWindowQuery
  case class SlidingWindow3[A] (sq: Query3[A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None, override val windowSize: Int, override val stepSize: Int = 1)(implicit tagA: TypeTag[A]) extends Query1[A] with SlidingWindowQuery
  case class SlidingWindow4[A] (sq: Query4[A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None, override val windowSize: Int, override val stepSize: Int = 1)(implicit tagA: TypeTag[A]) extends Query1[A] with SlidingWindowQuery
  case class SlidingWindow5[A] (sq: Query5[A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None, override val windowSize: Int, override val stepSize: Int = 1)(implicit tagA: TypeTag[A]) extends Query1[A] with SlidingWindowQuery
  case class SlidingWindow6[A] (sq: Query6[A, A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None, override val windowSize: Int, override val stepSize: Int = 1)(implicit tagA: TypeTag[A]) extends Query1[A] with SlidingWindowQuery

  case class NewAverage1[A] (sq: Query1[A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with NewAverageQuery
  case class NewAverage2[A] (sq: Query2[A, A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with NewAverageQuery
  case class NewAverage3[A] (sq: Query3[A, A, A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with NewAverageQuery
  case class NewAverage4[A] (sq: Query4[A, A, A, A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with NewAverageQuery
  case class NewAverage5[A] (sq: Query5[A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with NewAverageQuery
  case class NewAverage6[A] (sq: Query6[A, A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with NewAverageQuery

  case class ShrinkFilter2[A] (sq: Query2[A, A], requirements: Set[Requirement] = Set[Requirement](), cond: Any => Boolean, override val emitAlways: Option[Boolean] = None)(implicit tagA: TypeTag[A]) extends Query1[A] with ShrinkingFilterQuery
  case class ShrinkFilter3[A] (sq: Query3[A, A, A], requirements: Set[Requirement] = Set[Requirement](), cond: Any => Boolean, override val emitAlways: Option[Boolean] = None)(implicit tagA: TypeTag[A]) extends Query1[A] with ShrinkingFilterQuery
  case class ShrinkFilter4[A] (sq: Query4[A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), cond: Any => Boolean, override val emitAlways: Option[Boolean] = None)(implicit tagA: TypeTag[A]) extends Query1[A] with ShrinkingFilterQuery
  case class ShrinkFilter5[A] (sq: Query5[A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), cond: Any => Boolean, override val emitAlways: Option[Boolean] = None)(implicit tagA: TypeTag[A]) extends Query1[A] with ShrinkingFilterQuery
  case class ShrinkFilter6[A] (sq: Query6[A, A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), cond: Any => Boolean, override val emitAlways: Option[Boolean] = None)(implicit tagA: TypeTag[A]) extends Query1[A] with ShrinkingFilterQuery

  case class DatabaseJoin1[A] ( sq: Query1[A], requirements: Set[Requirement] = Set[Requirement](), override val db: mutable.HashMap[Int, Int])(implicit tagA: TypeTag[A]) extends Query1[A] with DatabaseJoinQuery
  case class DatabaseJoin2[A] ( sq: Query2[A, A], requirements: Set[Requirement] = Set[Requirement](), override val db: mutable.HashMap[Int, Int])(implicit tagA: TypeTag[A]) extends Query1[A] with DatabaseJoinQuery
  case class DatabaseJoin3[A] ( sq: Query3[A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val db: mutable.HashMap[Int, Int])(implicit tagA: TypeTag[A]) extends Query1[A] with DatabaseJoinQuery
  case class DatabaseJoin4[A] ( sq: Query4[A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val db: mutable.HashMap[Int, Int])(implicit tagA: TypeTag[A]) extends Query1[A] with DatabaseJoinQuery
  case class DatabaseJoin5[A] ( sq: Query5[A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val db: mutable.HashMap[Int, Int])(implicit tagA: TypeTag[A]) extends Query1[A] with DatabaseJoinQuery
  case class DatabaseJoin6[A] ( sq: Query6[A, A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val db: mutable.HashMap[Int, Int])(implicit tagA: TypeTag[A]) extends Query1[A] with DatabaseJoinQuery

  case class Convert1[A] ( sq: Query1[A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with ConverterQuery
  case class Convert2[A] ( sq: Query2[A, A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with ConverterQuery
  case class Convert3[A] ( sq: Query3[A, A, A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with ConverterQuery
  case class Convert4[A] ( sq: Query4[A, A, A, A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with ConverterQuery
  case class Convert5[A] ( sq: Query5[A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with ConverterQuery
  case class Convert6[A] ( sq: Query6[A, A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement]())(implicit tagA: TypeTag[A]) extends Query1[A] with ConverterQuery

  case class WindowStatistic1[A] ( sq: Query1[A], requirements: Set[Requirement] = Set[Requirement](), override val windowSize: Int)(implicit tagA: TypeTag[A]) extends Query1[A] with WindowStatisticQuery
  case class WindowStatistic2[A] ( sq: Query2[A, A], requirements: Set[Requirement] = Set[Requirement](), override val windowSize: Int)(implicit tagA: TypeTag[A]) extends Query1[A] with WindowStatisticQuery
  case class WindowStatistic3[A] ( sq: Query3[A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val windowSize: Int)(implicit tagA: TypeTag[A]) extends Query1[A] with WindowStatisticQuery
  case class WindowStatistic4[A] ( sq: Query4[A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val windowSize: Int)(implicit tagA: TypeTag[A]) extends Query1[A] with WindowStatisticQuery
  case class WindowStatistic5[A] ( sq: Query5[A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val windowSize: Int)(implicit tagA: TypeTag[A]) extends Query1[A] with WindowStatisticQuery
  case class WindowStatistic6[A] ( sq: Query6[A, A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val windowSize: Int)(implicit tagA: TypeTag[A]) extends Query1[A] with WindowStatisticQuery

  case class Average2[A] (sq: Query2[A, A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None)(implicit tagA: TypeTag[A]) extends Query1[A] with AverageQuery
  case class Average4[A] (sq: Query4[A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None)(implicit tagA: TypeTag[A]) extends Query1[A] with AverageQuery
  case class Average6[A] (sq: Query6[A, A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None)(implicit tagA: TypeTag[A]) extends Query1[A] with AverageQuery
  // a join of six unary queries
  //case class SenaryJoin[A, B, C, D, E, F] (sq1: Query1[A], sq2: Query1[B], sq3: Query1[C], sq4: Query1[D], sq5: Query1[E], sq6: Query1[F], w1: Window, w2: Window, w3: Window, w4: Window, w5: Window, w6: Window, requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with SenaryJoinQuery
  // only to be used in PlacementStrategy
  case class PublisherDummyQuery(p: String, requirements: Set[Requirement] = Set()) extends LeafQuery { def estimateEventSize: Long = 0; val types = Vector() }
  case class ClientDummyQuery(requirements: Set[Requirement] = Set()) extends LeafQuery { def estimateEventSize: Long = 0; val types = Vector() }


}
