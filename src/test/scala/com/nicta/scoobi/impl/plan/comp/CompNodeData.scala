package com.nicta.scoobi
package impl
package plan
package comp

import org.scalacheck.{Arbitrary, Gen}
import org.specs2.ScalaCheck
import org.specs2.main.CommandLineArguments
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import org.kiama.attribution.Attribution

import data.Data
import core._
import com.nicta.scoobi.io.ConstantStringDataSource
import application._
import mapreducer._
import WireFormat._

trait CompNodeData extends Data with ScalaCheck with CommandLineArguments with CompNodeFactory { this: Specification =>

  /**
   * Arbitrary instance for a CompNode
   */
  import scalaz.Scalaz._

  override def defaultValues = Map(minTestsOk   -> arguments.commandLine.int("mintestsok").getOrElse(1000),
                                   maxSize      -> arguments.commandLine.int("maxsize").getOrElse(8),
                                   minSize      -> arguments.commandLine.int("minsize").getOrElse(1),
                                   maxDiscarded -> arguments.commandLine.int("maxdiscarded").getOrElse(50),
                                   workers      -> arguments.commandLine.int("workers").getOrElse(1))

  import Gen._
  implicit lazy val arbitraryCompNode: Arbitrary[CompNode] = Arbitrary(arbitraryDList.arbitrary.map(_.getComp))
  implicit lazy val arbitraryDList: Arbitrary[DList[String]] = Arbitrary(Gen.sized(depth => genList(depth).map(_.setComp(c => init(c)).map(_.toString))))
  implicit lazy val arbitraryDObject: Arbitrary[DObject[String]] = Arbitrary(Gen.sized(depth => genObject(depth)))

  /** lists of elements of any type */
  def genList(depth: Int = 1): Gen[DList[_]] = Gen.oneOf(genList1(depth), genList2(depth), genList3(depth))

  /** lists of elements with a simple type A */
  def genList1(depth: Int = 1): Gen[DList[String]] =
    if (depth <= 1) Gen.value(new DListImpl[String](load))
    else            Gen.oneOf(genList1(depth - 1).map(l => l.map(identity)),
                              genList2(depth - 1).map(l => l.map(_._1)),
                              genList3(depth - 1).map(l => l.map(_._1)),
                              ^(genList1(depth / 2), genList1(depth / 2))((_ ++ _))).memo

  /** objects of elements with a simple type A */
  def genObject(depth: Int = 1): Gen[DObject[String]] =
    if (depth <= 1) DObjects[String]("start")
    else            Gen.oneOf(genList(depth - 1).map(l => l.materialize.map(_.toString)),
                              ^(genObject(depth / 2), genObject(depth / 2))((_ join _)).map(o => o.map(_.toString))).memo

  /** lists of elements with a type (K, V) */
  def genList2(depth: Int = 1): Gen[DList[(String, String)]] =
    if (depth <= 1) genList1(1).map(l => l.map(_.partition(c => c > 'a')))
    else            Gen.oneOf(genList1(depth - 1).map(l => l.map(_.partition(c => c > 'a'))),
                              genList2(depth - 1).map(l => l.map(identity)),
                              genList3(depth - 1).map(l => l.combine((_:String) ++ (_:String))),
                              ^(genList2(depth / 2), genList2(depth / 2))((_ ++ _)),
                              ^(genObject(depth / 2), genList1(depth / 2))((_ join _))).memo

  /** lists of elements with a type (K, Iterable[V]) */
  def genList3(depth: Int = 1): Gen[DList[(String, Iterable[String])]] =
    if (depth <= 1) genList2(1).map(l => l.map { case (k, v) => (k, Seq.fill(2)(v)) })
    else            Gen.oneOf(genList2(depth - 1).map(l => l.groupByKey),
                              genList3(depth - 1).map(l => l.map(identity))).memo
}

/**
 * Creation functions
 */
trait CompNodeFactory extends Scope {

  implicit def manifestWireFormatString = manifestWireFormat[String]
  def mapReducer = SimpleMapReducer(manifestWireFormatString)

  def load                                   = Load(ConstantStringDataSource("start"), mapReducer)
  def flatten[A](nodes: CompNode*)           = Flatten(nodes.toList.map(_.asInstanceOf[DComp[A]]), mapReducer)
  def parallelDo(in: CompNode)               = pd(in)
  def rt                                     = Return("", mapReducer)
  def cb(in: CompNode)                       = Combine[String, String](in.asInstanceOf[DComp[(String, Iterable[String])]], (s1: String, s2: String) => s1 + s2,
                                                                       KeyValueMapReducer(manifestWireFormat[String], grouping[String], manifestWireFormat[String]))
  def gbk(in: CompNode)                      = GroupByKey(in.asInstanceOf[DComp[(String,String)]],
                                                          KeyValuesMapReducer(manifestWireFormat[String], grouping[String], manifestWireFormat[String]))

  def mt(in: CompNode)                       = Materialize(in.asInstanceOf[DComp[String]], mapReducer)
  def op(in1: CompNode, in2: CompNode)       = Op[String, String, String](in1.asInstanceOf[DComp[String]], in2.asInstanceOf[DComp[String]], (a, b) => a, mapReducer)

  def pd(in: CompNode, env: CompNode = rt, groupBarrier: Boolean = false, fuseBarrier: Boolean = false) =
    ParallelDo[String, String, String](in.asInstanceOf[DComp[String]], env.asInstanceOf[DComp[String]], fn,
                                     DoMapReducer(manifestWireFormat[String], manifestWireFormat[String], manifestWireFormat[String]), Seq(),
                                     Barriers(groupBarrier, fuseBarrier))

  lazy val fn = new EnvDoFn[String, String, String] {
    def setup(env: String) {}
    def cleanup(env: String, emitter: Emitter[String]) {}
    def process(env: String, input: String, emitter: Emitter[String]) { emitter.emit(input) }
  }

  /** initialize the Kiama attributes of a CompNode */
  def init[T <: CompNode](t: T): T  = { if (!t.children.hasNext) Attribution.initTree(t); t }

}
