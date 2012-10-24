package com.nicta.scoobi
package impl
package plan
package comp

import data.Data
import io.ConstantStringDataSource
import core.{ManifestWireFormat, WireFormat, Emitter, BasicDoFn}
import org.scalacheck.{Arbitrary, Gen}
import org.specs2.ScalaCheck
import org.specs2.main.CommandLineArguments
import org.specs2.mutable.Specification
import org.kiama.attribution.Attribution
import CompNode._
import control.Functions._
import WireFormat._
import org.specs2.specification.Scope

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
  implicit lazy val arbitraryCompNode: Arbitrary[CompNode] = Arbitrary(Gen.sized(depth => genDComp(depth).map(init)))
  implicit lazy val arbitraryParallelDo: Arbitrary[ParallelDo[_,_,_]] = Arbitrary(Gen.sized(depth => genParallelDo(depth).map(init)))

  def genDComp(depth: Int = 1): Gen[CompNode] = lzy(frequency[CompNode]((3, genLoad(depth)),
                                                              (4, genParallelDo(depth)),
                                                              (4, genGroupByKey(depth)),
                                                              (3, genMaterialize(depth)),
                                                              (3, genCombine(depth)),
                                                              (5, genFlatten(depth)),
                                                              (2, genOp(depth)),
                                                              (2, genReturn(depth))).filter(!isCyclic))

  def genLoad       (depth: Int = 1): Gen[CompNode] = Gen.oneOf(load, load)
  def genReturn     (depth: Int = 1): Gen[CompNode] = Gen.oneOf(rt, rt)
  def genFlatten    (depth: Int = 1): Gen[CompNode] = if (depth <= 1) Gen.value(flatten(load)   ) else memo(choose(1, 3).flatMap(n => listOfN(n, genDComp(depth - 1))).map(l => flatten(l:_*)))
  def genCombine    (depth: Int = 1): Gen[CompNode] = if (depth <= 1) Gen.value(cb(load)        ) else memo(genDComp(depth - 1) map (cb _))
  def genOp         (depth: Int = 1): Gen[CompNode] = if (depth <= 1) Gen.value(op(load, load)  ) else memo(^(genDComp(depth - 1), genDComp(depth - 1))((op _)))
  def genMaterialize(depth: Int = 1): Gen[CompNode] = if (depth <= 1) Gen.value(mt(load)        ) else memo(genDComp(depth - 1) map (mt _))
  def genGroupByKey (depth: Int = 1): Gen[CompNode] = if (depth <= 1) Gen.value(gbk(load)       ) else memo(genDComp(depth - 1) map (gbk _))
  def genParallelDo (depth: Int = 1): Gen[ParallelDo[_,_,_]] =
    if (depth <= 1) Gen.value(parallelDo(load)) else memo(Gen.oneOf(genLoad(depth-1),
                                                                    genCombine(depth-1),
                                                                    genParallelDo(depth-1),
                                                                    genGroupByKey(depth-1),
                                                                    genFlatten(depth-1)).map(parallelDo _))

}

/**
 * Creation functions
 */
trait CompNodeFactory extends Scope {

  def load                                   = Load(StraightIO(manifestWireFormat[String], source = ConstantStringDataSource("start")))
  def flatten[A](nodes: CompNode*)           = Flatten(nodes.toList.map(_.asInstanceOf[DComp[A]]), StraightIO(manifestWireFormat[String]))
  def parallelDo(in: CompNode)               = pd(in)
  def rt                                     = Return("", StraightIO(manifestWireFormat[String]))
  def cb(in: CompNode)                       = Combine[String, String](in.asInstanceOf[DComp[(String, Iterable[String])]], (s1: String, s2: String) => s1 + s2,
                                                                       KeyValueIO(manifestWireFormat[String], grouping[String], manifestWireFormat[String]))
  def gbk(in: CompNode)                      = GroupByKey(in.asInstanceOf[DComp[(String,String)]],
                                                          KeyValuesIO(manifestWireFormat[String], grouping[String], manifestWireFormat[String]))

  def mt(in: CompNode)                       = Materialize(in.asInstanceOf[DComp[String]], StraightIO(manifestWireFormat[String]))
  def op(in1: CompNode, in2: CompNode)       = Op[String, String, String](in1.asInstanceOf[DComp[String]], in2.asInstanceOf[DComp[String]], (a, b) => a, StraightIO(manifestWireFormat[String]))
  def pd(in: CompNode, env: CompNode = rt, groupBarrier: Boolean = false, fuseBarrier: Boolean = false) =
    ParallelDo[String, String, Unit](in.asInstanceOf[DComp[String]], env.asInstanceOf[DComp[Unit]], fn,
                                     DoIO(manifestWireFormat[String], manifestWireFormat[String], manifestWireFormat[Unit]),
                                     Barriers(groupBarrier, fuseBarrier))

  lazy val fn = new BasicDoFn[String, String] { def process(input: String, emitter: Emitter[String]) { emitter.emit(input) } }

  /** initialize the Kiama attributes of a CompNode */
  def init[T <: CompNode](t: T): T  = { if (!t.children.hasNext) Attribution.initTree(t); t }

}
