/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobi
package impl
package plan
package comp

import data.Data
import org.scalacheck.{Arbitrary, Gen}
import org.specs2._
import matcher.ScalaCheckMatchers
import specification.Scope
import main.CommandLineArguments

import core._, Reduction._
import com.nicta.scoobi.io.ConstantStringDataSource
import application._
import WireFormat._
import org.kiama.rewriting.Rewriter._
import CompNodeData._

trait CompNodeData extends Data with ScalaCheckMatchers with CommandLineArguments with CompNodeFactory { outer =>

  /**
   * Arbitrary instance for a CompNode
   */
  import scalaz.Scalaz._

  override def defaultValues = Map(
    minTestsOk   -> arguments.commandLine.int("mintestsok").  getOrElse(1000),
    maxSize      -> arguments.commandLine.int("maxsize").     getOrElse(8),
    minSize      -> arguments.commandLine.int("minsize").     getOrElse(1),
    maxDiscarded -> arguments.commandLine.int("maxdiscarded").getOrElse(50),
    workers      -> arguments.commandLine.int("workers").     getOrElse(1))

  import Gen._
  implicit lazy val arbitraryCompNode: Arbitrary[CompNode]        = Arbitrary(arbitraryDList.arbitrary.map(_.getComp).map(CompNodes.reinitAttributable))

  implicit lazy val arbitraryDList:    Arbitrary[DList[String]]   = Arbitrary(Gen.sized(depth => genList(depth).map(_.map(normalise))))
  implicit lazy val arbitraryDObject:  Arbitrary[DObject[String]] = Arbitrary(Gen.sized(depth => genObject(depth)))

  implicit lazy val groupByKey: Arbitrary[GroupByKey] =
    Arbitrary(arbitraryDList.arbitrary.map(_.map(_.partition(_ > 'a')).groupByKey.getComp.asInstanceOf[GroupByKey]))

  /** lists of elements of any type */
  def genList(depth: Int = 1): Gen[DList[_]] = Gen.oneOf(genList1(depth), genList2(depth), genList3(depth))

  /** lists of elements with a simple type A */
  def genList1(depth: Int = 1): Gen[DList[String]] =
    if (depth <= 1) Gen.value(DList("source"))
    else            Gen.oneOf(genList1(depth - 1).map(l => l.map(identity)),
                              genList2(depth - 1).map(l => l.map(_._1)),
                              genList3(depth - 1).map(l => l.map(_._1)),
                              ^(genList1(depth / 2), genList1(depth / 2))((_ ++ _))).memo

  /** objects of elements with a simple type A */
  def genObject(depth: Int = 1): Gen[DObject[String]] =
    if (depth <= 1) DObjects("start")
    else            Gen.oneOf(genList(depth - 1).map(l => l.materialise.map(normalise)),
                              ^(genObject(depth / 2), genObject(depth / 2))((_ zip _)).map(o => o.map(_.toString))).memo

  /** lists of elements with a type (K, V) */
  def genList2(depth: Int = 1): Gen[DList[(String, String)]] =
    if (depth <= 1) genList1(1).map(l => l.map(_.partition(c => c > 'a')))
    else            Gen.oneOf(genList1(depth - 1).map(l => l.map(_.partition(c => c > 'a'))),
                              genList2(depth - 1).map(l => l.map(identity)),
                              genList3(depth - 1).map(l => l.combine(string)),
                              ^(genList2(depth / 2), genList2(depth / 2))((_ ++ _)),
                              ^(genObject(depth / 2), genList1(depth / 2))((_ join _))).memo

  /** lists of elements with a type (K, Iterable[V]) */
  def genList3(depth: Int = 1): Gen[DList[(String, Iterable[String])]] =
    if (depth <= 1) genList2(1).map(l => l.map { case (k, v) => (k, Vector.fill(2)(v)) })
    else            Gen.oneOf(genList2(depth - 1).map(l => l.groupByKey),
                              genList3(depth - 1).map(l => l.map(identity))).memo

}

object CompNodeData { outer =>
  /**
   * rewrite the MscrReducer iterables as vectors to facilitate the comparison
   * The elements inside the iterables are also sorted in alphanumeric order because they could be produced in
   * a different order
   */
  def normalise(result: Any) = rewrite {
    everywherebu(rule {
      case iterable: Iterable[_] => Vector(iterable.iterator.toSeq.sortBy(_.toString):_*)
      case other                 => other
    })
  }(result).toString

  implicit def toNormalised(result: Any): ToNormalised = new ToNormalised(result)
  class ToNormalised(result: Any) {
    def normalise = outer.normalise(result)
  }

}
/**
 * Creation functions
 */
trait CompNodeFactory extends Scope {

  def source                           = ConstantStringDataSource("start")
  def loadWith(s: String)              = Load(ConstantStringDataSource(s), wireFormat[String])
  def load                             = loadWith("start")
  def aRoot(nodes: CompNode*)          = Root(nodes)
  def rt                               = Return("", wireFormat[String])
  def cb(in: CompNode)                 = Combine(in, (s1: Any, s2: Any) => s1.toString + s2.toString, wireFormat[String], wireFormat[String])
  def gbk(in: CompNode): GroupByKey    = GroupByKey(in, wireFormat[String], grouping[String], wireFormat[String])
  def gbk(sink: Sink): GroupByKey      = gbk(load).addSink(sink).asInstanceOf[GroupByKey]
  def mt(in: ProcessNode)              = Materialise(in, wireFormat[Iterable[String]])

  def op(in1: CompNode, in2: CompNode) = Op(in1, in2, (a, b) => a, wireFormat[String])
  def parallelDo(in: CompNode)         = pd(in)

  def pd(ins: CompNode*): ParallelDo =
    ParallelDo(ins, rt, EmitterDoFunction, wireFormat[String], wireFormat[String], Seq(), java.util.UUID.randomUUID().toString)

  def pdWithEnv(in: CompNode, env: ValueNode): ParallelDo =
    ParallelDo(Seq(in), env, EmitterDoFunction, wireFormat[String], wireFormat[String], Seq(), java.util.UUID.randomUUID.toString)

}
