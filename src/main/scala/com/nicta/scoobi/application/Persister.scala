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
package application

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.compress.{GzipCodec, CompressionCodec}
import org.apache.hadoop.io.SequenceFile.CompressionType
import scalaz.{Scalaz, State}
import Scalaz._

import core._
import io.DataSource
import io.DataSink
import impl.plan._
import impl.exec._
import Smart._
import Mode._


object Eval {
  type HadoopST = (ExecState, Map[Smart.DComp[_, _ <: Shape], AST.Node[_, _ <: Shape]])
  type VectorST = Int // This is kind of arbitrary

  sealed abstract class ST
  case class Hadoop(st: HadoopST) extends ST
  case class Vector(st: VectorST) extends ST
}


/** Type class for things that can be persisted. Mechanism to allow tuples of DLists and
  * DObjects to be persisted. */
trait Persister[In] {
  type Out
  def apply(in: In, conf: ScoobiConfiguration): Out

}

/** Persister type class instances for tuples. */
object Persister extends LowImplicitsPersister {
  lazy val logger = LogFactory.getLog("scoobi.Persister")

  /** Evaluate and persist a distributed computation. This can be a combination of one or more
   * DLists (i.e. DListPersisters) and DObjects. */
  def persist[P](p: P)(implicit conf: ScoobiConfiguration, persister: Persister[P]): persister.Out = persister(p, conf)

  implicit def sequencePersister[T](implicit pfn: PFn[T]): Persister[Seq[T]] = new Persister[Seq[T]] {

    type Out = Seq[pfn.Ret]

    def apply(seq: Seq[T], conf: ScoobiConfiguration): Seq[pfn.Ret] = {
      val st = prepareST(seq.map(pfn.plan).toList, conf)
      seq.toStream.traverseS(x => pfn.execute(x, conf)).eval(st).toSeq
    }
  }

  implicit def tuple1persister[T1](implicit pfn1: PFn[T1]) = new Persister[T1] {

    type Out = pfn1.Ret

    def apply(x: T1, conf: ScoobiConfiguration): pfn1.Ret = {
      val st = prepareST(List(pfn1.plan(x)), conf)
      pfn1.execute(x, conf).eval(st)
    }
  }

  implicit def tuple2persister[T1, T2]
    (implicit pfn1: PFn[T1],
              pfn2: PFn[T2]) = new Persister[(T1, T2)] {

    type Out = (pfn1.Ret, pfn2.Ret)

    def apply(x: (T1, T2), conf: ScoobiConfiguration): (pfn1.Ret, pfn2.Ret) = {
      val st = prepareST(List(pfn1.plan(x._1), pfn2.plan(x._2)), conf)
      val execute = for {
        r1 <- pfn1.execute(x._1, conf)
        r2 <- pfn2.execute(x._2, conf)
      } yield (r1, r2)
      execute.eval(st)
    }
  }

  implicit def tuple3persister[T1, T2, T3]
    (implicit pfn1: PFn[T1],
              pfn2: PFn[T2],
              pfn3: PFn[T3]) = new Persister[(T1, T2, T3)] {

    type Out = (pfn1.Ret, pfn2.Ret, pfn3.Ret)

    def apply(x: (T1, T2, T3), conf: ScoobiConfiguration): (pfn1.Ret, pfn2.Ret, pfn3.Ret) = {
      val st = prepareST(List(pfn1.plan(x._1), pfn2.plan(x._2), pfn3.plan(x._3)), conf)
      val execute = for {
        r1 <- pfn1.execute(x._1, conf)
        r2 <- pfn2.execute(x._2, conf)
        r3 <- pfn3.execute(x._3, conf)
      } yield (r1, r2, r3)
      execute.eval(st)
    }
  }

  implicit def tuple4persister[T1, T2, T3, T4]
    (implicit pfn1: PFn[T1],
              pfn2: PFn[T2],
              pfn3: PFn[T3],
              pfn4: PFn[T4]) = new Persister[(T1, T2, T3, T4)] {

    type Out = (pfn1.Ret, pfn2.Ret, pfn3.Ret, pfn4.Ret)

    def apply(x: (T1, T2, T3, T4), conf: ScoobiConfiguration): (pfn1.Ret, pfn2.Ret, pfn3.Ret, pfn4.Ret) = {
      val st = prepareST(List(pfn1.plan(x._1), pfn2.plan(x._2), pfn3.plan(x._3), pfn4.plan(x._4)), conf)
      val execute = for {
        r1 <- pfn1.execute(x._1, conf)
        r2 <- pfn2.execute(x._2, conf)
        r3 <- pfn3.execute(x._3, conf)
        r4 <- pfn4.execute(x._4, conf)
      } yield (r1, r2, r3, r4)
      execute.eval(st)
    }
  }

  implicit def tuple5persister[T1, T2, T3, T4, T5]
    (implicit pfn1: PFn[T1],
              pfn2: PFn[T2],
              pfn3: PFn[T3],
              pfn4: PFn[T4],
              pfn5: PFn[T5]) = new Persister[(T1, T2, T3, T4, T5)] {

    type Out = (pfn1.Ret, pfn2.Ret, pfn3.Ret, pfn4.Ret, pfn5.Ret)

    def apply(x: (T1, T2, T3, T4, T5), conf: ScoobiConfiguration): (pfn1.Ret, pfn2.Ret, pfn3.Ret, pfn4.Ret, pfn5.Ret) = {
      val st = prepareST(List(pfn1.plan(x._1), pfn2.plan(x._2), pfn3.plan(x._3), pfn4.plan(x._4), pfn5.plan(x._5)), conf)
      val execute = for {
        r1 <- pfn1.execute(x._1, conf)
        r2 <- pfn2.execute(x._2, conf)
        r3 <- pfn3.execute(x._3, conf)
        r4 <- pfn4.execute(x._4, conf)
        r5 <- pfn5.execute(x._5, conf)
      } yield (r1, r2, r3, r4, r5)
      execute.eval(st)
    }
  }

  implicit def tuple6persister[T1, T2, T3, T4, T5, T6]
    (implicit pfn1: PFn[T1],
              pfn2: PFn[T2],
              pfn3: PFn[T3],
              pfn4: PFn[T4],
              pfn5: PFn[T5],
              pfn6: PFn[T6]) = new Persister[(T1, T2, T3, T4, T5, T6)] {

    type Out = (pfn1.Ret, pfn2.Ret, pfn3.Ret, pfn4.Ret, pfn5.Ret, pfn6.Ret)

    def apply(x: (T1, T2, T3, T4, T5, T6), conf: ScoobiConfiguration): (pfn1.Ret, pfn2.Ret, pfn3.Ret, pfn4.Ret, pfn5.Ret, pfn6.Ret) = {
      val st = prepareST(List(pfn1.plan(x._1), pfn2.plan(x._2), pfn3.plan(x._3), pfn4.plan(x._4), pfn5.plan(x._5), pfn6.plan(x._6)), conf)
      val execute = for {
        r1 <- pfn1.execute(x._1, conf)
        r2 <- pfn2.execute(x._2, conf)
        r3 <- pfn3.execute(x._3, conf)
        r4 <- pfn4.execute(x._4, conf)
        r5 <- pfn5.execute(x._5, conf)
        r6 <- pfn6.execute(x._6, conf)
      } yield (r1, r2, r3, r4, r5, r6)
      execute.eval(st)
    }
  }

  implicit def tuple7persister[T1, T2, T3, T4, T5, T6, T7]
    (implicit pfn1: PFn[T1],
              pfn2: PFn[T2],
              pfn3: PFn[T3],
              pfn4: PFn[T4],
              pfn5: PFn[T5],
              pfn6: PFn[T6],
              pfn7: PFn[T7]) = new Persister[(T1, T2, T3, T4, T5, T6, T7)] {

    type Out = (pfn1.Ret, pfn2.Ret, pfn3.Ret, pfn4.Ret, pfn5.Ret, pfn6.Ret, pfn7.Ret)

    def apply(x: (T1, T2, T3, T4, T5, T6, T7), conf: ScoobiConfiguration): (pfn1.Ret, pfn2.Ret, pfn3.Ret, pfn4.Ret, pfn5.Ret, pfn6.Ret, pfn7.Ret) = {
      val st = prepareST(List(pfn1.plan(x._1), pfn2.plan(x._2), pfn3.plan(x._3), pfn4.plan(x._4), pfn5.plan(x._5), pfn6.plan(x._6), pfn7.plan(x._7)), conf)
      val execute = for {
        r1 <- pfn1.execute(x._1, conf)
        r2 <- pfn2.execute(x._2, conf)
        r3 <- pfn3.execute(x._3, conf)
        r4 <- pfn4.execute(x._4, conf)
        r5 <- pfn5.execute(x._5, conf)
        r6 <- pfn6.execute(x._6, conf)
        r7 <- pfn7.execute(x._7, conf)
      } yield (r1, r2, r3, r4, r5, r6, r7)
      execute.eval(st)
    }
  }

  implicit def tuple8persister[T1, T2, T3, T4, T5, T6, T7, T8]
    (implicit pfn1: PFn[T1],
              pfn2: PFn[T2],
              pfn3: PFn[T3],
              pfn4: PFn[T4],
              pfn5: PFn[T5],
              pfn6: PFn[T6],
              pfn7: PFn[T7],
              pfn8: PFn[T8]) = new Persister[(T1, T2, T3, T4, T5, T6, T7, T8)] {

    type Out = (pfn1.Ret, pfn2.Ret, pfn3.Ret, pfn4.Ret, pfn5.Ret, pfn6.Ret, pfn7.Ret, pfn8.Ret)

    def apply(x: (T1, T2, T3, T4, T5, T6, T7, T8), conf: ScoobiConfiguration): (pfn1.Ret, pfn2.Ret, pfn3.Ret, pfn4.Ret, pfn5.Ret, pfn6.Ret, pfn7.Ret, pfn8.Ret) = {
      val st = prepareST(List(pfn1.plan(x._1), pfn2.plan(x._2), pfn3.plan(x._3), pfn4.plan(x._4), pfn5.plan(x._5), pfn6.plan(x._6), pfn7.plan(x._7), pfn8.plan(x._8)), conf)
      val execute = for {
        r1 <- pfn1.execute(x._1, conf)
        r2 <- pfn2.execute(x._2, conf)
        r3 <- pfn3.execute(x._3, conf)
        r4 <- pfn4.execute(x._4, conf)
        r5 <- pfn5.execute(x._5, conf)
        r6 <- pfn6.execute(x._6, conf)
        r7 <- pfn7.execute(x._7, conf)
        r8 <- pfn8.execute(x._8, conf)
      } yield (r1, r2, r3, r4, r5, r6, r7, r8)
      execute.eval(st)
    }
  }


  private def sources(outputs: List[Smart.DComp[_, _ <: Shape]]): List[DataSource[_,_,_]] = {
    import Smart._
    def addLoads(set: Set[Load[_]], comp: DComp[_, _ <: Shape]): Set[Load[_]] = comp match {
      case ld@Load(_)                   => set + ld
      case ParallelDo(in, env, _, _, _) => { val s1 = addLoads(set, in); addLoads(s1, env) }
      case GroupByKey(in)               => addLoads(set, in)
      case Combine(in, _)               => addLoads(set, in)
      case Flatten(ins)                 => ins.foldLeft(set) { case (s, in) => addLoads(s, in) }
      case Materialise(in)              => addLoads(set, in)
      case Op(in1, in2, f)              => { val s1 = addLoads(set, in1); addLoads(s1, in2) }
      case Return(_)                    => set
    }

    val loads = outputs.foldLeft(Set.empty: Set[Load[_]]) { case (set, output) => addLoads(set, output) }
    loads.toList.map(_.source)
  }

  private def prepareST(outputs: List[(Smart.DComp[_, _ <: Shape], Option[DataSink[_,_,_]])], conf: ScoobiConfiguration): Eval.ST = {
    /* Check inputs + outputs. */
    sources(outputs.map(_._1)) foreach { _.inputCheck(conf) }
    outputs collect { case (_, Some(sink)) => sink } foreach { _.outputCheck(conf) }

    /* Performing any up-front planning before execution. */
    if (conf.isInMemory) VectorMode.prepareST(outputs, conf) else HadoopMode.prepareST(outputs, conf)
  }
}


/**
 * Those additional implicits allow to pass tuples of persisters to persist at once with the persist method
 * @see PersisterSpec
 */
trait LowImplicitsPersister {

  implicit def tuple2persisters[T1, T2]  (implicit p1: Persister[T1], p2: Persister[T2]) =
    new Persister[(T1, T2)] {
      type Out = (p1.Out, p2.Out)
      def apply(x: (T1, T2), conf: ScoobiConfiguration) =
        (p1.apply(x._1, conf), p2.apply(x._2, conf))
    }

  implicit def tuple3persisters[T1, T2, T3]  (implicit p1: Persister[T1], p2: Persister[T2], p3: Persister[T3]) =
    new Persister[(T1, T2, T3)] {
      type Out = (p1.Out, p2.Out, p3.Out)
      def apply(x: (T1, T2, T3), conf: ScoobiConfiguration) =
        (p1.apply(x._1, conf), p2.apply(x._2, conf), p3.apply(x._3, conf))
    }

  implicit def tuple4persisters[T1, T2, T3, T4]  (implicit p1: Persister[T1], p2: Persister[T2], p3: Persister[T3], p4: Persister[T4]) =
    new Persister[(T1, T2, T3, T4)] {
      type Out = (p1.Out, p2.Out, p3.Out, p4.Out)
      def apply(x: (T1, T2, T3, T4), conf: ScoobiConfiguration) =
        (p1.apply(x._1, conf), p2.apply(x._2, conf), p3.apply(x._3, conf), p4.apply(x._4, conf))
    }

  implicit def tuple5persisters[T1, T2, T3, T4, T5]  (implicit p1: Persister[T1], p2: Persister[T2], p3: Persister[T3], p4: Persister[T4], p5: Persister[T5]) =
    new Persister[(T1, T2, T3, T4, T5)] {
      type Out = (p1.Out, p2.Out, p3.Out, p4.Out, p5.Out)
      def apply(x: (T1, T2, T3, T4, T5), conf: ScoobiConfiguration) =
        (p1.apply(x._1, conf), p2.apply(x._2, conf), p3.apply(x._3, conf), p4.apply(x._4, conf), p5.apply(x._5, conf))
    }

  implicit def tuple6persisters[T1, T2, T3, T4, T5, T6]  (implicit p1: Persister[T1], p2: Persister[T2], p3: Persister[T3], p4: Persister[T4], p5: Persister[T5], p6: Persister[T6]) =
    new Persister[(T1, T2, T3, T4, T5, T6)] {
      type Out = (p1.Out, p2.Out, p3.Out, p4.Out, p5.Out, p6.Out)
      def apply(x: (T1, T2, T3, T4, T5, T6), conf: ScoobiConfiguration) =
        (p1.apply(x._1, conf), p2.apply(x._2, conf), p3.apply(x._3, conf), p4.apply(x._4, conf), p5.apply(x._5, conf), p6.apply(x._6, conf))
    }

  implicit def tuple7persisters[T1, T2, T3, T4, T5, T6, T7]  (implicit p1: Persister[T1], p2: Persister[T2], p3: Persister[T3], p4: Persister[T4], p5: Persister[T5], p6: Persister[T6], p7: Persister[T7]) =
    new Persister[(T1, T2, T3, T4, T5, T6, T7)] {
      type Out = (p1.Out, p2.Out, p3.Out, p4.Out, p5.Out, p6.Out, p7.Out)
      def apply(x: (T1, T2, T3, T4, T5, T6, T7), conf: ScoobiConfiguration) =
        (p1.apply(x._1, conf), p2.apply(x._2, conf), p3.apply(x._3, conf), p4.apply(x._4, conf), p5.apply(x._5, conf), p6.apply(x._6, conf), p7.apply(x._7, conf))
    }

  implicit def tuple8persisters[T1, T2, T3, T4, T5, T6, T7, T8]  (implicit p1: Persister[T1], p2: Persister[T2], p3: Persister[T3], p4: Persister[T4], p5: Persister[T5], p6: Persister[T6], p7: Persister[T7], p8: Persister[T8]) =
    new Persister[(T1, T2, T3, T4, T5, T6, T7, T8)] {
      type Out = (p1.Out, p2.Out, p3.Out, p4.Out, p5.Out, p6.Out, p7.Out, p8.Out)
      def apply(x: (T1, T2, T3, T4, T5, T6, T7, T8), conf: ScoobiConfiguration) =
        (p1.apply(x._1, conf), p2.apply(x._2, conf), p3.apply(x._3, conf), p4.apply(x._4, conf), p5.apply(x._5, conf), p6.apply(x._6, conf), p7.apply(x._7, conf), p8.apply(x._8, conf))
    }


}

/** The container for persisting a DList. */
case class DListPersister[A](dlist: DList[A], sink: DataSink[_, _, A]) {
  def compress = compressWith(new GzipCodec)
  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = copy(sink = sink.outputCompression(codec))
}


sealed trait PFn[A] {
  type Ret
  def plan(x: A): (Smart.DComp[_, _ <: Shape], Option[DataSink[_,_,_]])
  def execute(x: A, c: ScoobiConfiguration): State[Eval.ST, Ret]
}


/** PFn type class instances for DLists (i.e. DListPersister) and DObjects. */
object PFn {

  implicit def DListPersister[A] = new PFn[DListPersister[A]] {
    type Ret = Unit
    def plan(x: DListPersister[A]) = (x.dlist.getComp, Some(x.sink))
    def execute(x: DListPersister[A], sc: ScoobiConfiguration): State[Eval.ST, Unit] = sc.mode match {
      case InMemory        => VectorMode.executeDListPersister(x, sc)
      case Local | Cluster => HadoopMode.executeDListPersister(x, sc)
    }
  }

  implicit def DObjectPersister[A] = new PFn[DObject[A]] {
    type Ret = A
    def plan(x: DObject[A]) = (x.getComp, None)
    def execute(x: DObject[A], sc: ScoobiConfiguration): State[Eval.ST, A] = sc.mode match {
      case InMemory        => VectorMode.executeDObject(x, sc)
      case Local | Cluster => HadoopMode.executeDObject(x, sc)
    }
  }
}
