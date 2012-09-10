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
import scala.collection.immutable.DefaultMap

import core._
import io.DataSink
import impl.plan._
import impl.exec._
import Smart._
import org.apache.hadoop.io.compress.{GzipCodec, CompressionCodec}
import org.apache.hadoop.io.SequenceFile.CompressionType

/** Type class for things that can be persisted. Mechanism to allow tuples of DLists and
  * DObjects to be persisted. */
trait Persister[In] {
  type Out
  def apply(in: In, conf: ScoobiConfiguration): Out

}

/** Persister type class instances for tuples. */
object Persister {
  lazy val logger = LogFactory.getLog("scoobi.Persister")

  /** Evaluate and persist a distributed computation. This can be a combination of one or more
   * DLists (i.e. DListPersisters) and DObjects. */
  def persist[P](p: P)(implicit conf: ScoobiConfiguration, persister: Persister[P]): persister.Out = persister(p, conf)

  implicit def tuple1persister[T1](implicit pfn1: PFn[T1]) = new Persister[T1] {

    type Out = pfn1.Ret

    def apply(x: T1, conf: ScoobiConfiguration): pfn1.Ret = {
      val (st, nodeMap) = createPlan(List(pfn1.plan(x)), conf)
      pfn1.execute(x).eval((st, nodeMap))

    }
  }

  implicit def tuple2persister[T1, T2]
    (implicit pfn1: PFn[T1],
              pfn2: PFn[T2]) = new Persister[(T1, T2)] {

    type Out = (pfn1.Ret, pfn2.Ret)

    def apply(x: (T1, T2), conf: ScoobiConfiguration): (pfn1.Ret, pfn2.Ret) = {
      val (st, nodeMap) = createPlan(List(pfn1.plan(x._1), pfn2.plan(x._2)), conf)
      val execute = for {
        r1 <- pfn1.execute(x._1)
        r2 <- pfn2.execute(x._2)
      } yield (r1, r2)
      execute.eval((st, nodeMap))
    }
  }

  implicit def tuple3persister[T1, T2, T3]
    (implicit pfn1: PFn[T1],
              pfn2: PFn[T2],
              pfn3: PFn[T3]) = new Persister[(T1, T2, T3)] {

    type Out = (pfn1.Ret, pfn2.Ret, pfn3.Ret)

    def apply(x: (T1, T2, T3), conf: ScoobiConfiguration): (pfn1.Ret, pfn2.Ret, pfn3.Ret) = {
      val (st, nodeMap) = createPlan(List(pfn1.plan(x._1), pfn2.plan(x._2), pfn3.plan(x._3)), conf)
      val execute = for {
        r1 <- pfn1.execute(x._1)
        r2 <- pfn2.execute(x._2)
        r3 <- pfn3.execute(x._3)
      } yield (r1, r2, r3)
      execute.eval((st, nodeMap))
    }
  }

  implicit def tuple4persister[T1, T2, T3, T4]
    (implicit pfn1: PFn[T1],
              pfn2: PFn[T2],
              pfn3: PFn[T3],
              pfn4: PFn[T4]) = new Persister[(T1, T2, T3, T4)] {

    type Out = (pfn1.Ret, pfn2.Ret, pfn3.Ret, pfn4.Ret)

    def apply(x: (T1, T2, T3, T4), conf: ScoobiConfiguration): (pfn1.Ret, pfn2.Ret, pfn3.Ret, pfn4.Ret) = {
      val (st, nodeMap) = createPlan(List(pfn1.plan(x._1), pfn2.plan(x._2), pfn3.plan(x._3), pfn4.plan(x._4)), conf)
      val execute = for {
        r1 <- pfn1.execute(x._1)
        r2 <- pfn2.execute(x._2)
        r3 <- pfn3.execute(x._3)
        r4 <- pfn4.execute(x._4)
      } yield (r1, r2, r3, r4)
      execute.eval((st, nodeMap))
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
      val (st, nodeMap) = createPlan(List(pfn1.plan(x._1), pfn2.plan(x._2), pfn3.plan(x._3), pfn4.plan(x._4), pfn5.plan(x._5)), conf)
      val execute = for {
        r1 <- pfn1.execute(x._1)
        r2 <- pfn2.execute(x._2)
        r3 <- pfn3.execute(x._3)
        r4 <- pfn4.execute(x._4)
        r5 <- pfn5.execute(x._5)
      } yield (r1, r2, r3, r4, r5)
      execute.eval((st, nodeMap))
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
      val (st, nodeMap) = createPlan(List(pfn1.plan(x._1), pfn2.plan(x._2), pfn3.plan(x._3), pfn4.plan(x._4), pfn5.plan(x._5), pfn6.plan(x._6)), conf)
      val execute = for {
        r1 <- pfn1.execute(x._1)
        r2 <- pfn2.execute(x._2)
        r3 <- pfn3.execute(x._3)
        r4 <- pfn4.execute(x._4)
        r5 <- pfn5.execute(x._5)
        r6 <- pfn6.execute(x._6)
      } yield (r1, r2, r3, r4, r5, r6)
      execute.eval((st, nodeMap))
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
      val (st, nodeMap) = createPlan(List(pfn1.plan(x._1), pfn2.plan(x._2), pfn3.plan(x._3), pfn4.plan(x._4), pfn5.plan(x._5), pfn6.plan(x._6), pfn7.plan(x._7)), conf)
      val execute = for {
        r1 <- pfn1.execute(x._1)
        r2 <- pfn2.execute(x._2)
        r3 <- pfn3.execute(x._3)
        r4 <- pfn4.execute(x._4)
        r5 <- pfn5.execute(x._5)
        r6 <- pfn6.execute(x._6)
        r7 <- pfn7.execute(x._7)
      } yield (r1, r2, r3, r4, r5, r6, r7)
      execute.eval((st, nodeMap))
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
      val (st, nodeMap) = createPlan(List(pfn1.plan(x._1), pfn2.plan(x._2), pfn3.plan(x._3), pfn4.plan(x._4), pfn5.plan(x._5), pfn6.plan(x._6), pfn7.plan(x._7), pfn8.plan(x._8)), conf)
      val execute = for {
        r1 <- pfn1.execute(x._1)
        r2 <- pfn2.execute(x._2)
        r3 <- pfn3.execute(x._3)
        r4 <- pfn4.execute(x._4)
        r5 <- pfn5.execute(x._5)
        r6 <- pfn6.execute(x._6)
        r7 <- pfn7.execute(x._7)
        r8 <- pfn8.execute(x._8)
      } yield (r1, r2, r3, r4, r5, r6, r7, r8)
      execute.eval((st, nodeMap))
    }
  }


  private def createPlan(outputs: List[(Smart.DComp[_, _ <: Shape], Option[DataSink[_,_,_]])], conf: ScoobiConfiguration)
    : (ExecState, Map[Smart.DComp[_, _ <: Shape], AST.Node[_, _ <: Shape]]) = {

    /* Produce map of all unique outputs and their corresponding persisters. */
    val rawOutMap: List[(Smart.DComp[_, _ <: Shape], Set[DataSink[_,_,_]])] =
      outputs.groupBy(_._1)
             .map(t => (t._1, t._2.map(_._2).flatten.toSet))
             .toList

    logger.debug("Raw graph")
    rawOutMap foreach { case (c, s) => logger.debug("(" + c.toVerboseString + ", " + s + ")") }


    /* Optimise the plan associated with the outputs. */
    val optOuts = {
      val opt: List[Smart.DComp[_, _ <: Shape]] = Smart.optimisePlan(rawOutMap.map(_._1))
      (rawOutMap zip opt) map { case ((o1, s), o2) => (o2, (o1, s)) }
    }

    logger.debug("Optimised graph")
    optOuts foreach { case (c, s) => logger.debug("(" + c.toVerboseString + ", " + s + ")") }

    /*
     *  Convert the Smart.DComp abstract syntax tree to AST.Node abstract syntax tree.
     *  This is a side-effecting expression. The @m@ field of the @ci@ parameter is updated.
     */
    import com.nicta.scoobi.impl.plan.{Intermediate => I}
    val outMap = optOuts.map { case (o2, (o1, s)) => (o2, s) }.toMap[Smart.DComp[_, _ <: Shape], Set[DataSink[_,_,_]]]
    val ds = outMap.keys
    val iMSCRGraph: I.MSCRGraph = I.MSCRGraph(ds)
    val ci = ConvertInfo(conf, outMap , iMSCRGraph.mscrs, iMSCRGraph.g)

    logger.debug("Intermediate MSCRs")
    iMSCRGraph.mscrs foreach { mscr => logger.debug(mscr.toString) }

    ds.foreach(_.convert(ci))     // Step 3 (see top of Intermediate.scala)
    val mscrGraph = MSCRGraph(ci) // Step 4 (See top of Intermediate.scala)

    logger.debug("Converted graph")
    mscrGraph.outputs.map(_.node.toVerboseString) foreach { out => logger.debug(out) }

    logger.debug("MSCRs")
    mscrGraph.mscrs foreach { mscr => logger.debug(mscr.toString) }

    logger.debug("Environments")
    mscrGraph.environments foreach { env => logger.debug(env.toString) }

    /* Do execution preparation - setup state, etc */
    val st = Executor.prepare(mscrGraph, conf)

    /* Generate a map from the original (non-optimised) Smart.DComp to AST.Node */
    val nodeMap = new DefaultMap[Smart.DComp[_, _ <: Shape], AST.Node[_, _ <: Shape]] {
      def get(d: Smart.DComp[_, _ <: Shape]): Option[AST.Node[_, _ <: Shape]] = {
        for {
          rawOutput <- rawOutMap.find(_._1 == d)
          dataSinks <- Some(rawOutput._2)
          optOutput <- optOuts.find(_._2 == rawOutput)
          optNode   <- Some(optOutput._1)
        } yield (ci.astMap(optNode))
      }

      val iterator = {
        val it = rawOutMap.toIterator
        new Iterator[(Smart.DComp[_, _ <: Shape], AST.Node[_, _ <: Shape])] {
          def hasNext: Boolean = it.hasNext
          def next(): (Smart.DComp[_, _ <: Shape], AST.Node[_, _ <: Shape]) = {
            val n = for {
              dcomp <- Some(it.next()._1)
              ast   <- get(dcomp)
            } yield (dcomp, ast)
            n.orNull
          }
        }
      }
    }

    (st, nodeMap)
  }
}


/** The container for persisting a DList. */
case class DListPersister[A](dlist: DList[A], sink: DataSink[_, _, A]) {
  def compress = compressWith(new GzipCodec)
  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = copy(sink = sink.outputCompression(codec))
}

import scalaz.State

/** Type class for persisting something. */
sealed trait PFn[A] {
  type Ret
  def plan(x: A): (Smart.DComp[_, _ <: Shape], Option[DataSink[_,_,_]])
  def execute(x: A): State[(ExecState, Map[Smart.DComp[_, _ <: Shape], AST.Node[_, _ <: Shape]]), Ret]
}


/** PFn type class instances for DLists (i.e. DListPersister) and DObjects. */
object PFn {

  implicit def DListPersister[A] = new PFn[DListPersister[A]] {
    type Ret = Unit
    def plan(x: DListPersister[A]) = (x.dlist.getComp, Some(x.sink))
    def execute(x: DListPersister[A]): State[(ExecState, Map[Smart.DComp[_, _ <: Shape], AST.Node[_, _ <: Shape]]), Unit] =
      State({ case(st, nodeMap) =>
        val node: AST.Node[A, _ <: Shape] = nodeMap(x.dlist.getComp).asInstanceOf[AST.Node[A, _ <: Shape]]
        ((), (Executor.executeArrOutput(node, x.sink, st), nodeMap))
      })
  }

  implicit def DObjectPersister[A] = new PFn[DObject[A]] {
    type Ret = A
    def plan(x: DObject[A]) = (x.getComp, None)
    def execute(x: DObject[A]): State[(ExecState, Map[Smart.DComp[_, _ <: Shape], AST.Node[_, _ <: Shape]]), A] =
      State({ case(st, nodeMap) =>
        val node: AST.Node[A, _ <: Shape] = nodeMap(x.getComp).asInstanceOf[AST.Node[A, _ <: Shape]]
        val (e, stU) = Executor.executeExp(node, st)
        (e, (stU, nodeMap))
      })
  }
}
