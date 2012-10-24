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
import comp.CompNode
import impl.exec._
import org.apache.hadoop.io.compress.{GzipCodec, CompressionCodec}
import org.apache.hadoop.io.SequenceFile.CompressionType
import scalaz.Scalaz
import Scalaz._
import Mode._

object Persister {
  lazy val logger = LogFactory.getLog("scoobi.Persister")

  def persist[A](list: DList[A])(implicit sc: ScoobiConfiguration) {
    sc.mode match {
      case InMemory        => InMemoryMode.execute(list)
//      case Local | Cluster => HadoopMode.prepareST(list)
    }
  }

  def persist[A](o: DObject[A])(implicit sc: ScoobiConfiguration): A = {
    sc.mode match {
      case InMemory        => InMemoryMode.execute(o).asInstanceOf[A]
//      case Local | Cluster => HadoopMode.prepareST(o)
    }
  }

  private
  def createPlan(outputs: List[(CompNode, Option[DataSink[_,_,_]])])(implicit sc: ScoobiConfiguration): (ExecState, Map[CompNode, ExecutionNode]) = {
/*
++++++++ HEAD
    /* Produce map of all unique outputs and their corresponding persisters. */
    val rawOutMap: List[(CompNode, Set[DataSink[_,_,_]])] =
      outputs.groupBy(_._1).
              map(t => (t._1, t._2.map(_._2).flatten.toSet)).
              toList

    logger.debug("Raw graph")
    rawOutMap foreach { case (c, s) => logger.debug("(" + c.toVerboseString + ", " + s + ")") }


    /* Optimise the plan associated with the outputs. */
    val optOuts = {
      val opt: List[Smart.DComp[_, _ <: Shape]] = Smart.optimisePlan(rawOutMap.map(_._1))
      (rawOutMap zip opt) map { case ((o1, s), o2) => (o2, (o1, s)) }
=======
  private def sources(outputs: List[Smart.DComp[_, _ <: Shape]]): List[DataSource[_,_,_]] = {
    import Smart._
    def addLoads(set: Set[Load[_]], comp: DComp[_, _ <: Shape]): Set[Load[_]] = comp match {
      case ld@Load(_)                   => set + ld
      case ParallelDo(in, env, _, _, _) => { val s1 = addLoads(set, in); addLoads(s1, env) }
      case GroupByKey(in)               => addLoads(set, in)
      case Combine(in, _)               => addLoads(set, in)
      case Flatten(ins)                 => ins.foldLeft(set) { case (s, in) => addLoads(s, in) }
      case Materialize(in)              => addLoads(set, in)
      case Op(in1, in2, f)              => { val s1 = addLoads(set, in1); addLoads(s1, in2) }
      case Return(_)                    => set
-------- origin
    }

    val loads = outputs.foldLeft(Set.empty: Set[Load[_]]) { case (set, output) => addLoads(set, output) }
    loads.toList.map(_.source)
  }

  private def prepareST(outputs: List[(Smart.DComp[_, _ <: Shape], Option[DataSink[_,_,_]])])(implicit sc: ScoobiConfiguration): Eval.ST = {
    /* Check inputs + outputs. */
    sources(outputs.map(_._1)) foreach { _.inputCheck(conf) }
    outputs collect { case (_, Some(sink)) => sink } foreach { _.outputCheck(conf) }

++++++++ HEAD
    (st, nodeMap)
    (ExecState(conf), Map())
=======
    /* Performing any up-front planning before execution. */
    if (conf.isInMemory) VectorMode.prepareST(outputs) else HadoopMode.prepareST(outputs)
------- origin
    */
    (ExecState(sc), Map())
  }
}


/** The container for persisting a DList. */
case class DListPersister[A](dlist: DList[A], sink: DataSink[_, _, A]) {
  def compress = compressWith(new GzipCodec)
  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = copy(sink = sink.outputCompression(codec))
}
