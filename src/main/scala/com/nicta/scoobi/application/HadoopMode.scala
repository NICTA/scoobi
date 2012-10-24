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
import scalaz.State

import core._
import impl.plan._
import comp.CompNode
import impl.exec._
import io.DataSource
import io.DataSink


/**
  * Execution of Scoobi applications using Hadoop.
  */
object HadoopMode {
  lazy val logger = LogFactory.getLog("scoobi.HadoopMode")


  def prepareST(outputs: List[(CompNode, Option[DataSink[_,_,_]])])(implicit sc: ScoobiConfiguration) = {

    /* Produce map of all unique outputs and their corresponding persisters. */
    /*
    val rawOutMap: List[(CompNode, Set[DataSink[_,_,_]])] =
      outputs.groupBy(_._1)
             .map(t => (t._1, t._2.map(_._2).flatten.toSet))
             .toList

    logger.debug("Raw graph")
    rawOutMap foreach { case (c, s) => logger.debug("(" + c.toVerboseString + ", " + s + ")") }


    /* Optimise the plan associated with the outputs. */
    val optOuts = {
      val opt: List[CompNode] = Smart.optimisePlan(rawOutMap.map(_._1))
      (rawOutMap zip opt) map { case ((o1, s), o2) => (o2, (o1, s)) }
    }

    logger.debug("Optimised graph")
    optOuts foreach { case (c, s) => logger.debug("(" + c.toVerboseString + ", " + s + ")") }

    /*
     *  Convert the Smart.DComp abstract syntax tree to AST.Node abstract syntax tree.
     *  This is a side-effecting expression. The @m@ field of the @ci@ parameter is updated.
     */
    import com.nicta.scoobi.impl.plan.{Intermediate => I}
    val outMap = optOuts.map { case (o2, (o1, s)) => (o2, s) }.toMap[CompNode, Set[DataSink[_,_,_]]]
    val ds = outMap.keys
    val iMSCRGraph  = I.MSCRGraph(ds)
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
    val nodeMap = new DefaultMap[CompNode, ExecutionNode] {
      def get(d: CompNode): Option[ExecutionNode] = {
        for {
          rawOutput <- rawOutMap.find(_._1 == d)
          dataSinks <- Some(rawOutput._2)
          optOutput <- optOuts.find(_._2 == rawOutput)
          optNode   <- Some(optOutput._1)
        } yield (ci.astMap(optNode))
      }

      val iterator = {
        val it = rawOutMap.toIterator
        new Iterator[(CompNode, ExecutionNode)] {
          def hasNext: Boolean = it.hasNext
          def next(): (CompNode, ExecutionNode) = {
            val n = for {
              dcomp <- Some(it.next()._1)
              ast   <- get(dcomp)
            } yield (dcomp, ast)
            n.orNull
          }
        }
      }
    }

    */
    //Eval.Hadoop((ExecState(ScoobiConfiguration()), Map()))
  }

/*
  def executeDListPersister[A](x: DListPersister[A])(implicit sc: ScoobiConfiguration): State[Eval.ST, Unit] = State({
    case Eval.Hadoop((exSt, nodeMap)) => {
//      val node = nodeMap(x.dlist.getComp).asInstanceOf[ExecutionNode]
      // (Eval.Hadoop(Executor.executeArrOutput(node, x.sink, exSt), nodeMap), ())
      sys.error("Not implemented yet")
    }
    case _ => sys.error("something went wrong")
  })


  def executeDObject[A](x: DObject[A])(implicit sc: ScoobiConfiguration): State[Eval.ST, A] = State({
    case Eval.Hadoop((exSt, nodeMap)) => {
//      val node = nodeMap(x.getComp).asInstanceOf[ExecutionNode]
//      val (e, stU) = Executor.executeExp(node, exSt)
//      (Eval.Hadoop((stU, nodeMap)), e)
      sys.error("Not implemented yet")
    }
    case _ => sys.error("something went wrong")
  })
  */
}
