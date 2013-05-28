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
package exec

import org.apache.commons.logging.LogFactory
import scala.collection.immutable.VectorBuilder
import scala.collection.JavaConversions._

import core._
import monitor.Loggable._
import impl.plan._
import comp._
import ScoobiConfiguration._
import scalaz.Scalaz._
import org.apache.hadoop.mapreduce.Job
import Configurations._

/**
 * A fast local mode for execution of Scoobi applications.
 */
case class InMemoryMode() extends ExecutionMode {

  implicit lazy val modeLogger = LogFactory.getLog("scoobi.InMemoryMode")

  def execute(list: DList[_])(implicit sc: ScoobiConfiguration) {
    execute(list.getComp)
  }

  def execute(o: DObject[_])(implicit sc: ScoobiConfiguration): Any = {
    execute(o.getComp)
  }

  def execute(node: CompNode)(implicit sc: ScoobiConfiguration): Any = {
    val toExecute = prepare(node)
    val result = toExecute -> computeValue(sc)
    allSinks(toExecute).debug("sinks: ").foreach(markSinkAsFilled)
    result
  }

  /** optimisation: we only consider sinks which are related to expected results nodes */
  override protected lazy val allSinks: CachedAttribute[CompNode, Seq[Sink]] = attr {
    case n if isExpectedResult(n) => n.sinks ++ children(n).flatMap(allSinks)
    case n                        => children(n).flatMap(allSinks)
  }

  private
  lazy val computeValue: ScoobiConfiguration => CompNode => Any =
    paramAttr { sc: ScoobiConfiguration => node: CompNode =>
      (node -> compute(sc)).head
    }

  private
  lazy val compute: ScoobiConfiguration => CompNode => Seq[_] =
    paramAttr { sc: ScoobiConfiguration => node: CompNode =>
      implicit val c = sc
      val result = node match {
        case n: Load                                               => computeLoad(n)
        case n: Root                                               => Vector(n.ins.map(_ -> computeValue(c)):_*)
        case n: Return                                             => Vector(n.in)
        case n: Op                                                 => Vector(n.execute(n.in1 -> computeValue(c),  n.in2 -> computeValue(c)))
        case n: Materialise                                        => Vector(n.in -> compute(c))
        case n: ProcessNode if n.bridgeStore.exists(hasBeenFilled) => n.bridgeStore.map(b => loadSource(b.toSource, n.wf)).getOrElse(Seq())
        case n: GroupByKey                                         => computeGroupByKey(n)
        case n: Combine                                            => computeCombine(n)
        case n: ParallelDo                                         => computeParallelDo(n)
      }

      if (isExpectedResult(node))
        node match {
          case Materialise1(_) | Op1(_) | GroupByKey1(_) | ParallelDo1(_) | Combine1(_) => saveSinks(result, node)
          case _                                                                        => ()
        }
      result
    }

  protected def sinksToSave(node: CompNode): Seq[Sink] = {
    node match {
      case Materialise1(in) if node.sinks.isEmpty => in.sinks
      case _                                      => node.sinks
    }
  }

  /** @return true if the result must be returned to the client user of this evaluation mode */
  private def isExpectedResult = (node: CompNode) =>
    !parent(node).isDefined || parent(node).map(isRoot).getOrElse(false) || node.hasCheckpoint

  private def computeLoad(load: Load)(implicit sc: ScoobiConfiguration): Seq[Any] =
    loadSource(load.source, load.wf).debug(_ => "computeLoad")

  private def loadSource(source: Source, wf: WireReaderWriter)(implicit sc: ScoobiConfiguration): Seq[Any] =
    Source.read(source).debug(_ => "loadSource")

  private def computeParallelDo(pd: ParallelDo)(implicit sc: ScoobiConfiguration): Seq[_] = {
    val vb = new VectorBuilder[Any]()
    val emitter = new EmitterWriter with NoScoobiJobContext { def write(v: Any) { vb += v } }

    val (dofn, env) = (pd.dofn, (pd.env -> compute(sc)).head)
    dofn.setupFunction(env)
    (pd.ins.flatMap(_ -> compute(sc))).foreach { v => dofn.processFunction(env, v, emitter) }
    dofn.cleanupFunction(env, emitter)
    vb.result.debug(_ => "computeParallelDo")
  }

  private def computeGroupByKey(gbk: GroupByKey)(implicit sc: ScoobiConfiguration): Seq[_] = {
    val in = gbk.in -> compute(sc)
    val gpk = gbk.gpk

    /* Partitioning */
    val partitions: IndexedSeq[Vector[(Any, Any)]] = {
      val numPart = 10    // TODO - set this based on input size? or vary it randomly?
      val vbs = IndexedSeq.fill(numPart)(new VectorBuilder[(Any, Any)]())
      in foreach { case kv @ (k, _) =>
        val p = gpk.partitionKey(k, numPart)
        vbs(p) += kv
      }
      vbs map { _.result() }
    }

    modeLogger.debug("partitions:")
    partitions.zipWithIndex foreach { case (p, ix) => modeLogger.debug(ix + ": " + p) }

    val sorted: IndexedSeq[Vector[(Any, Any)]] = partitions map { (v: Vector[(Any, Any)]) =>
      v.sortBy(_._1)(gpk.toSortOrdering)
    }
    modeLogger.debug("sorted:")
    sorted.zipWithIndex foreach { case (p, ix) => modeLogger.debug(ix + ": " + p) }

    val grouped: IndexedSeq[Vector[(Any, Vector[Any])]] =
      sorted map { kvs =>
        val vbMap = kvs.foldLeft(Vector.empty: Vector[(Any, VectorBuilder[(Any, Any)])]) { case (groups, kv@(k, _)) =>
          groups.lastOption.filter { case (g, _) => gpk.isEqualWithGroup(g, k) } match {
            case Some((_, q)) => {
              q += kv
              groups
            }
            case None =>
              groups :+ {
                val vb = new VectorBuilder[(Any, Any)]()
                vb += kv
                (k, vb)
              }
          }
        }
        vbMap map (_ :-> (_.result.map(_._2)))
      }

    modeLogger.debug("grouped:")
    grouped.zipWithIndex foreach { case (p, ix) => modeLogger.debug(ix + ": " + p) }

    /* Concatenate */
    Vector(grouped.flatten:_*).debug("computeGroupByKey")
  }


  private def computeCombine(combine: Combine)(implicit sc: ScoobiConfiguration): Seq[_] =
    (combine.in -> compute(sc)).map { case (k, vs: Iterable[_]) =>
      (k, combine.combine(vs))
    }.debug("computeCombine")


}


