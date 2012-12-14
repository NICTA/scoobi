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
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.MapContext
import org.apache.hadoop.mapreduce.task.MapContextImpl
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import scala.collection.immutable.VectorBuilder
import scala.collection.JavaConversions._
import scalaz.{Scalaz, State, Memo, Ordering => _}
import Scalaz._

import core._
import impl.plan._
import Smart._
import io.DataSource
import io.DataSink


/**
  * A fast local mode for execution of Scoobi applications.
  */
object VectorMode {
  lazy val logger = LogFactory.getLog("scoobi.VectorMode")


  def prepareST(outputs: List[(Smart.DComp[_, _ <: Shape], Option[DataSink[_,_,_]])], conf: ScoobiConfiguration): Eval.ST = {
    outputs collect { case (_, Some(sink)) => sink } foreach { _.outputCheck(conf) }
    Eval.Vector(0)
  }


  def executeDListPersister[A, K, V](x: DListPersister[A], conf: ScoobiConfiguration): State[Eval.ST, Unit] = State({
    case Eval.Vector(st) => {
      val vec = computeArr(x.dlist.getComp)(conf)

      val job = new Job(new Configuration(conf))
      val sink = x.sink.asInstanceOf[DataSink[K, V, A]]
      val outputFormat = sink.outputFormat.newInstance()

      job.setOutputFormatClass(sink.outputFormat)
      job.setOutputKeyClass(sink.outputKeyClass)
      job.setOutputValueClass(sink.outputValueClass)
      job.getConfiguration.set("mapreduce.output.basename", "ch0out0")  // Attempting to be consistent
      sink.configureCompression(job)
      sink.outputConfigure(job)(conf)

      val tid = new TaskAttemptID()
      val taskContext = new TaskAttemptContextImpl(job.getConfiguration, tid)
      val rw = outputFormat.getRecordWriter(taskContext)
      val oc = outputFormat.getOutputCommitter(taskContext)

      oc.setupJob(job)
      oc.setupTask(taskContext)

      vec foreach { x =>
        val (k, v) = sink.outputConverter.toKeyValue(x)
        rw.write(k, v)
      }

      rw.close(taskContext)
      oc.commitTask(taskContext)
      oc.commitJob(job)

      (Eval.Vector(0), ())
    }
    case _ => sys.error("something went wrong")
  })


  def executeDObject[A](x: DObject[A], conf: ScoobiConfiguration): State[Eval.ST, A] = State({
    case Eval.Vector(st) => (Eval.Vector(0), computeExp(x.getComp)(conf))
    case _ => sys.error("something went wrong")
  })


  private def computeLoad[K, V, A](load: Load[A])(implicit conf: ScoobiConfiguration): Vector[A] = {

    /* Perorms a deep copy of an arbitrary object by first serialising then deserialising
     * it via its WireFormat. */
    def wireFormatCopy[A : WireFormat](x: A): A = {
      import java.io._
      val byteArrOs = new ByteArrayOutputStream()
      implicitly[WireFormat[A]].toWire(x, new DataOutputStream(byteArrOs))
      implicitly[WireFormat[A]].fromWire(new DataInputStream(new ByteArrayInputStream(byteArrOs.toByteArray())))
    }

    val vb = new VectorBuilder[A]()
    val job = new Job(new Configuration(conf))
    val source: DataSource[K, V, A] = load.source.asInstanceOf[DataSource[K, V, A]]
    val inputFormat = source.inputFormat.newInstance()

    job.setInputFormatClass(source.inputFormat)
    source.inputConfigure(job)

    inputFormat.getSplits(job) foreach { split =>
      val tid = new TaskAttemptID()
      val taskContext = new TaskAttemptContextImpl(job.getConfiguration, tid)
      val rr = inputFormat.createRecordReader(split, taskContext)
      val mapContext: MapContext[K, V, _, _] = new MapContextImpl(job.getConfiguration, tid, rr, null, null, null, split)

      rr.initialize(split, taskContext)
      while (rr.nextKeyValue()) {
        val k = rr.getCurrentKey()
        val v = rr.getCurrentValue()
        val a = source.inputConverter.fromKeyValue(mapContext, k, v)
        vb += wireFormatCopy(a)(load.wtA)
      }
      rr.close()
    }

    val vec = vb.result()
    logger.debug("computeLoad: " + vec)
    vec
  }


  private def computeParallelDo[A, B, E](pd: ParallelDo[A, B, E])(implicit conf: ScoobiConfiguration): Vector[B] = {
    val vb = new VectorBuilder[B]()
    val emitter = new Emitter[B] { def emit(v: B) = vb += v }

    val dofn = pd.dofn
    val env = computeExp(pd.env)
    dofn.setup(env)
    computeArr(pd.in) foreach { v => dofn.process(env, v, emitter) }
    dofn.cleanup(env, emitter)

    val vec = vb.result()
    logger.debug("computeParallelDo: " + vec)
    vec
  }


  private def computeGroupByKey[K, V](gbk: GroupByKey[K, V])(implicit conf: ScoobiConfiguration): Vector[(K, Iterable[V])] = {
    val in: Vector[(K, V)] = computeArr(gbk.in)
    val grp: Grouping[K] = gbk.grpK

    /* Partitioning */
    val partitions: IndexedSeq[Vector[(K, V)]] = {
      val numPart = 10    // TODO - set this based on input size? or vary it randomly?
      val vbs = IndexedSeq.fill(numPart)(new VectorBuilder[(K, V)]())
      in foreach { case kv@(k, _) => val p = grp.partition(k, numPart); vbs(p) += kv }
      vbs map { _.result() }
    }

    logger.debug("partitions:")
    partitions.zipWithIndex foreach { case (p, ix) => logger.debug(ix + ": " + p) }

    /* Grouping values */
    val grouped: IndexedSeq[Map[K, Vector[(K, V)]]] = partitions map { (kvs: Vector[(K, V)]) =>

      val vbMap = kvs.foldLeft(Map.empty: Map[K, VectorBuilder[(K, V)]]) { case (bins, (k, v)) =>
        bins.find(kkvs => grp.groupCompare(kkvs._1, k) == 0) match {
          case Some((kk, vb)) => bins.updated(kk, vb += ((k, v)))
          case None           => { val vb = new VectorBuilder[(K, V)](); bins + (k -> (vb += ((k, v)))) }
        }
      }

      vbMap map { case (k, vb) => (k, vb.result()) }
    }

    logger.debug("grouped:")
    grouped.zipWithIndex foreach { case (p, ix) => logger.debug(ix + ": " + p) }

    /* Sorting */
    val ord = new Ordering[K] { def compare(x: K, y: K): Int = grp.sortCompare(x, y) }

    val sorted: IndexedSeq[Map[K, Vector[(K, V)]]] = grouped map { (kvMap: Map[K, Vector[(K, V)]]) =>
      kvMap map { case (k, kvs) => (k, kvs.sortBy(_._1)(ord)) }
    }

    logger.debug("sorted:")
    sorted.zipWithIndex foreach { case (p, ix) => logger.debug(ix + ": " + p) }

    /* Concatenate */
    val vec = Vector(sorted.flatMap(_ map { case (k, kvs) => (k, kvs.map(_._2).toIterable) }): _*)
    logger.debug("computeGroupByKey: " + vec)
    vec
  }


  private def computeCombine[K, V](combine: Combine[K, V])(implicit conf: ScoobiConfiguration): Vector[(K, V)] = {
    val in: Vector[(K, Iterable[V])] = computeArr(combine.in)
    val vec = in map { case (k, vs) => (k, vs.reduce(combine.f)) }
    logger.debug("computeCombine: " + vec)
    vec
  }


  private def computeArr[A](comp: Smart.DComp[A, Arr])(implicit conf: ScoobiConfiguration): Vector[A] = comp match {
    case ld@Load(_)                   => computeLoad(ld)
    case pd@ParallelDo(_, _, _, _, _) => computeParallelDo(pd)
    case gbk@GroupByKey(_)            => computeGroupByKey(gbk)
    case c@Combine(in, op)            => computeCombine(c)
    case Flatten(ins)                 => ins.map(computeArr(_)).reduce(_++_)
  }

  private def computeExp[A](comp: Smart.DComp[A, Exp])(implicit conf: ScoobiConfiguration): A = comp match {
    case Materialise(in)    => computeArr(in).toIterable
    case Op(in1, in2, f)    => f(computeExp(in1), computeExp(in2))
    case Return(x)          => x
  }

}
