package com.nicta.scoobi
package impl
package exec

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.task.MapContextImpl
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import scala.collection.immutable.VectorBuilder
import scala.collection.JavaConversions._
import org.kiama.attribution._

import core._
import monitor.Loggable._
import impl.plan._
import comp._
import CompNodes._
import WireFormat._
import ScoobiConfigurationImpl._
import ShowNode._

/**
 * A fast local mode for execution of Scoobi applications.
 */
case class InMemoryMode() extends Attribution {

  implicit lazy val logger = LogFactory.getLog("scoobi.VectorMode")

  def execute(list: DList[_])(implicit sc: ScoobiConfiguration) {
    list.getComp -> prepare(sc)
    list.getComp -> compute(sc)
  }

  def execute(o: DObject[_])(implicit sc: ScoobiConfiguration) = {
    o.getComp -> prepare(sc)
    o.getComp -> computeValue(sc)
  }

  private
  lazy val prepare: ScoobiConfiguration => CompNode => Unit =
    paramAttr { sc: ScoobiConfiguration => { node: CompNode =>
      logger.debug("nodes\n"+pretty(node))
      logger.debug("graph\n"+showGraph(node))
      node.sinks.foreach(_.outputCheck(sc))
      node.children.foreach(_.asNode -> prepare(sc))
    }}

  private
  lazy val computeValue: ScoobiConfiguration => CompNode => Any =
    paramAttr { sc: ScoobiConfiguration => (n: CompNode) =>
      (n -> compute(sc)).head
    }

  private
  lazy val compute: ScoobiConfiguration => CompNode => Seq[_] =
    paramAttr { sc: ScoobiConfiguration => (n: CompNode) =>
      implicit val c = sc
      n match {
        case n: Load[_]           => saveSinks(computeLoad(n)                                     , n.sinks)
        case n: ParallelDo[_,_,_] => saveSinks(computeParallelDo(n)                               , n.sinks)
        case n: GroupByKey[_,_]   => saveSinks(computeGroupByKey(n)                               , n.sinks)
        case n: Combine[_,_]      => saveSinks(computeCombine(n)                                  , n.sinks)
        case n: Flatten[_]        => saveSinks(Vector(n.ins.map(_ -> compute(c)).reduce(_++_):_*) , n.sinks)
        case n: Materialize[_]    => saveSinks(Vector(n.in -> compute(c))                         , n.sinks)
        case n: Op[_,_,_]         => saveSinks(Vector(n.unsafeExecute(n.in1 -> computeValue(c),
                                                                      n.in2 -> computeValue(c)))  , n.sinks)
        case n: Return[_]         => saveSinks(Vector(n.in)                                       , n.sinks)
      }
    }

  private def computeLoad(load: Load[_])(implicit sc: ScoobiConfiguration): Seq[_] = {

    val vb = new VectorBuilder[Any]()
    val job = new Job(new Configuration(sc))
    val source = load.source
    val inputFormat = source.inputFormat.newInstance

    job.setInputFormatClass(source.inputFormat)
    source.inputConfigure(job)

    inputFormat.getSplits(job) foreach { split =>
      val tid = new TaskAttemptID()
      val taskContext = new TaskAttemptContextImpl(job.getConfiguration, tid)
      val rr = inputFormat.createRecordReader(split, taskContext)
      val mapContext = new MapContextImpl(job.getConfiguration, tid, rr, null, null, null, split)

      rr.initialize(split, taskContext)

      source.unsafeRead(rr, mapContext, (a: Any) => vb += wireFormatCopy(a)(load.wf.asInstanceOf[WireFormat[Any]]))
      rr.close()
    }

    vb.result.debug("computeLoad")
  }


  private def computeParallelDo(pd: ParallelDo[_,_,_])(implicit sc: ScoobiConfiguration): Seq[_] = {
    val vb = new VectorBuilder[Any]()
    val emitter = new Emitter[Any] { def emit(v: Any) { vb += v } }

    val (dofn, env) = (pd.dofn, (pd.env -> compute(sc)).head)
    dofn.unsafeSetup(env)
    (pd.in -> compute(sc)).foreach { v => dofn.unsafeProcess(env, v, emitter) }
    dofn.unsafeCleanup(env, emitter)
    vb.result.debug("computeParallelDo")

  }


  private def computeGroupByKey(gbk: GroupByKey[_,_])(implicit sc: ScoobiConfiguration): Seq[_] = {
    val in = gbk.in -> compute(sc)
    val grp = gbk.gpk

    /* Partitioning */
    val partitions = {
      val numPart = 10    // TODO - set this based on input size? or vary it randomly?
      val vbs = IndexedSeq.fill(numPart)(new VectorBuilder[Any]())
      in foreach { case kv @ (k, _) => val p = grp.unsafePartition(k, numPart); vbs(p) += kv }
      vbs map { _.result() }
    }

    logger.debug("partitions:")
    partitions.zipWithIndex foreach { case (p, ix) => logger.debug(ix + ": " + p) }

    /* Grouping values */
    val grouped = partitions map { (kvs: Vector[_]) =>

      val vbMap = kvs.foldLeft(Map.empty: Map[Any, VectorBuilder[Any]]) { case (bins, (k, v)) =>
        bins.find(kkvs => grp.unsafeGroupCompare(kkvs._1, k) == 0) match {
          case Some((kk, vb)) => bins.updated(kk, vb += ((k, v)))
          case None           => { val vb = new VectorBuilder[Any](); bins + (k -> (vb += ((k, v)))) }
        }
      }

      vbMap map { case (k, vb) => (k, vb.result()) }
    }

    logger.debug("grouped:")
    grouped.zipWithIndex foreach { case (p, ix) => logger.debug(ix + ": " + p) }

    /* Sorting */
    val ord = new Ordering[Any] { def compare(x: Any, y: Any): Int = grp.unsafeSortCompare(x, y) }

    val sorted = grouped map { (kvMap: Map[_, Vector[_]]) =>
      kvMap map { case (k, kvs) => (k, kvs.sortBy(vs => vs.asInstanceOf[Tuple2[_,_]]._1)(ord)) }
    }

    logger.debug("sorted:")
    sorted.zipWithIndex foreach { case (p, ix) => logger.debug(ix + ": " + p) }

    /* Concatenate */
    Vector(sorted.flatMap(_.toSeq map { case (k, kvs) => (k, kvs.map(vs => vs.asInstanceOf[Tuple2[_,_]]._2)) }): _*).debug("computeGroupByKey")

  }


  private def computeCombine(combine: Combine[_,_])(implicit sc: ScoobiConfiguration): Seq[_] =
    (combine.in -> compute(sc)).map { case (k, vs: Iterable[_]) =>
      (k, combine.unsafeReduce(vs))
    }.debug("computeCombine")

  def saveSinks(result: Seq[_], sinks: Seq[Sink])(implicit sc: ScoobiConfiguration): Seq[_] = {
    sinks.foreach { sink =>
      val job = new MapReduceJob(stepId = 1).configureJob(sc)(new Job(new Configuration(sc)))

      val outputFormat = sink.outputFormat.newInstance()

      job.setOutputFormatClass(sink.outputFormat)
      job.setOutputKeyClass(sink.outputKeyClass)
      job.setOutputValueClass(sink.outputValueClass)
      job.getConfiguration.set("mapreduce.output.basename", "ch0out0")  // Attempting to be consistent
      sink.configureCompression(job)
      sink.outputConfigure(job)(sc)

      val tid = new TaskAttemptID()
      val taskContext = new TaskAttemptContextImpl(job.getConfiguration, tid)
      val rw = outputFormat.getRecordWriter(taskContext)
      val oc = outputFormat.getOutputCommitter(taskContext)

      oc.setupJob(job)
      oc.setupTask(taskContext)

      sink.unsafeWrite(result, rw)

      rw.close(taskContext)
      oc.commitTask(taskContext)
      oc.commitJob(job)
    }
    result
  }


}

