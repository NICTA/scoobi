/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.{Reducer => HReducer, _}
import org.apache.hadoop.mapred.lib.MultipleOutputs


/** Hadoop Reducer class for an MSCR. */
class MscrReducer[K, V, B] extends HReducer[TaggedKey, TaggedValue, NullWritable, ScoobiWritable[B]] {

  private var outputs: Map[Int, (Int, TaggedReducer[_,_,_])] = _
  private var multipleOutputs: MultipleOutputs = _
  private var conf: JobConf = _

  def configure(conf: JobConf) = {
    outputs = DistributedObject.pullObject(conf, "scoobi.output.reducers").asInstanceOf[Map[Int, (Int, TaggedReducer[_,_,_])]]
    multipleOutputs = new MultipleOutputs(conf)
    this.conf = conf
  }

  def reduce(key: TaggedKey,
             values: java.util.Iterator[TaggedValue],
             output: OutputCollector[NullWritable, ScoobiWritable[B]],
             reporter: Reporter) = {

    val tag = key.tag
    val namedOutput = "ch" + tag

    /* Get the right output value type and output directory for the current channel,
     * specified by the key's tag. */
    val numOutputs = outputs(tag)._1
    val reducer = outputs(tag)._2.asInstanceOf[TaggedReducer[K, V, B]]

    val v = MultipleOutputs.getNamedOutputValueClass(conf, namedOutput + "out0").newInstance
                       .asInstanceOf[ScoobiWritable[B]]

    val collectors = (0 to numOutputs - 1) map { namedOutput + "out" + _ } map {
      multipleOutputs.getCollector(_, reporter)
                     .asInstanceOf[OutputCollector[NullWritable, ScoobiWritable[B]]]
    }

    /* Convert Iterator[TaggedValue] to Iterable[V]. */
    val valuesStream = Stream.continually(if (values.hasNext) values.next else null).takeWhile(_ != null)
    val untaggedValues = valuesStream.map(_.get(tag).asInstanceOf[V]).toIterable

    /* Do the reduction. */
    reducer.reduce(key.get(tag).asInstanceOf[K], untaggedValues).foreach { out =>
      v.set(out)
      collectors foreach { _.collect(NullWritable.get, v) }
    }
  }

  def close() = {
    multipleOutputs.close()
  }
}
