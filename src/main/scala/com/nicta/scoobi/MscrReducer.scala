/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.{Reducer => HReducer, _}
import org.apache.hadoop.mapred.lib.MultipleOutputs


/** Hadoop Reducer class for an MSCR. */
class MscrReducer[K, V, B] extends HReducer[TaggedKey, TaggedValue, NullWritable, ScoobiWritable[B]] {

  private var outputs: Map[Int, TaggedReducer[_,_,_]] = _
  private var multipleOutputs: MultipleOutputs = _
  private var conf: JobConf = _

  def configure(conf: JobConf) = {
    outputs = DistributedObject.pullObject(conf, "scoobi.output.reducers").asInstanceOf[Map[Int, TaggedReducer[_,_,_]]]
    multipleOutputs = new MultipleOutputs(conf)
    this.conf = conf
  }

  def reduce(key: TaggedKey,
             values: java.util.Iterator[TaggedValue],
             output: OutputCollector[NullWritable, ScoobiWritable[B]],
             reporter: Reporter) = {

    val tag = key.tag
    val namedOutput = "channel" + tag

    /* Get the right output value type and output directory for the current channel,
     * specified by the key's tag. */
    val reducer = outputs(tag).asInstanceOf[TaggedReducer[K, V, B]]

    val v = MultipleOutputs.getNamedOutputValueClass(conf, namedOutput).newInstance
                       .asInstanceOf[ScoobiWritable[B]]

    val collector = multipleOutputs.getCollector(namedOutput, reporter)
                                   .asInstanceOf[OutputCollector[NullWritable, ScoobiWritable[B]]]

    /* Convert Iterator[TaggedValue] to Iterable[V]. */
    val valuesStream = Stream.continually(if (values.hasNext) values.next else null).takeWhile(_ != null)
    val untaggedValues = valuesStream.map(_.get(tag).asInstanceOf[V]).toIterable

    /* Do the reduction. */
    reducer.reduce(key.get(tag).asInstanceOf[K], untaggedValues).foreach { out =>
      v.set(out)
      collector.collect(NullWritable.get, v)
    }
  }

  def close() = {
    multipleOutputs.close()
  }
}
