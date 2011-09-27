/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import org.apache.hadoop.mapred.{Reducer => HReducer, _}


/** Hadoop Combiner class for an MSCR. */
class MscrCombiner[V] extends HReducer[TaggedKey, TaggedValue, TaggedKey, TaggedValue] {

  private var combiners: Map[Int, TaggedCombiner[_]] = _
  private var tv: TaggedValue = _

  def configure(conf: JobConf) = {
    combiners = DistributedObject.pullObject(conf, "scoobi.combiners").asInstanceOf[Map[Int, TaggedCombiner[_]]]
    tv = conf.getMapOutputValueClass.newInstance.asInstanceOf[TaggedValue]
  }

  def reduce(key: TaggedKey,
             values: java.util.Iterator[TaggedValue],
             output: OutputCollector[TaggedKey, TaggedValue],
             reporter: Reporter) = {

    val tag = key.tag
    val valuesStream = Stream.continually(if (values.hasNext) values.next else null).takeWhile(_ != null)

    if (combiners.contains(tag)) {
      /* Only perform combining if one is available for this tag. */
      val combiner = combiners(tag).asInstanceOf[TaggedCombiner[V]]

      /* Convert Iterator[TaggedValue] to Iterable[V]. */
      val untaggedValues = valuesStream.map(_.get(tag).asInstanceOf[V]).toIterable

      /* Do the combining. */
      val reduction = untaggedValues.tail.foldLeft(untaggedValues.head)(combiner.combine)
      tv.set(tag, reduction)

      output.collect(key, tv)
    } else {
      /* If no combiner for this tag, TK-TV passes through. */
      valuesStream.foreach { value => output.collect(key, value) }
    }
  }

  def close() = {
  }
}
