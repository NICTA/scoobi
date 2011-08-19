/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import java.io._
import java.lang.reflect.Constructor

import org.apache.hadoop.io._
import org.apache.hadoop.mapred.{Mapper => HMapper, _}


/** The Hadoop Mapper class used by all Scoobi Hadoop jobs.
  *
  * A generic Hadoop Mapper class that is used for all Scoobi Hadoop jobs. It
  * is simply a wrapper for the actual mapping functionality that has been
  * described in a Scoopi Mapper object. That object is serialized and
  * retrieved via the distributed cache whereby it is subsequently deserialized
  * and used. */
class GenericMapper[K1, V1, K2, V2]
  extends HMapper[ScoobiWritableComparable[K1],
                  ScoobiWritable[V1],
                  ScoobiWritableComparable[K2],
                  ScoobiWritable[V2]] {

  var m: Mapper[K1, V1, K2, V2] = _

  def configure(conf: JobConf) = {
    m = DistributedObject.pullObject(conf, "mapper").asInstanceOf[Mapper[K1,V1,K2,V2]]
  }

  def map(key: ScoobiWritableComparable[K1],
          value: ScoobiWritable[V1],
          output: OutputCollector[ScoobiWritableComparable[K2], ScoobiWritable[V2]],
          reporter: Reporter) = {

    for {
      (k, v) <- m.map(key.get.asInstanceOf[K1], value.get.asInstanceOf[V1])
      k2 = { var wc = m.k2WritableComparableClass.newInstance.asInstanceOf[ScoobiWritableComparable[K2]]; wc.set(k); wc }
      v2 = { var w = m.v2WritableClass.newInstance.asInstanceOf[ScoobiWritable[V2]]; w.set(v); w }
    } yield output.collect(k2, v2)
  }

  def close() = {
  }
}

