/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import java.io._
import java.lang.reflect.Constructor

import org.apache.hadoop.io._
import org.apache.hadoop.mapred.{Reducer => HReducer, _}

import scala.collection.JavaConverters._


/** The Hadoop Reducer class used by all Scoobi Hadoop jobs.
  *
  * A generic Hadoop Reducer class that is used for all Scoobi Hadoop jobs. It
  * is simply a wrapper for the actual reducing functionality that has been
  * described in a Scoopi Reducer object. That object is serialized and
  * retrieved via the distributed cache whereby it is subsequently deserialized
  * and used. */
class GenericReducer[K2, V2, K3, V3]
  extends HReducer[ScoobiWritableComparable[K2],
                   ScoobiWritable[V2],
                   ScoobiWritableComparable[K3],
                   ScoobiWritable[V3]] {

  var r: Reducer[K2, V2, K3, V3] = _

  def configure(conf: JobConf) = {
    r = DistributedObject.pullObject(conf, "reducer").asInstanceOf[Reducer[K2,V2,K3,V3]]
  }

  def reduce(
      key: ScoobiWritableComparable[K2],
      values: java.util.Iterator[ScoobiWritable[V2]],
      output: OutputCollector[ScoobiWritableComparable[K3], ScoobiWritable[V3]],
      reporter: Reporter): Unit = {

    val valuesL = values.asScala.toList

    for {
      (k, v) <- r.reduce(key.get.asInstanceOf[K2], valuesL.map { _.get.asInstanceOf[V2] })
      k3 = { var wc = r.k3WritableComparableClass.newInstance.asInstanceOf[ScoobiWritableComparable[K3]]; wc.set(k); wc }
      v3 = { var w = r.v3WritableClass.newInstance.asInstanceOf[ScoobiWritable[V3]]; w.set(v); w }
    } yield output.collect(k3, v3)
  }

  def close() = {
  }
}

