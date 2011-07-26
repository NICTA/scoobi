/**
  *
  * Copyright: [2011] Ben Lever
  *
  *
  */
package com.nicta.scoobi

import java.io._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred.{Reducer => HReducer, _}
import scala.collection.JavaConverters._


/**
  *
  *
  *
  */
trait Reducer[K2, V2, K3, V3] extends Serializable {
  def reduce(key: K2, values: List[V2]): List[(K3, V3)]
}




/**
  *
  *
  *
  */
class GenericReducer[K2, V2, K3, V3] extends HReducer[Writable, Writable, Writable, Writable] {

  var r: Reducer[K2, V2, K3, V3] = _

  def configure(conf: JobConf) = {
    val ois = new ObjectInputStream(new FileInputStream("/tmp/reducer.obj"))
    r = ois.readObject().asInstanceOf[Reducer[K2,V2,K3,V3]]
    ois.close()
  }

  def reduce(
      key: Writable,
      values: java.util.Iterator[Writable],
      output: OutputCollector[Writable, Writable],
      reporter: Reporter): Unit = {

    val valuesL = values.asScala.toList

    for { (k, v) <- r.reduce(Implicits.wireToReal(key).asInstanceOf[K2], valuesL.map(Implicits.wireToReal(_).asInstanceOf[V2])) }
      yield output.collect(Implicits.realToWire(k), Implicits.realToWire(v))
  }

  def close() = {
  }
}

