/**
  *
  * Copyright: [2011] Ben Lever
  *
  *
  */
package com.nicta.scoobi

import java.io._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred.{Mapper => HMapper, _}

/**
  *
  *
  *
  */
trait Mapper[K1, V1, K2, V2] extends Serializable {
  def map(key: K1, value: V1): List[(K2, V2)]
}




/**
  *
  *
  *
  */
class GenericMapper[K1, V1, K2, V2] extends HMapper[Writable, Writable, Writable, Writable] {

  var m: Mapper[K1, V1, K2, V2] = _

  def configure(conf: JobConf) = {
    val ois = new ObjectInputStream(new FileInputStream("/tmp/mapper.obj"))
    m = ois.readObject().asInstanceOf[Mapper[K1,V1,K2,V2]]
    ois.close()
  }

  def map(key: Writable, value: Writable, output: OutputCollector[Writable, Writable], reporter: Reporter) = {
    for {(k, v) <- m.map(Implicits.wireToReal(key).asInstanceOf[K1], Implicits.wireToReal(value).asInstanceOf[V1])}
      yield output.collect(Implicits.realToWire(k), Implicits.realToWire(v))
  }

  def close() = {
  }
}

