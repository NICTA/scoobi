package com.nicta.scoobi
import impl._
import impl.exec.DistCache
import Scoobi._
import org.apache.hadoop.conf.Configuration

object ScoobiMain extends ScoobiApp {

  def module(o: Object) = {
    o.getClass.getField("MODULE$").get(this)
  }

  DistCache.pushObject(conf, module(this), "scoobi-ScoobiMain")

  val threshold = 2
  val list = DList(1, 2, 3, 4)

  val result = list.filter { i => i > threshold }.materialize
  persist(result.use)
  println(result.get)

}

