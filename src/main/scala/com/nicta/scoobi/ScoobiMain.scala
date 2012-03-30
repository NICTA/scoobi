package com.nicta.scoobi
import impl._
import impl.exec.DistCache
import Scoobi._
import org.apache.hadoop.conf.Configuration

object ScoobiMain extends ScoobiApp {

  def module(o: Object) = {
    o.getClass.getField("MODULE$").get(this)
  }
  val c = new Configuration
  c.set("scoobi.jobid", "1")
  val beforeMap = Map(conf.toMap.toList:_*)
  println("before")
  beforeMap.toList.sortBy(_._1) foreach println

  DistCache.pushObject(conf, "hello", "scoobi-ScoobiMain")
  val afterMap = Map(conf.toMap.toList:_*)
  println("after diff")
  (afterMap -- beforeMap.keys).toList.sortBy(_._1) foreach println

  val threshold = 2
  val list = DList(1, 2, 3, 4)

  val result = list.filter { i => i > threshold }.materialize
  persist(result.use)
  println(result.get)

  val endMap = Map(conf.toMap.toList:_*)
  println("end diff")
  (endMap -- afterMap.keys).toList.sortBy(_._1) foreach println
}

