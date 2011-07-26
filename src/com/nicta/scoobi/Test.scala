/**
  *
  * Copyright: [2011] Ben Lever
  *
  *
  */
package com.nicta.scoobi


/**
  * Canonical wordcount example.
  */
object WordCount {

  val m = new Mapper[Long, String, String, Int] {
    def map(off: Long, line: String) = line.split(' ') map { (_, 1) } toList
  }

  val r = new Reducer[String, Int, String, Int] {
    def reduce(word: String, cnts: List[Int]) = List((word, cnts.sum))
  }

  val job = new HadoopJob(m, r)
}


object Test {
  def main(args: Array[String]) = {
    if (args.length < 2) {
      println("Test <input> <output>")
    }
    else {
      WordCount.job.run(args(0), args(1))
    }
  }
}
