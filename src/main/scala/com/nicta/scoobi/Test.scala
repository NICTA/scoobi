/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi


/**
  * Canonical wordcount example.
  */
object WordCount {

  def map(off: Long, line: String) = line.split(' ') map { (_, 1) } toList
  def reduce(word: String, cnts: List[Int]) = List((word, cnts.sum))

  val job = new HadoopJob(map, reduce)
}


/**
  * Build a co-occurnece matrix from a corpa of documents. For simplicity, co-occurnece
  * is done within a line context only, not an entire document.
  */
object CooccurenceMatrix {

  val windowSize = 4

  def map(off: Long, line: String) = {

    val words = line.split(' ').toList
    val windows = words.sliding(windowSize).toList
    val pairs = windows map { w => w.tail map { t => (w.head, t) } }

    pairs.flatten.toList map { (_, 1) }
  }

  def reduce(pair: (String, String), cnts: List[Int]) = List((pair, cnts.sum))

  val job = new HadoopJob(map, reduce)
}



object Test {
  def main(args: Array[String]) = {
    if (args.length < 2) {
      println("Test <input> <output>")
    }
    else {
      //WordCount.job.execute(args(0), args(1))
      CooccurenceMatrix.job.execute(args(0), args(1))
    }
  }
}


