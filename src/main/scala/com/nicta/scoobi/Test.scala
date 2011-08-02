/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi


/** Test the Hadoop-level interface. */
object HadoopTest {

  /** Canonical wordcount example. */
  object WordCount {

    def map(off: Long, line: String) = line.split(' ') map { (_, 1) } toList
    def reduce(word: String, cnts: List[Int]) = List((word, cnts.sum))

    val job = new HadoopJob(map, reduce)
  }


  /** Build a co-occurnece matrix from a corpa of documents. For simplicity, co-occurnece
    * is done within a line context only, not an entire document. */
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


  /** Run them! */
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


/** Test the language front-end */
object LanguageTest {

  import DList._

  /** Simple example demonstrating use of primitive and derived combinators. */
  def simple() = {
    println("simple:")
    val data = load(List(1,2,3,4,5,6,7,8,9,10))

    val trans = data.flatMap(x => List(x + 1))
                    .filter(_ % 2 == 0)

    println(trans.runInterp)
    println()
  }


  /** Canonical 'word count' example. */
  def wordcount() = {
    println("wordcount:")

    val document = List(
      "THIS SOFTWARE IS PROVIDED BY DAVID HALL ``AS IS'' AND ANY",
      "EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED",
      "WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE",
      "DISCLAIMED. IN NO EVENT SHALL DAVID HALL BE LIABLE FOR ANY",
      "DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES",
      "(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;",
      "LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND",
      "ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT",
      "(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS",
      "SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.")

    val lines = load(document)

    val fq: DList[(String, Int)] = lines.flatMap(_.split(" "))
                  .map { w => (w, 1) }
                  .groupByKey
                  .combine(0, (_+_))

    println(fq.runInterp)
    println()
  }


  /** "Join" example. */
  def join() = {
    println("join:")

    val d1 = List((1, "Joe"),
                  (2, "Kate"),
                  (3, "Peter"),
                  (4, "Rami"),
                  (6, "Fred"))
    val d2 = List((1, 36.8),
                  (2, 92.3),
                  (2, 49.1),
                  (3, 88.6),
                  (6, 12.9),
                  (7, 59.2))

    val dd1 = load(d1)
    val dd2 = load(d2)

    val dd1s: DList[(Int, (Option[String], Option[Double]))] = dd1.map { case (k, v) => (k, (Some(v), None)) }
    val dd2s: DList[(Int, (Option[String], Option[Double]))] = dd2.map { case (k, v) => (k, (None,    Some(v))) }

    val joined = (dd1s concat dd2s).groupByKey

    val fix = joined.map {
      case (k, vs) => {
        val v1 = vs flatMap { case (Some(d), _) => List(d); case _ => List() }
        val v2 = vs flatMap { case (_, Some(d)) => List(d); case _ => List() }
        (v1, v2)
      }
    }

    println(fix.runInterp)
    println()
  }

  /** Run them. */
  def main(args: Array[String]) {
    simple()
    wordcount()
    join()
  }
}
