/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import java.io._


/** Test the language front-end */
object LanguageTest {

  import DList._
  import TextInput._
  import TextOutput._

  private val FILE_DIR = "src/test/resources"


  /** Simple example demonstrating use of primitive and derived combinators. */
  def simple() = {
    val data = fromTextFile(FILE_DIR + "/ints.txt").map(_.toInt)

    val trans = data map { _ + 1 } filter { _ % 2 == 0 }

    persist { toTextFile(trans, FILE_DIR + "/ints-out") }
  }


  /** Canonical 'word count' example. */
  def wordcount() = {
    val lines = fromTextFile(FILE_DIR + "/document.txt")

    val fq: DList[(String, Int)] = lines.flatMap(_.split(" "))
                                        .map { w => (w, 1) }
                                        .reduceByKey((_+_))

    persist { toTextFile(fq, FILE_DIR + "/fq-out") }
  }

  /** Database input */
  def avgAge() = {

    def average[A](values: Iterable[A])(implicit ev: Numeric[A]) = {
      import ev._
      toInt(values.sum) / values.size
    }

    /* Read in lines of the form: 234242, Ben, Lever, 31. */
    case class Person(val id: Int,
                      val secondName: String,
                      val firstName: String,
                      val age: Int) extends Serializable

    val persons = extractFromDelimitedTextFile(",", FILE_DIR + "/people.txt") {
                    case id :: fN :: sN :: age :: _ => Person(id.toInt, sN, fN, age.toInt)
                  }

    val raw: DList[(Int, String, String, Int)] = fromDelimitedTextFile(",", FILE_DIR + "/people.txt")

    val avgAgeForName: DList[(String, Int)] = persons.map(p => (p.firstName, p.age))
                                                     .groupByKey
                                                     .map{case (n, ages) => (n, average(ages))}

    val rawMod = raw map { case (id, fN, sN, age) => (id, age) }

    persist {
      toTextFile(avgAgeForName, FILE_DIR + "/avg-age");
      toTextFile(rawMod, FILE_DIR + "/raw-mod")
    }
  }

  /** "Join" example. */
  def join() = {
    val d1: DList[(Int, String)] = fromDelimitedTextFile(",", FILE_DIR + "/id-names.txt")
    val d2: DList[(Int, Double)] = fromDelimitedTextFile(",", FILE_DIR + "/id-cnt.txt")

    val d1s: DList[(Int, (Option[String], Option[Double]))] = d1.map { case (k, v) => (k, (Some(v), None)) }
    val d2s: DList[(Int, (Option[String], Option[Double]))] = d2.map { case (k, v) => (k, (None,    Some(v))) }

    val joined = (d1s ++ d2s).groupByKey flatMap { case (k, vs) =>
      val v1 = vs flatMap { case (Some(d), _) => List(d); case _ => List() }
      val v2 = vs flatMap { case (_, Some(d)) => List(d); case _ => List() }
      (v1, v2) match { case (Nil, ys) => Nil; case (x,   ys) => List((x.head, ys)) }
    }

    persist { toTextFile(joined, FILE_DIR + "/id-names-cnt") }
  }

  def graphTest() = {
    import com.nicta.scoobi.Intermediate._

    val d1: DList[(Int, String)] = fromDelimitedTextFile(",", FILE_DIR + "/id-cnt.txt")
    val d2: DList[(Int, Iterable[String])] = d1.groupByKey
    val d2_ = d1.groupByKey

    val d3 = d2.flatMap(_._2)
    val d4: DList[(Int, String)] = d2_.combine(_++_)

    persist(toTextFile(d3, FILE_DIR + "/d3"),
            toTextFile(d4, FILE_DIR + "/d4"))
  }

  def noReducerTest() = {
    val d0: DList[(Int, String)] = fromDelimitedTextFile(",", FILE_DIR + "/id-cnt.txt")
    val d1: DList[(Int, Iterable[String])] = d0.groupByKey

    persist(toTextFile(d1, FILE_DIR + "/d1"))
  }

  def bypassInputChannelTest() = {
    val d0: DList[(Int, String)] = fromDelimitedTextFile(",", FILE_DIR + "/id-cnt.txt")
    val d1: DList[(Int, Iterable[String])] = d0.groupByKey
    val d2: DList[(Int, Iterable[Iterable[String]])] = d1.groupByKey

    persist(toTextFile(d2, FILE_DIR + "/d2"))
  }

  /** Test out fusion of flattens and flatMap. */
  def optimiseTest() = {
    import com.nicta.scoobi.Intermediate._

    val d1 = fromTextFile("whatever")
    val d2 = fromTextFile("whatever, again")
    val d3 = fromTextFile("whatever, again, and again")

    val f1 = d1 ++ d2 ++ d3

    val m1 = f1 map ((_, 42))
    val m2 = m1 map (identity)
    val m3 = m2 map (identity)

    val g1 = m1.groupByKey
    val g2 = m2.groupByKey
    val g3 = m3.groupByKey

    val c3 = g3.combine((a: Int, b: Int) => a + b)
    val r3 = c3.map(identity)

    val m4 = f1 map ((_, List(42, 43, 44).toIterable))
    val c2 = m4.combine((a: Int, b: Int) => a + b)
    val m5 = c2 map (identity)

    val c4 = g2.combine((a: Int, b: Int) => a + b)
    val m6 = g2.map(t => (t._1, t._2.tail.foldLeft(t._2.head)(_+_)))
  }


  /** Run them. */
  def main(args: Array[String]) {
    Scoobi.setJarByClass(this.getClass)
    println("------------- simple ------------"); simple()
    println("------------ wordcount ----------"); wordcount()
    println("------------- avgAge ------------"); avgAge()
    println("-------------- join -------------"); join()
    println("------------ graphTest ----------"); graphTest()
    println("----- bypassInputChannelTest ----"); bypassInputChannelTest()
    println("---------- optimiseTest ---------"); optimiseTest()
  }
}
