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

    val trans = data.map(_ + 1)
                    .filter(_ % 2 == 0)

    persist { toTextFile(trans, FILE_DIR + "/int-out.txt") }
  }


  /** Canonical 'word count' example. */
  def wordcount() = {
    val lines = fromTextFile(FILE_DIR + "/document.txt")

    val fq: DList[(String, Int)] = lines.flatMap(_.split(" "))
                                        .map { w => (w, 1) }
                                        .reduceByKey((_+_))

    persist { toTextFile(fq, FILE_DIR + "/fq-out.txt") }
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
      toTextFile(avgAgeForName, FILE_DIR + "/avg-age.txt");
      toTextFile(rawMod, FILE_DIR + "/raw-mod.txt")
    }
  }

  /** "Join" example. */
  def join() = {
    val d1: DList[(Int, String)] = fromDelimitedTextFile(",", FILE_DIR + "/id-names.txt")
    val d2: DList[(Int, Double)] = fromDelimitedTextFile(",", FILE_DIR + "/id-cnt.txt")

    val d1s: DList[(Int, (Option[String], Option[Double]))] = d1.map { case (k, v) => (k, (Some(v), None)) }
    val d2s: DList[(Int, (Option[String], Option[Double]))] = d2.map { case (k, v) => (k, (None,    Some(v))) }

    val joined = (d1s concat d2s).groupByKey

    val fix = joined.map {
      case (k, vs) => {
        val v1 = vs flatMap { case (Some(d), _) => List(d); case _ => List() }
        val v2 = vs flatMap { case (_, Some(d)) => List(d); case _ => List() }
        (v1, v2)
      }
    }

    persist { toTextFile(fix, FILE_DIR + "/id-names-cnt.txt") }
  }

  def graphTest() = {
    import com.nicta.scoobi.Intermediate._

    val d1: DList[(Int, String)] = fromDelimitedTextFile(",", FILE_DIR + "/test.txt")
    val d2: DList[(Int, Iterable[String])] = d1.groupByKey
    val d3 = d2.flatMap(_._2)
    val d4: DList[(Int, String)] = d2.combine(_++_)
    val d5: DList[String] = d4.flatMap(x => List(x._2))
    persist { toTextFile(d3, FILE_DIR + "/some-name.txt") }
    persist { toTextFile(d4, FILE_DIR + "/some-name.txt") }
    persist { toTextFile(d5, FILE_DIR + "/some-name.txt") }
  }


  /** Run them. */
  def main(args: Array[String]) {
    try {
      graphTest()
      //    simple()
      //    wordcount()
      //    avgAge()
      //join()

    } catch {
      case ex: RuntimeException => {
        println(ex.toString())
        throw new RuntimeException("failed")
      }
    }


  }
}
