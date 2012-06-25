package com.nicta.scoobi
package acceptance

import Scoobi._
import testing.NictaSimpleJobs
import testing.TestFiles
import org.apache.hadoop.io._
import org.specs2.matcher.MatchResult
import org.specs2.matcher.HaveTheSameElementsAs
import collection.immutable.Map

class AvroFileReadWriteSpec extends NictaSimpleJobs {

  "Reading (Int, Seq[(Float, String)], Map[String, Int]) Avro file" >> { implicit sc: SC =>
    run {
      // create test data
      val testData: Seq[(Int, Seq[(Float, String)], Map[String, Int])] = Seq(
        (1, Seq((3.4f, "abc")), Map("a" -> 5, "b" -> 6)),
        (2, Seq((5.1f, "def")), Map("c" -> 7, "d" -> 8)))

      // store test data in an avro file
      val tmpAvroFile = createTempAvroFile(testData.toDList)

      // load test data from the avro file
      val loadedTestData = fromAvroFile[(Int, Seq[(Float, String)], Map[String, Int])](tmpAvroFile)
      persist(loadedTestData.materialize).toSeq must_== testData
    }
  }

  "Writing (String, List[(Double,Boolean,String)], Array[Long]) Avro file" >> { implicit sc: SC =>
    run {
      val filePath = createTempFile()

      // create test data
      val testData: Seq[(String, List[(Double, Boolean, String)], Array[Long])] = Seq(
        ("abcd", List((6.9d, false, "qwerty")), Array(100l, 200l)),
        ("efghi", List((9.15d, true, "dvorak")), Array(9999l, 11111l)))

      // write the test data out
      persist(toAvroFile(testData.toDList, filePath, true))

      // load the test data back, and check
      val loadedTestData: DList[(String, List[(Double, Boolean, String)], Array[Long])] = fromAvroFile(filePath)
      persist(loadedTestData.materialize).toSeq must haveTheSameElementsAs(testData, equality)
    }
  }

  /**
   * Helper methods and classes
   */

  def createTempAvroFile[T](input: DList[T])(implicit sc: SC, as: AvroSchema[T]): String = {
    val initialTmpFile = createTempFile()
    persist(toAvroFile(input, initialTmpFile, true))
    initialTmpFile
  }

  def createTempFile(prefix: String = "iotest")(implicit sc: SC): String = TestFiles.path(TestFiles.createTempFile(prefix))

  def run(f: => MatchResult[Any])(implicit sc: SC): MatchResult[Any] = try { f } finally { TestFiles.deleteFiles; sc.deleteWorkingDirectory; }

  val equality = (t1: Any, t2: Any) => (t1, t2) match {
    case (tt1: Array[_], tt2: Array[_]) => tt1.toSeq == tt2.toSeq
    case (tt1: Iterable[_], tt2: Iterable[_]) => iterablesEqual(tt1, tt2)
    case (tt1: Product, tt2: Product) => productsEqual(tt1, tt2)
    case other => t1 == t2
  }

  def productsEqual(t1: Product, t2: Product): Boolean = {
    if(t1.productArity != t2.productArity) false
    val i1 = t1.productIterator
    val i2 = t2.productIterator
    while (i1.hasNext && i2.hasNext)
      if (!equality(i1.next, i2.next)) false
    true
  }

  def iterablesEqual[T](t1: Iterable[T], t2: Iterable[T]): Boolean = {
    if(t1.size != t2.size) false
    val i1 = t1.iterator
    val i2 = t2.iterator
    while (i1.hasNext && i2.hasNext)
      if (!equality(i1.next, i2.next)) false
    true
  }

  def haveTheSameElementsAs[T](seq: Seq[T], equality: (T, T) => Boolean) = new HaveTheSameElementsAs(seq, equality)
}


