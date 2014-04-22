/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobi
package core

import org.apache.hadoop.io.Text

import testing.mutable.NictaSimpleJobs
import Scoobi._
import org.specs2.matcher.TerminationMatchers
import org.specs2.ScalaCheck
import impl.plan.comp.CompNodeData._
import org.scalacheck._
import org.scalacheck.Arbitrary._
import org.specs2.execute.Skipped
import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.testing.TempFiles

class DListSpec extends NictaSimpleJobs with TerminationMatchers with ScalaCheck {

  "length" >> { implicit sc: SC =>
    DList(1, 2, 3).length.run must_== 3
  }

  "we can create a DList by calling a function repeatedly with 'tabulate' " >> { implicit sc: SC =>
    DList.tabulate(3)(_.toString).run.toList must_== List("0", "1", "2")
  }

  "we can create a DList with some elements" >> { implicit sc: SC =>
    DList(1, 2, 3).run.toList must_== List(1, 2, 3)
  }

  "it must be possible to create an empty DList from an empty Seq and persist it" >> { implicit sc: SC =>
    DList[Int]().run must_== Seq()
  }

  "it must be possible to create an empty DList from a missing file and persist it" >> { implicit sc: SC =>
    val list = fromTextFiles(Seq("missing"), check = Source.noInputCheck)
    list.run === Vector()
  }

  tag("issue 99")
  "a DList can be created and persisted with some Text" >> { implicit sc: SC =>
    val list = DList(("key1", "value1"), ("key2", "value2")).map { case (k, v) => (new Text(k), new Text(v)) }
    run(list).map(_.toString).sorted must_== Seq("(key1,value1)", "(key2,value2)")
  }

  tag("issue 117")
  "A complex graph example must not throw an exception" >> { implicit sc: SC =>

    def simpleJoin[T: WireFormat, V: WireFormat](a: DList[(Int, T)], b: DList[(Int, V)]) =
      (a.map(x => (x._1, x._1)) ++ b.map(x => (x._1, x._1))).groupByKey

    val data = DList(12 -> 13, 14 -> 15, 13 -> 55)
    val (a, b, c, d, e) = (data, data, data, data, data)

    // joinab = joincd = Vector((12,Vector(12, 12)), (13,Vector(13, 13)), (14,Vector(14,14)))
    val (joinab, joincd) = (simpleJoin(a, b), simpleJoin(c, d))
    // q = Vector((12,Vector(12, 12)), (13,Vector(13, 13)), (14,Vector(14,14)))
    val q = simpleJoin(joinab, joincd)
    // qe = Vector((12,Vector(12, 12)), (13,Vector(13, 13)), (14,Vector(14,14)))
    val qe = simpleJoin(q, e).groupByKey
    val res = simpleJoin(q, qe)

    normalise(res.run) === "Vector((12,Vector(12, 12)), (13,Vector(13, 13)), (14,Vector(14, 14)))"
  }

  tag("issue 119")
  "joining an object created from random elements and a DList must not crash" >> { implicit sc: SC =>
    val r = new scala.util.Random

    val s = (1 to 10).map(i => (i, r.nextInt(i))).
                      groupBy(_._2).
                      mapValues(r.shuffle(_))

    (DObject(s) join DList(1, 2, 3)).run must not(throwAn[Exception])
  }

  tag("issue 137")
  "DList.concat will concatenate multiple DLists." >> { implicit sc: SC =>
    val aa = DList(1 to 5)
    val bb = DList(6 to 10)

    (aa ++ bb).run.sorted must_== (1 to 10).toSeq
  }

  tag("issue 194")
  "Length of an empty list should be zero" >> { implicit sc: SC =>
     DList[Int]().length.run === 0
  }

  tag("issue 319")
  "Sum and maps" >> { implicit sc: ScoobiConfiguration =>
    implicit val wf = WireFormat.EitherFmt[String, Int]

    val dlist = DList[Either[String, Int]](Left("test"), Right(1))

    val lefts = dlist.collect { case Left(l) => l+"2" }
    val rights = dlist.collect { case Right(r) => r+1 }

    val other = rights.map[Either[String, Int]](r => Left[String, Int](r.toString+"3"))
    val otherLefts = other.collect { case Left(l) => l+"last" }

    val all = (lefts ++ otherLefts)

    val size = all.length
    persist(size, all.toTextFile(path(TempFiles.createTempDir("bug").getPath), overwrite = true))
    size.run must_== 2
  }

  "DLists can be concatenated via reduce" >> {
    "without group by key" >> { implicit sc: SC =>
      Seq.fill(5)(DList(1 -> 2)).reduce(_++_).run === Seq.fill(5)(1 -> 2)
    }
    "with a group by key" >> { implicit sc: SC =>
      Seq.fill(5)(DList(1 -> 2)).reduce(_++_).groupByKey.run.toList.toString === Seq(1 -> Vector.fill(5)(2)).toString
    }
  }

  "DList zipWithIndex works" >> { implicit sc: SC =>
    val len   = 1
    val dlist = DList(1 to len).map(_.toString)

    val withIndexes = dlist.zipWithIndex
    val words       = withIndexes.map(_._1)
    val indexes     = withIndexes.map(_._2)


    val (uniqueWords, minIndex, uniqueIndexes, maxIndex) = run(words.distinct.size, indexes.min, indexes.distinct.size, indexes.max)

    uniqueWords   === len
    minIndex      === 0
    uniqueIndexes === len
    maxIndex      === (len - 1)
  }

  "DList distinct works" >> { implicit sc: SC =>
    implicit def fmt = mkCaseWireFormat(PoorHashString, PoorHashString.unapply _)

    val words = (1 to 1000).map { x => PoorHashString(scala.util.Random.nextString(1)) }

    words.toDList.distinct.size.run must_== words.distinct.size
  }

  "DList isEqual works" >> { implicit sc: SC =>

    val as = (1 to 100).map(scala.util.Random.nextInt(_).toString) // lots of dupes
    val bs = scala.util.Random.shuffle(as)

    val a = as.toDList
    val b = bs.toDList
    val a1 = a.filter(_ != bs.head)

    run((a isEqual a, a isEqual b, b isEqual a, a1 isEqual a)) must_== (true, true, true, false)
  }

  "A shuffled DList contains the same elements" >> { implicit sc: SC =>
    val shuffleProp = (list: List[Int]) => {
      val orig = list.toDList
      val result = run(orig.shuffle isEqual orig)
      result must_== true
    }
    Prop.forAll(arbitrary[List[Int]])(shuffleProp).set(minTestsOk = 1, minSize = 3, maxSize = 3, workers = 1)
  }

  tag("issue 256")
  "A DList can be created from a sequence of elements which will only be evaluated when executed" >> { implicit sc: SC =>
    val out: StringBuffer = new StringBuffer
    val list = fromLazySeq(Seq(1, 2, 3))
    "effect is not evaluated" ==> { out.toString must beEmpty }
    list.run.normalise === "Vector(1, 2, 3)"
    "effect is evaluated" ==> { out.toString must not be empty }.unless(sc.isRemote)
  }

  "DList diff'ing actually works works" >> { implicit sc: SC =>

    val eqProp = (first: List[Int], second: List[Int]) => {
      run(first.toDList diff second.toDList).sorted must_== (first diff second).sorted
      run(first.toDList distinctDiff second.toDList).sorted must_== (first.toSet diff second.toSet).toList.sorted
    }

    Prop.forAllNoShrink(arbitrary[List[Int]], arbitrary[List[Int]])(eqProp).set(minTestsOk = 5, minSize = 0, maxSize = 10)
  }

  "Partitioning a DList doesn't add or remove any elements" >> {

    /**
     * Property that when a DList is split into 1 or more, partitions, the concatenation of
     * all partitions contains the same set of elements as the original DList.
     */
    def partitionProperty(f: DList[Int] => Seq[DList[Int]])(implicit sc: ScoobiConfiguration): List[Int] => Prop = {
      (list: List[Int]) => {
        (list.length > 0) ==>  {
          val orig = list.toDList
          val partitions = f(orig)
          run(orig isEqual partitions.reduce(_ ++ _))(sc.duplicate) must beTrue
        }
      }
    }

    def check(p: Prop) = p.set(minTestsOk = 5, minSize = 0, maxSize = 100000)

    "Into 2 DLists" >> { implicit sc: SC =>
      val prop = partitionProperty { xs =>
        val (l, r) = xs.partition(_ => scala.util.Random.nextBoolean)
        Seq(l, r)
      }
      check { Prop.forAllNoShrink(arbitrary[List[Int]])(prop) }
    }

    "Stratifying" >> { sc: SC =>
      val prop = (n: Int, list: List[Int]) => partitionProperty(_.stratify(n)(i => math.abs(i % n)))(sc)(list)
      check { Prop.forAllNoShrink(Gen.choose(1, 10), arbitrary[List[Int]])(prop) }
    }

    "Stratifying with weights" >> { sc: SC =>
      def genWeights: Gen[Seq[Int]] = for {
        n  <- Gen.choose(1, 10)
        ws <- Gen.containerOfN[List, Int](n, Gen.choose(1, 30))
      } yield ws.toSeq
      val prop = (ws: Seq[Int], list: List[Int]) => partitionProperty(_.stratifyWeighted(ws))(sc)(list)
      check { Prop.forAllNoShrink(genWeights, arbitrary[List[Int]])(prop) }
    }
  }

  "DList#materialise doesn't exceed the client file descriptor limit" >> { implicit sc: SC =>
    import scala.sys.process._
      import scala.util.Random
    if (sc.isRemote) {
      /* Force the number of part files generated to be equal to the file descriptor limit. */
      val n = "ulimit -n".lines_!.headOption.map(_.toInt).getOrElse(0)
      sc.setMinReducers(n)

      val seq = Seq.fill(100000)("a")
      val out = seq.toDList.groupBy(_ => Random.nextInt()).mapFlatten(_._2).materialise.run
      (out.toSeq must_== seq).toResult
    } else Skipped("only run this test on the cluster")
  }.pendingUntilFixed("Find how to execute ulimit on the cluster")

  "A DList can be shorter than the number of 'hinted at' map tasks" >> { implicit sc: SC =>
    sc.set("mapred.map.tasks", 10)
    val in = DList(1, 1, 1)
    in.run.toSeq must_== Seq(1, 1, 1)
  }

  tag("issue 328")
  "It must be possible to concatenate result lists" >> { implicit sc: SC =>

    val init = DList(1, 2, 3, 2)

    val validated: DList[Either[String, Int]] = init.map(i => if(i < 2) Left("number too low") else Right(i))
    val errs: DList[String] = validated.collect { case Left(e) => e }
    val good: DList[Int] = validated.collect { case Right(i) => i }

    val secondVal: DList[Either[String, Int]] = good.groupBy(identity).collect {
      case (k, vs) if vs.size > 1 => Right(k)
      case (k, vs)                => Left(s"Too few entries in group $k")
    }

    val secondErrs = secondVal.collect { case Left(e) => e }
    val secondGood = secondVal.collect { case Right(i) => i }

    persist((errs ++ secondErrs), secondGood)
    secondGood.run.toList must_== List(2)
  }

  "A DList with lots of inputs must not overflow during the Scoobi job compilation" >> { implicit sc: SC =>

    val list = (1 to 1000).foldLeft(DList(0)) { (res, cur) => res ++ DList(cur) }
    list.run
    ok
  }

}

case class PoorHashString(s: String) {
  override def hashCode() = s.hashCode() & 0xff
}