package com.nicta.scoobi
package impl
package plan
package graph

import org.specs2.mutable.{Tags, Specification}
import comp._
import org.specs2.ScalaCheck
import org.specs2.matcher.{Expectable, Matcher}
import CompNodePrettyPrinter._
import Optimiser._

class MscrGraphSpec extends Specification with CompNodeData with ScalaCheck with MscrGraph with Tags {
  // set the specification as sequential for now
  // otherwise there are cyclic attribute evaluations
  sequential

  override implicit def defaultParameters = set(minTestsOk -> 10000, maxSize -> 8)

  "1. We should build MSCRs around related GroupByKey nodes" >> {
    "1.1 if two GroupByKey nodes share the same input, they belong to the same Mscr" >> {
      val l1 = load
      val gbk1 = gbk(flatten(l1))
      val gbk2 = gbk(flatten(l1))
      mscrsFor(gbk1, gbk2)
      gbk1 -> mscr ==== gbk2 -> mscr
    }
    "1.2 if two GroupByKey nodes don't share the same input, they belong to 2 different Mscrs" >> {
      gbk(flatten(load)) -> mscr !== gbk(flatten(load)) -> mscr
    }
    "1.3 two Gbks in the same Mscr cannot be ancestor of each other" >> check { node: CompNode =>
      mscrsFor(node) foreach { m =>
        val gbks = m.groupByKeys
        gbks foreach { gbk =>
          gbks.filterNot(_ eq gbk) foreach { other => (other:CompNode) aka (pretty(node, mscr)) must not(beAnAncestorOf(gbk)) }
        }
      }
      ok
    }
    "1.3.1 a Gbk must have the exact same Mscr that references it" >> check { node: CompNode =>
      mscrsFor(node) foreach { m =>
        m.groupByKeys foreach { gbk => (gbk -> mscr) aka (pretty(node, mscr)) must be(m) }
      }
      ok
    }
    "1.4 if two Gbks have the same ParallelDo input, then this ParallelDo must belong to another Mscr" >> check { node: CompNode =>
      mscrsFor(node) foreach { m =>
        m.groupByKeys foreach { gbk =>
          descendents(gbk) collect {
            case p @ ParallelDo(_,_,_,_,_) => ((p -> mscr) aka "the mscr of "+p+" in "+pretty(node)+"\n") must be_!== (m)
          }
        }
      };  ok
    }
  }
  "2. We need to create, for each GroupByKey, a GbkOutputChannel" >> {
    "There should be a GbkOutputChannel for a GroupByKey" >> {
      val gbk1 = gbk(cb(load))
      mscrFor(gbk1).outputChannels must_== Set(GbkOutputChannel(gbk1))
    }
    "There should be one GbkOutputChannel for each GroupByKey in the Mscr" >> {
      val l1 = load
      val gbk1 = gbk(cb(l1))
      val gbk2 = gbk(cb(l1))
      mscrsFor(gbk1, gbk2).flatMap(_.outputChannels).toSeq must_== Seq(GbkOutputChannel(gbk1), GbkOutputChannel(gbk2))
    }
    "If the input of a GroupByKey is a Flatten node then add it to this channel" >> {
      val fl1  = flatten(load)
      val gbk1 = gbk(fl1)
      (gbk1 -> mscr).outputChannels must_== Set(GbkOutputChannel(gbk1, flatten = Some(fl1)))
    }
    "If the output of a GroupByKey is a Combine node then add it to this channel" >> {
      val gbk1 = gbk(rt)
      val cb1  = cb(gbk1)
      (cb1 -> mscr).outputChannels must_== Set(GbkOutputChannel(gbk1, combiner = Some(cb1)))
    }
    "If the Combine following a GroupByKey is followed by a ParallelDo, then the ParallelDo can be added as a reducer" >> {
      "if it has a groupBarrier" >> {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val pd1  = pd(cb1, groupBarrier = true)
        mscrFor(pd1).outputChannels must_== Set(GbkOutputChannel(gbk1, combiner = Some(cb1), reducer = Some(pd1)))
      }
      "if it has a fuseBarrier" >> {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val pd1  = pd(cb1, fuseBarrier = true)
        mscrFor(pd1).outputChannels must_== Set(GbkOutputChannel(gbk1, combiner = Some(cb1), reducer = Some(pd1)))
      }
      "if it has no successor" >> {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val pd1  = pd(cb1)
        mscrFor(pd1).outputChannels must_== Set(GbkOutputChannel(gbk1, combiner = Some(cb1), reducer = Some(pd1)))
      }
      "if it a Materialize successor" >> {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val pd1  = pd(cb1)
        val mat1 = mt(pd1)
        mscrFor(mat1).outputChannels must_== Set(GbkOutputChannel(gbk1, combiner = Some(cb1), reducer = Some(pd1)))
      }
      "but it's not added if none of those conditions is true" >> {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val fl1  = flatten(pd(cb1, groupBarrier = false, fuseBarrier = false))
        mscrFor(fl1).outputChannels must_== Set(GbkOutputChannel(gbk1, combiner = Some(cb1), reducer = None))
      }
    }
  }
  "3. We need to create InputChannels for each Mscr" >> {
    "3.1 We create a MapperInputChannel for all ParallelDos sharing the same input as the Mscr GroupByKey" >> check { node: CompNode =>
      mscrsFor(node)
      // collect the parallel does of the current mscr which are not reducers
      descendents(node).collect(isAParallelDo).filterNot(p => (p -> mscr).reducers.contains(p)) foreach { p =>
        (p -> mscr).mappers aka ("for node:\n"+pretty(node, mscr)+"\nand mscr\n"+(p -> mscr)) must contain(p)
      }; ok
    }
    "3.1.1 a ParallelDo can not be a mapper and a reducer at the same time" >> check { node: CompNode =>
      mscrsFor(node)
      // collect the parallel does of the current mscr which are not reducers
      descendents(node).collect(isAParallelDo) foreach { p =>
        ((p -> mscr).mappers intersect (p -> mscr).reducers) aka ("for node:\n"+pretty(node, mscr)+"\nand mscr\n"+(p -> mscr)) must beEmpty
      }; ok
    }
  }

  def beAnAncestorOf(node: CompNode): Matcher[CompNode] = new Matcher[CompNode] {
    def apply[S <: CompNode](other: Expectable[S]) =
      result(isAncestor(node, other.value),
             other.description+" is an ancestor of "+node+": "+(Seq(node) ++ ancestors(node)).mkString(" -> "),
             other.description+" is not an ancestor of "+node, other)
  }
}

