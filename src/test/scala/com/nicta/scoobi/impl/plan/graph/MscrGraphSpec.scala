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
import org.specs2.main.CommandLineArguments

class MscrGraphSpec extends Specification with CompNodeData with ScalaCheck with MscrGraph with Tags {
  // set the specification as sequential for now
  // otherwise there are cyclic attribute evaluations
  sequential

  "1. We should build MSCRs around related GroupByKey nodes" >> {
    "if two GroupByKey nodes share the same input, they belong to the same Mscr" >> {
      val l1 = load
      val gbk1 = gbk(flatten(l1))
      val gbk2 = gbk(flatten(l1))
      mscrsFor(gbk1, gbk2)
      gbk1 -> mscr ==== gbk2 -> mscr
    }
    "if two GroupByKey nodes don't share the same input, they belong to 2 different Mscrs" >> {
      gbk(flatten(load)) -> mscr !== gbk(flatten(load)) -> mscr
    }
    "two Gbks in the same Mscr cannot be ancestor of each other" >> check { graph: CompNode =>
      mscrsFor(graph) foreach { m =>
        val gbks = m.groupByKeys
        gbks foreach { gbk =>
          gbks.filterNot(_ eq gbk) foreach { other => (other:CompNode) aka show(gbk) must not(beAnAncestorOf(gbk)) }
        }
      }
      ok
    }
    "a Gbk must have the exact same Mscr that references it" >> check { graph: CompNode =>
      mscrsFor(graph) foreach { m =>
        m.groupByKeys foreach { gbk => (gbk -> mscr) aka show(gbk) must be(m) }
      }
      ok
    } lt;
  }
  "2. We need to create, for each GroupByKey, a GbkOutputChannel" >> {
    "There should be 1 GbkOutputChannel for 1 GroupByKey" >> {
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
      "if it has a Materialize successor" >> {
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
        mscrsFor(fl1)
        mscrFor(gbk1).outputChannels aka(pretty(gbk1, mscr)) must_== Set(GbkOutputChannel(gbk1, combiner = Some(cb1), reducer = None))
      }
    }
  }
  "3. We need to create InputChannels for each Mscr" >> {
    "we create MapperInputChannels for ParallelDos which are not reducers of the GroupByKey" >> check { graph: CompNode =>
      mscrsFor(graph)
      // collect the parallel does of the current mscr which are not reducers
      descendents(graph).collect(isAParallelDo).filterNot(p => (p -> mscr).reducers.contains(p)) foreach { p =>
        (p -> mscr).mappers aka show(p) must contain(p)
      }; ok
    }
    "Mappers" >> {
      "two mappers in 2 different input channels must not share the same input" >> check { graph: CompNode =>
        mscrsFor(graph).filter(_.inputChannels.size > 1) foreach { m =>
          val independentPdos = m.mapperChannels.flatMap(_.parDos.headOption).toSeq
          val (pdo1, pdo2) = (independentPdos(0), independentPdos(1))
          pdo1.in !== pdo2.in
        }; ok
      }
      "two mappers in the same input channel must share the same input" >> check { graph: CompNode =>
        mscrsFor(graph).flatMap(_.mapperChannels).filter(_.parDos.size > 1) foreach { input =>
          val (pdo1, pdo2) = (input.parDos(0), input.parDos(1))
          pdo1.in === pdo2.in
        }; ok
      }
      "two parallelDos sharing the same input must be in the same inputChannel" >> check { graph: CompNode =>
        mscrsFor(graph)
        distinctPairs(descendents(graph).collect(isAParallelDo)).foreach  { case (p1, p2) =>
          if (p1.in == p2.in) {
            (p1 -> mscr).inputChannelFor(p1) === (p2 -> mscr).inputChannelFor(p2)
          }
        }; ok
      }
      "if a ParallelDo is an input shared by 2 others ParallelDos, then it must belong to another Mscr" >> check { graph: CompNode =>
        mscrsFor(graph).filter(_.mappers.size > 1) foreach { m =>
          m.mappers foreach { pd =>
            reachableInputs(pd) collect {
              case p @ ParallelDo(_,_,_,_,_) => (p -> mscr) aka show(p) must be_!== (m)
            }
          }
        }; ok
      }
    }
    "a ParallelDo can not be a mapper and a reducer at the same time" >> check { graph: CompNode =>
      mscrsFor(graph)
      descendents(graph).collect(isAParallelDo) foreach { p =>
        ((p -> mscr).mappers intersect (p -> mscr).reducers) aka show(p) must beEmpty
      }; ok
    }
    "all the ParallelDos must be in a mapper or a reducer" >> check { graph: CompNode =>
      mscrsFor(graph)
      descendents(graph).collect(isAParallelDo) foreach { p =>
        (p -> mscr).mappers.contains(p) || (p -> mscr).reducers.contains(p)
      }; ok
    }
  }

  def show(node: CompNode): String = "SHOWING NODE: "+showNode(node, None)+"\n"+pretty(ancestors(node).headOption.getOrElse(node).asInstanceOf[CompNode], mscr)

  def beAnAncestorOf(node: CompNode): Matcher[CompNode] = new Matcher[CompNode] {
    def apply[S <: CompNode](other: Expectable[S]) =
      result(isAncestor(node, other.value),
             other.description+" is an ancestor of "+node+": "+(Seq(node) ++ ancestors(node)).mkString(" -> "),
             other.description+" is not an ancestor of "+node, other)
  }
}

