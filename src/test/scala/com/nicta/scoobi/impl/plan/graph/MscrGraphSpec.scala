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
  override def defaultValues = super.defaultValues + (minTestsOk -> arguments.commandLine.int("mintestsok").getOrElse(10))

  // set the specification as sequential for now
  // otherwise there are cyclic attribute evaluations
  "1. We should build MSCRs around related GroupByKey nodes" >> {
    "if two GroupByKey nodes share the same input, they belong to the same Mscr" >> {
      val pd0 = load
      val gbk1 = gbk(pd(pd0))
      val gbk2 = gbk(pd(pd0))
      val graph = flatten(gbk1, gbk2)
      mscrFor(graph)
      (graph -> mscr).groupByKeys ==== Seq(gbk1, gbk2)
      gbk1 -> mscr ==== gbk2 -> mscr
    }
    "if two GroupByKey nodes don't share the same input, they belong to 2 different Mscrs" >> {
      gbk(flatten(load)) -> mscr !== gbk(flatten(load)) -> mscr
    }
    "two successive gbks must be in 2 different mscrs" >> {
      val pd0 = load
      val gbk1 = gbk(pd(pd0))
      val pd1 = pd(gbk1)
      val gbk2 = gbk(pd1)
      val graph = flatten(gbk2)
      mscrFor(graph)
      gbk1 -> mscr !=== gbk2 -> mscr
    }
    "two Gbks in the same Mscr cannot be ancestor of each other" >> check { graph: CompNode =>
      mscrsFor(graph) foreach { m =>
        val gbks = m.groupByKeys
        gbks foreach { gbk =>
          gbks.foreach { other =>
            if (other.id != gbk.id) {
              (other:CompNode) aka show(gbk) must not(beAnAncestorOf(gbk))
            }
          }
        }
      }; ok
    }
    "a Gbk must have the same Mscr that references it" >> check { graph: CompNode =>
      mscrsFor(graph).collect(isAGroupByKey) foreach { gbk =>
        val m = (gbk -> mscr)
        m.groupByKeys.map(_.id) aka show(gbk) must contain(gbk.id)
      }; ok
    } lt;
  }
  "2. We need to create, for each GroupByKey, a GbkOutputChannel" >> {
    "There should be 1 GbkOutputChannel for 1 GroupByKey" >> {
      val gbk1 = gbk(cb(load))
      mscrFor(gbk1).outputChannels must_== Seq(GbkOutputChannel(gbk1))
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
      (gbk1 -> mscr).outputChannels must_== Seq(GbkOutputChannel(gbk1, flatten = Some(fl1)))
    }
    "If the output of a GroupByKey is a Combine node then add it to this channel" >> {
      val gbk1 = gbk(pd(load))
      val cb1  = cb(gbk1)
      mscrFor(cb1).combiners must_== Seq(cb1)
    }
    "If the Combine following a GroupByKey is followed by a ParallelDo, then the ParallelDo can be added as a reducer" >> {
      "if it has a groupBarrier" >> {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val pd1  = pd(cb1, groupBarrier = true)
        mscrFor(pd1).reducers must_== Seq(pd1)
      }
      "if it has a fuseBarrier" >> {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val pd1  = pd(cb1, fuseBarrier = true)
        mscrFor(pd1).reducers must_== Seq(pd1)
      }
      "if it has no successor" >> {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val pd1  = pd(cb1)
        mscrFor(pd1).reducers must_== Seq(pd1)
      }
      "if it has a Materialize successor" >> {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val pd1  = pd(cb1)
        val mat1 = mt(pd1)
        mscrFor(mat1).reducers must_== Seq(pd1)
      }
      "if it has a no gbk ancestor" >> {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val pd1 = pd(cb1, groupBarrier = false, fuseBarrier = false)
        val fl1  = flatten(pd1)
        mscrFor(fl1).reducers must_== Seq(pd1)
      }
      "but it's not added if none of those conditions is true" >> {
        val gbk1 = gbk(rt)
        val cb1  = cb(gbk1)
        val gbk2  = gbk(pd(cb1, groupBarrier = false, fuseBarrier = false))
        val m = mscrFor(gbk2)
        m.reducers aka show(gbk2) must beEmpty
      }
    }
  }

  "3. We need to create InputChannels for each Mscr" >> {
    "we create MapperInputChannels for ParallelDos which are not reducers of the GroupByKey" >> check { graph: CompNode =>
      mscrsFor(graph)
      // collect the parallel does of the current mscr which are not reducers
      descendents(graph).collect(isAParallelDo).filterNot(p => (p -> mscr).reducers.contains(p)) foreach { p =>
        if ((p -> mscr).groupByKeys.nonEmpty && ancestors(p).collect(isAParallelDo).isEmpty) {
          val mappers = (p -> mscr).mappers
          mappers aka show(graph) must contain(p)
        }
      }; ok
    }
    "Mappers" >> {
      "two mappers in 2 different input channels must not share the same input" >> check { graph: CompNode =>
        mscrsFor(graph).filter(_.inputChannels.size > 1) foreach { m =>
          val independentPdos = m.mapperChannels.flatMap(_.parDos.headOption).toSeq
          val (pdo1, pdo2) = (independentPdos(0), independentPdos(1))
          pdo1.in aka show(pdo1) must be_!==(pdo2.in)
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
      "example of parallel dos sharing the same input" >> {
        val pd0 = pd(load)
        val (pd1, pd2) = (pd(pd0), pd(pd0))
        val (gbk1, gbk2) = (gbk(pd1), gbk(pd2))
        val graph = flatten(gbk1, gbk2)
        mscrFor(graph) must_== Mscr(inputChannels = Seq(MapperInputChannel(Seq(pd1, pd2))),
                                    outputChannels = Seq(GbkOutputChannel(gbk1), GbkOutputChannel(gbk2)))
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
        val m = p -> mscr
        if ((p -> descendents).collect(isAGroupByKey).nonEmpty) {
          (p -> ancestors).exists(a => isAParallelDo.isDefinedAt(a)) ||
          m.mappers.contains(p) ||
          m.reducers.contains(p) aka "for "+p+"\n"+pretty(graph, mscr)+"\n"+(m.mappers, m.reducers)+"\n"+pretty(graph, isGbkOutput) must beTrue
        }
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

