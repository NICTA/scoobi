package com.nicta.scoobi
package impl
package plan
package graph

import org.kiama.attribution.Attribution
import org.specs2.mutable.{Tags, Specification}
import org.specs2.ScalaCheck
import org.specs2.specification.AfterExample
import org.specs2.matcher.{Expectable, Matcher}
import comp._
import CompNodePrettyPrinter._
import Optimiser._
import CompNode._
import Mscr._

class MscrGraphSpec extends Specification with CompNodeData with ScalaCheck with MscrGraph with Tags with AfterExample {

  "1. We must build MSCRs around related GroupByKey nodes" >> {
    "if two GroupByKey nodes share the same input, they belong to the same Mscr" >> {
      val load0 = load
      val gbk1 = gbk(pd(load0))
      val gbk2 = gbk(pd(load0))
      val graph = flatten(gbk1, gbk2)
      val mscrs = makeMscrs(graph).filter(isGbkMscr)
      mscrs must have size(1)
      mscrs.toSeq(0).groupByKeys ==== Set(gbk1, gbk2)
    }
    "if two GroupByKey nodes don't share the same input, they belong to 2 different Mscrs" >> {
      val gbk1 = gbk(pd(load))
      val gbk2 = gbk(pd(load))
      val graph = flatten(gbk1, gbk2)

      makeMscrs(graph).filter(isGbkMscr) must have size(2)
    }
    "two successive gbks must be in 2 different mscrs" >> {
      val pd0 = load
      val gbk1 = gbk(pd(pd0))
      val pd1 = pd(gbk1)
      val gbk2 = gbk(pd1)
      val graph = flatten(gbk2)
      val mscrs = makeMscrs(graph)
      mscrs.map(_.groupByKeys).filterNot(_.isEmpty) ==== Set(Set(gbk1), Set(gbk2))
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
  "2. We must create InputChannels for each Mscr" >> {
    "MapperInputChannels" >> {
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
      "two mappers in 2 different mapper input channels must not share the same input" >> check { graph: CompNode =>
        mscrsFor(graph).filter(isGbkMscr).filter(_.mapperChannels.size > 1) foreach { m =>
          val independentPdos = m.mapperChannels.flatMap(_.parDos.headOption).toSeq
          val (pdo1, pdo2) = (independentPdos(0), independentPdos(1))
          pdo1.in aka show(pdo1) must not beTheSameAs (pdo2.in)
        }; ok
      }
      "two mappers in the same mapper input channel must share the same input" >> check { graph: CompNode =>
        mscrsFor(graph).filter(isGbkMscr).flatMap(_.mapperChannels).filter(_.parDos.size > 1) foreach { input =>
          val (pdo1, pdo2) = (input.parDos.toSeq(0), input.parDos.toSeq(1))
          pdo1.in aka show(pdo1) must beTheSameAs(pdo2.in)
        }; ok
      }
      "two parallelDos sharing the same input must be in the same inputChannel" >> check { graph: CompNode =>
        mscrsFor(graph)
        distinctPairs(descendents(graph).collect(isAParallelDo)).foreach  { case (p1, p2) =>
          if (p1.in eq p2.in) {
            (p1 -> mscr).inputChannelFor(p1) aka show(p1) must beTheSameAs((p2 -> mscr).inputChannelFor(p2))
          }
        }; ok
      }
      "if a ParallelDo is an input shared by 2 others ParallelDos, then it must belong to another Mscr" >> check { graph: CompNode =>
        mscrsFor(graph).filter(_.mappers.size > 1) foreach { m =>
          m.mappers foreach { pd =>
            (pd -> descendents) collect {
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
        makeMscrs(graph) must have size(2)
        makeMscrs(graph).filter(isGbkMscr).toSeq(0) ==== Mscr(inputChannels  = Set(MapperInputChannel(Set(pd2, pd1))),
                                                              outputChannels = Set(GbkOutputChannel(gbk2), GbkOutputChannel(gbk1)))
      }
      "a ParallelDo can not be a mapper and a reducer at the same time" >> check { graph: CompNode =>
        mscrsFor(graph)
        descendents(graph).collect(isAParallelDo) foreach { p =>
          ((p -> mscr).mappers intersect (p -> mscr).reducers) aka show(p) must beEmpty
        }; ok
      }
      "all the ParallelDos must be in a mapper or a reducer" >> check { graph: CompNode =>
        descendents(graph).collect(isAParallelDo) foreach { p =>
          val m = p -> mscr
          if ((p -> descendents).collect(isAGroupByKey).nonEmpty) {
            (p -> ancestors).exists(a => isAParallelDo.isDefinedAt(a)) ||
              m.mappers.contains(p) ||
              m.reducers.contains(p) aka "for "+p+"\n"+pretty(graph, mscr)+"\n"+(m.mappers, m.reducers) must beTrue
          }
        }; ok
      }
    }
    "IdInputChannels" >> {
      "we create an IdInputChannel for each GroupByKey input which has no siblings" >> check { graph: CompNode =>
        mscrsFor(graph).filter(_.idChannels.size > 1) foreach { m =>
          m.idChannels foreach { channel =>
            val input = channel.input
            (input -> siblings) aka show(input) must beEmpty
            m.mappers.toSeq.asNodes aka mscrsGraph(graph) must not contain(input)
          }
        }; ok
      }
    }

  }
  "3. We must create OutputChannels for each Mscr" >> {
    "GbkOutputChannels" >> {
      "There should be 1 GbkOutputChannel for 1 GroupByKey" >> {
        val gbk1 = gbk(cb(load))
        mscrFor(gbk1).outputChannels must_== Set(GbkOutputChannel(gbk1))
      }
      "There should be one GbkOutputChannel for each GroupByKey in the Mscr" >> {
        val l1 = load
        val gbk1 = gbk(cb(l1))
        val gbk2 = gbk(cb(l1))
        mscrsFor(gbk1, gbk2).flatMap(_.outputChannels) must_== Set(GbkOutputChannel(gbk1), GbkOutputChannel(gbk2))
      }
      "If the input of a GroupByKey is a Flatten node then add it to this channel" >> {
        val fl1  = flatten(load)
        val gbk1 = gbk(fl1)
        makeMscr(gbk1).outputChannels === Set(GbkOutputChannel(gbk1, flatten = Some(fl1)))
      }
      "If the output of a GroupByKey is a Combine node then add it to this channel" >> {
        val gbk1 = gbk(pd(load))
        val cb1  = cb(gbk1)
        mscrFor(cb1).combiners must_== Set(cb1)
      }
      "If the Combine following a GroupByKey is followed by a ParallelDo, then the ParallelDo can be added as a reducer" >> {
        "if it has a groupBarrier" >> {
          val gbk1 = gbk(rt)
          val cb1  = cb(gbk1)
          val pd1  = pd(cb1, groupBarrier = true)
          mscrFor(pd1).reducers must_== Set(pd1)
        }
        "if it has a fuseBarrier" >> {
          val gbk1 = gbk(rt)
          val cb1  = cb(gbk1)
          val pd1  = pd(cb1, fuseBarrier = true)
          mscrFor(pd1).reducers must_== Set(pd1)
        }
        "if it has no successor" >> {
          val gbk1 = gbk(rt)
          val cb1  = cb(gbk1)
          val pd1  = pd(cb1)
          mscrFor(pd1).reducers must_== Set(pd1)
        }
        "if it has a Materialize successor" >> {
          val gbk1 = gbk(rt)
          val cb1  = cb(gbk1)
          val pd1  = pd(cb1)
          val mat1 = mt(pd1)
          mscrFor(mat1).reducers aka mscrsGraph(mat1) must_== Set(pd1)
        }
        "if it has a no ancestors" >> {
          val gbk1 = gbk(rt)
          val cb1  = cb(gbk1)
          val pd1 = pd(cb1, groupBarrier = false, fuseBarrier = false)
          mscrFor(pd1).reducers must_== Set(pd1)
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
    "BypassOutputChannels" >> {
      "There must be a BypassOutputChannel for each ParallelDo input having outputs which are not gbks" >> {
        val l1 = load
        val pd1 = pd(l1)
        val gbk1 = gbk(pd1)
        val fl1 = flatten(gbk1, pd1)
        makeMscrs(fl1).flatMap(_.bypassChannels) aka mscrsGraph(fl1) must_== Set(BypassOutputChannel(pd1))
      }
    }
  }
  "4. We must build MSCRs for 'floating' nodes, i.e. not related to Gbk mscrs" >> {
    "ParallelDo mscrs" >> {
      "There should be 1 Mscr per floating ParallelDo with a MapperInputChannel and a BypassOutputChannel" >> {
        val l1 = load
        val pd1 = pd(l1)
        val pd2 = pd(l1)
        val op1 = op(pd1, pd2)
        makeMscrs(op1).filter(isParallelDoMscr).toSeq(0) ==== Mscr(inputChannels = Set(MapperInputChannel(Set(pd1, pd2))),
                                                                   outputChannels = Set(BypassOutputChannel(pd1), BypassOutputChannel(pd2)))
      }
    }
    "Flatten mscrs" >> {
      "There should be 1 Mscr per floating Flatten with one input channel for each input of the Flatten and a FlattenOutputChannel" >> {
        val l1 = load
        val l2 = load
        val pd1 = pd(l1)
        val pd2 = pd(l1)
        val fl1 = flatten(pd1, pd2, l2)
        makeMscrs(fl1).filter(isFlattenMscr).toSeq(0) ==== Mscr(inputChannels = Set(StraightInputChannel(l2),
                                                                                    MapperInputChannel(Set(pd1)),
                                                                                    MapperInputChannel(Set(pd2))),
                                                                outputChannels = Set(FlattenOutputChannel(fl1)))
      }
    }
  }

  "Support functions" >> {
    "the inputs of a node are its children" >> {
      val load0 = load
      val cb1 = cb(load0)
      Attribution.initTree(cb1)

      (cb1 -> inputs) ==== Set(load0)
    }
    val l1 = load
    val pd1 = pd(l1)
    val gbk1 = gbk(pd1)
    val fl1 = flatten(gbk1, pd1)
    val mat1 = mt(fl1)
    Attribution.initTree(mat1)

    "the ancestors of a node are all its direct parents" >> {
      (l1 -> ancestors) ==== Set(pd1, fl1, mat1)
    }
    "the descendents of a node are the recursive list of all children" >> {
      (mat1 -> descendents) ==== Set(l1, pd1.env, pd1, gbk1, fl1)
    }
    "a node can be reached from another one if it is in the list of its descendents" >> {
      (fl1 -> canReach(l1)) must beTrue
    }
    "the parents of a node are all the nodes having this node in their descendents" >> {
      (l1 -> parents) ==== Set(pd1, fl1, gbk1, mat1)
    }
    "the outputs of a node are all its direct parents" >> {
      (pd1 -> outputs) ==== Set(fl1, gbk1)
    }
    "2 nodes are siblings if they share the same input" >> {
      val load0 = load
      val pd1 = pd(load0)
      val pd2 = pd(load0)
      val graph = flatten(pd1, pd2)
      Attribution.initTree(graph)

      (pd1 -> siblings) ==== Set(pd2)
    }
    "2 gbks are related if" >> {
      "they share the same ParallelDo input" >> {
        val pd1 = pd(load)
        val (gbk1, gbk2) = (gbk(pd1), gbk(pd1))
        val fl1 = flatten(gbk1, gbk2)
        Attribution.initTree(fl1)
        (gbk1 -> relatedGbks) ==== Set(gbk2)
      }
      "they have ParallelDos inputs which are siblings" >> {
        val ld1 = load
        val (pd1, pd2) = (pd(ld1), pd(ld1))
        val (gbk1, gbk2) = (gbk(pd1), gbk(pd2))
        val fl1 = flatten(gbk1, gbk2)
        Attribution.initTree(fl1)
        (gbk1 -> relatedGbks) ==== Set(gbk2)
      }
      "they have Flatten nodes inputs with ParallelDos inputs which are siblings" >> {
        val ld1 = load
        val (pd1, pd2) = (pd(ld1), pd(ld1))
        val (gbk1, gbk2) = (gbk(flatten(pd1)), gbk(flatten(pd2)))
        val fl1 = flatten(gbk1, gbk2)
        Attribution.initTree(fl1)
        (gbk1 -> relatedGbks) ==== Set(gbk2)
      }
      "they have Flatten nodes inputs with ParallelDos inputs which are siblings - mixed version" >> {
        val ld1 = load
        val (pd1, pd2) = (pd(ld1), pd(ld1))
        val (gbk1, gbk2) = (gbk(flatten(pd1)), gbk(pd2))
        val fl1 = flatten(gbk1, gbk2)
        Attribution.initTree(fl1)
        (gbk1 -> relatedGbks) ==== Set(gbk2)
      }
    }
  }

  def show(node: CompNode): String = "SHOWING NODE: "+showNode(node, None)+"\n"+mscrsGraph(ancestors(node).headOption.getOrElse(node).asInstanceOf[CompNode])

  def beAnAncestorOf(node: CompNode): Matcher[CompNode] = new Matcher[CompNode] {
    def apply[S <: CompNode](other: Expectable[S]) =
      result(isAncestor(node, other.value),
             other.description+" is an ancestor of "+node+": "+(Seq(node) ++ ancestors(node)).mkString(" -> "),
             other.description+" is not an ancestor of "+node, other)
  }

  // we set this specification as isolated to avoid interferences with the memoization of attributes on shared nodes
  isolated
  def after { Attribution.resetMemo }
}
