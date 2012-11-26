package com.nicta.scoobi
package impl
package plan
package mscr

import core._
import comp._

class InputChannelsSpec extends MscrMakerSpecification {

  "We must create InputChannels for each Mscr".txt

  "MapperInputChannels" >> {
    "we need to determine if parallelDos are mappers or reducers" >> {
      "A parallelDo is a reducer if it has a fuse barrier or a group barrier" >> prop { (graph: CompNode, ma: MscrAttributes) => import ma._
        parallelDos(graph) foreach { pd =>
          (pd -> isReducer) must beTrue.when(pd.fuseBarrier || pd.groupBarrier)
        }
      }
      "A parallelDo is a reducer if it has no outputs or a Materialize node in its outputs" >> prop { (graph: CompNode, ma: MscrAttributes) => import ma._
        parallelDos(graph) foreach { pd =>
          (pd -> isReducer) must beTrue.when((pd -> outputs).isEmpty || (pd -> outputs).exists(isMaterialize))
        }
      }
      "A parallelDo is not a reducer if it environment is produced by the same mscr" >> new factory {
        val l1 = load
        val pd1 = pd(l1)
        val gbk1 = gbk(pd1)
        val mat1 = mt(gbk1)
        val pd2 = pd(gbk1, env = mat1)

        (envInSameMscr(pd2)) must beTrue
        (pd2 -> isReducer) must beFalse
      }
      "A parallelDo is a mapper if it's not a reducer" >> prop { (graph: CompNode, ma: MscrAttributes) => import ma._
        parallelDos(graph) foreach { pd =>
          (pd -> isMapper) must beTrue.iff(!(pd -> isReducer))
        }
      }
    }

    "we must create a MapperInputChannel for each 'mapper' ParallelDo connected to a group by key" >> new factory {
      val l1 = load
      val pd1 = pd(l1)
      val gbk1 = gbk(pd1)
      val graph = pd(gbk1)
      (gbk1 -> mapperInputChannels) must not be empty
    }
    "two mappers in 2 different mapper input channels must not share the same input" >> prop { (graph: CompNode, ma: MscrAttributes) => import ma._
      val inputChannels = gbks(graph).flatMap(mapperInputChannels)
      val independentPdos = inputChannels.map(_.parDos.headOption).flatten.toSeq
      (independentPdos.size must be_<(2)) or {
        val (pdo1, pdo2) = (independentPdos(0), independentPdos(1))
        pdo1.in aka show(pdo1) must not beTheSameAs (pdo2.in)
      }
    }
    "two mappers in the same mapper input channel must share the same input" >> prop { (graph: CompNode, ma: MscrAttributes) => import ma._
      gbks(graph).flatMap(mapperInputChannels).collect { case i if i.parDos.size >= 2 => i.parDos } foreach { parDos =>
        val (pdo1, pdo2) = (parDos.toSeq(0), parDos.toSeq(1))
        (pdo1 -> descendents).intersect(pdo2 -> descendents) aka (pdo1, pdo2).toString must not beEmpty
      }
    }
    "two parallelDos sharing the same input must be in the same mapper inputChannel" >>  new factory {
      val l1 = load
      val (pd1, pd2) = (pd(l1), pd(l1))
      val (gbk1, gbk2) = (gbk(pd1), gbk(pd2))
      val graph = pd(flatten(gbk1, gbk2))
      (gbk1 -> mapperInputChannels) must have size(1)
      (gbk1 -> mapperInputChannels).head.parDos must have size(2)
    }
    "two parallelDos sharing the same input must be in the same inputChannel" >> prop { (graph: CompNode, ma: MscrAttributes) => import ma._
      forall((graph -> mscr).mapperChannels.flatMap(_.parDos)) { pd => isMapper(pd) must beTrue }
    }
  }
  "We must create an IdInputChannel for ParallelDo nodes which are not in the set of related parallel dos" >> new factory {
    val l1 = load
    val (pd1, pd2, pd3) = (pd(l1), pd(l1), pd(load))
    val gbk1 = gbk(flatten(pd1, pd2, pd3))
    val graph = flatten(gbk1, pd3)
    (gbk1 -> idInputChannels) must have size(1)
    (gbk1 -> idInputChannels).head.input must_== Some(pd3)

  }

}
