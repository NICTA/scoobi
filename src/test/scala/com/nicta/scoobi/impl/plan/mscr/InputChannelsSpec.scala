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

    "we must create a MapperInputChannel for each 'mapper' ParallelDo" >> prop { (graph: CompNode, ma: MscrAttributes) => import ma._
      parallelDos(graph) foreach { pd =>
        (pd -> mapperInputChannels).size aka show(graph) must be_==(1).when(pd -> isMapper)
        (pd -> mapperInputChannels).flatMap(_.parDos).contains(pd)
      }
    }
    "two mappers in 2 different mapper input channels must not share the same input" >> prop { (graph: CompNode, ma: MscrAttributes) => import ma._
      val inputChannels = parallelDos(graph).flatMap(mapperInputChannels)
      val independentPdos = inputChannels.map(_.parDos.headOption).flatten.toSeq
      (independentPdos.size must be_<(2)) or {
        val (pdo1, pdo2) = (independentPdos(0), independentPdos(1))
        pdo1.in aka show(pdo1) must not beTheSameAs (pdo2.in)
      }
    }
    "two mappers in the same mapper input channel must share the same input" >> prop { (graph: CompNode, ma: MscrAttributes) => import ma._
      parallelDos(graph).flatMap(mapperInputChannels).collect { case i if i.parDos.size >= 2 => i.parDos } foreach { parDos =>
        val (pdo1, pdo2) = (parDos.toSeq(0), parDos.toSeq(1))
        (pdo1 -> descendents).intersect(pdo2 -> descendents) aka (pdo1, pdo2).toString must not beEmpty
      }
    }
    "two parallelDos sharing the same input must be in the same inputChannel" >> prop { (graph: CompNode, ma: MscrAttributes) => import ma._
      distinctPairs(descendents(graph).collect(isAParallelDo)).foreach  { case (pd1, pd2) =>
        if ((pd1.in eq pd2.in) && (pd1 -> isMapper)) {
            (pd1 -> mapperInputChannels).flatMap(_.parDos) must contain(pd2)
        }
      }
    }
    "two parallelDos sharing the same input must be in the same inputChannel" >> prop { (graph: CompNode, ma: MscrAttributes) => import ma._
      forall((graph -> mscr).mapperChannels.flatMap(_.parDos)) { pd => isMapper(pd) must beTrue }
    }
  }
  "IdInputChannels" >> {
    "we create an IdInputChannel for each GroupByKey input which has no siblings" >> prop { (graph: CompNode, ma: MscrAttributes) => import ma._
      (graph -> descendents).collect { case d if (d -> idInputChannels(gbk(load))).size > 1 => d -> idInputChannels(gbk(load)) }.flatten foreach { channel =>
        val input = channel.input
        (input -> siblings) aka show(input) must beEmpty
      }
    }
  }
  "MapperInputChannels + IdInputChannels" >> {
    "The input channels of a node are all the mapper input channels for that node + the id input channels" >> new factory {
      val (l1, l2, rt1, rt2) = (load, load, rt, rt)
      val (pd1, pd2, pd3) = (pd(l1, rt1), pd(l1, rt1), pd(l2, rt2))
      val (gbk1, gbk2, gbk3) = (gbk(pd1), gbk(pd2), gbk(rt1))
      val graph = flatten(gbk1, gbk2, gbk3)

      (graph -> mapperInputChannels)   must have size(1)
      (graph -> idInputChannels(gbk3)) must have size(1)
      (graph -> inputChannels(gbk3))   must have size(2)
    }
  }

}
