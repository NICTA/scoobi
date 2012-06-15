package com.nicta.scoobi
package impl
package plan
package smart

import testing.mutable.UnitSpecification
import io.ConstantStringDataSource
import org.kiama.rewriting._
import core.{Emitter, BasicDoFn}
import org.specs2.matcher.DataTables
import org.specs2.ScalaCheck
import org.scalacheck.{Prop, Arbitrary, Gen}
import Rewriter._

class OptimisationsSpec extends UnitSpecification with Optimiser with DataTables with DCompData with ScalaCheck {

  "1. Nodes must be flattened" >> {
    "1.1 A Flatten Node which is an input to 2 other nodes must be copied to each node" >> {
      optimise(flattenSplit, parallelDo(f1), parallelDo(f1)) must beLike {
        case ParallelDo(ff1,_,_,_,_) :: ParallelDo(ff2,_,_,_,_) :: _  => ff1.id !=== ff2.id
      }
      optimise(flattenSplit, parallelDo(f1), parallelDo(f1), parallelDo(f1)) must beLike {
        case ParallelDo(ff1,_,_,_,_) :: ParallelDo(ff2,_,_,_,_)  :: ParallelDo(ff3,_,_,_,_) :: _  => Seq(ff1.id, ff2.id, ff3.id).distinct.size === 3
      }
    }
    "1.2 A Flatten Node which is an input to a ParallelDo then replicate the ParallelDo on each of the Flatten inputs" >> {
      optimise(flattenSink, parallelDo(flattens)) must beLike {
        case List(Flatten(ParallelDo(ll1,_,_,_,_) :: ParallelDo(ll2,_,_,_,_) :: _))  => (l1 === ll1) and (l2 === ll2)
      }
    }
    "1.3 A Flatten Node with Flatten inputs must collect all the inner inputs" >> {
      "input"                                       | "expected"                                    |>
      flatten(l1)                                   ! flatten(l1)                                   |
      flatten(flatten(l1))                          ! flatten(l1)                                   |
      flatten(flatten(flatten(l1)))                 ! flatten(l1)                                   |
      flatten(flatten(l1), l1)                      ! flatten(l1, l1)                               |
      flatten(l1, flatten(l1))                      ! flatten(l1, l1)                               |
      flatten(l1, flatten(l1), l2)                  ! flatten(l1, l1, l2)                           |
      flatten(l1, l2, flatten(flatten(l1)))         ! flatten(l1, l2, l1)                           |
      flatten(l1, l2, flatten(l1, pd(rt), l1), l2)  ! flatten(l1, l2, l1, pd(rt), l1, l2)           | { (input, output) =>
        show(optimise(flattenFuse, input).head) === show(output)
      }

      check(Prop.forAll { (node: AstNode) =>
        collectNestedFlatten(optimise(flattenFuse, node).head) aka show(node) must beEmpty
      })
    }
  }

  "2. Combines must be optimised" >> {
    "A Combine which doesn't have a GroupByKey as an Input must be transformed to a ParallelDo" >> {
      "input"                                        | "expected"                                    |
       cb(l1)                                        ! pd(l1)                                        |
       cb(gbk(l1))                                   ! cb(gbk(l1))                                   |> { (input, output) =>
         show(optimise(combineToParDo, input).head) === show(output)
       }
    }
    "Any optimised Combine in the graph can only have GroupByKey as an input" >> check { (node: AstNode) =>
      forall(collectCombine(optimise(combineToParDo, node).head)) { n =>
        n aka show(node) must beLike { case Combine(GroupByKey(_), _) => ok }
      }
    }
    "After optimisation, all the transformed Combines must be ParallelDo" >> check { (node: AstNode) =>
      val optimised = optimise(combineToParDo, node).head
      (collectCombine(node).size + collectParallelDo(node).size) ===
      (collectCombineGbk(optimised).size + collectParallelDo(optimised).size)
    }
  }

  "3. Successive ParallelDos must be fused" >> check { (node: AstNode) =>
    val optimised = optimise(parDoFuse, node).head
    collectSuccessiveParDos(optimised) must beEmpty
  };p

  "4. GroupByKeys" >> {
    "4.1 the GroupByKey is replicated so that it can not be the input of different nodes  " >> check { (node: AstNode) =>
      val optimised = optimise(groupByKeySplit, node).head

      // collects the gbks, they must form a set and not a bag
      val original = collectGroupByKey(node).map(_.id)
      val gbks = collectGroupByKey(optimised).map(_.id)
      original.size aka show(node) must be_>=(gbks.size)
      gbks.size aka show(optimised) must_== gbks.toSet.size
    }
    "4.2 if the input of a GroupByKey is a Flatten, the Flatten is also replicated" >> check { (node: AstNode) =>
      val optimised = optimise(groupByKeySplit, node).head

      // collects the flattens, they must form a set and not a bag
      val flattens = collectFlatten(optimised).map(_.id)
      flattens.size aka show(optimised) must_== flattens.toSet.size
    }
    "4.3 examples" >> {
      optimise(flattenSplit, parallelDo(f1), parallelDo(f1)) must beLike {
        case ParallelDo(ff1,_,_,_,_) :: ParallelDo(ff2,_,_,_,_) :: _  => ff1.id !=== ff2.id
      }
      optimise(flattenSplit, parallelDo(f1), parallelDo(f1), parallelDo(f1)) must beLike {
        case ParallelDo(ff1,_,_,_,_) :: ParallelDo(ff2,_,_,_,_)  :: ParallelDo(ff3,_,_,_,_) :: _  => Seq(ff1.id, ff2.id, ff3.id).distinct.size === 3
      }
    }
  }

  val (l1, l2) = (load, load)
  val f1       = flatten(l1)
  val flattens = flatten(l1, l2)

  def collectNestedFlatten = collectl {
    case f @ Flatten(ins) if ins exists isFlatten => f
  }

  def collectFlatten          = collectl { case f @ Flatten(_) => f }
  def collectCombine          = collectl { case c @ Combine(_,_) => c }
  def collectCombineGbk       = collectl { case c @ Combine(GroupByKey(_),_) => c }
  def collectParallelDo       = collectl { case p @ ParallelDo(_,_,_,_,_) => p }
  def collectSuccessiveParDos = collectl { case p @ ParallelDo(ParallelDo(_,_,_,_,_),_,_,_,false) => p }
  def collectGroupByKey       = collectl { case g @ GroupByKey(_) => g }
}

trait Optimiser {
  type Term = AstNode

  def flattenSplit = everywhere(rule {
    case f @ Flatten(_) => f.clone
  })

  def flattenSink = everywhere(rule {
    case p @ ParallelDo(Flatten(ins),_,_,_,_) => Flatten(ins.map(in => p.copy(in)))
  })

  val isFlatten: DComp[_,_] => Boolean = { case Flatten(_) => true; case other => false }

  def flattenFuse = repeat(sometd(rule {
    case Flatten(ins) if ins exists isFlatten => Flatten(ins.flatMap { case Flatten(nodes) => nodes; case other => List(other) })
  }))

  def combineToParDo = everywhere(rule {
    case c @ Combine(GroupByKey(_), _) => c
    case c @ Combine(other, f)         => c.toParallelDo
  })

  def parDoFuse = repeat(sometd(rule {
    case p1 @ ParallelDo(p2 @ ParallelDo(_,_,_,_,_),_,_,_,false) => p2 fuse p1
  }))

  def groupByKeySplit = everywhere(fail
  //  case g @ GroupByKey(f @ Flatten(ins)) => g.copy(in = f.copy())
  //  case g @ GroupByKey(_)                => g.clone
  )

  def optimise(strategy: Strategy, nodes: AstNode*): List[AstNode] = {
    rewrite(strategy)(nodes).toList
  }

}

trait DCompData {

  def load = Load(ConstantStringDataSource("start"))
  def flatten[A](nodes: AstNode*) = Flatten(nodes.toList.map(_.asInstanceOf[DComp[A,Arr]]))
  def parallelDo(in: AstNode) = pd(in)
  def rt = Return("")
  def pd(in: AstNode) = ParallelDo[String, String, Unit](in.asInstanceOf[DComp[String,Arr]], Return(()), fn)
  def cb(in: AstNode) = Combine[String, String](in.asInstanceOf[DComp[(String, Iterable[String]),Arr]], (s1: String, s2: String) => s1 + s2)
  def gbk(in: AstNode) = GroupByKey(in.asInstanceOf[DComp[(String,String),Arr]])

  val fn = new BasicDoFn[String, String] { def process(input: String, emitter: Emitter[String]) { emitter.emit(input) } }


  val show = (_:AstNode).toVerboseString.replaceAll("\\d","")

  implicit def arbitraryDComp: Arbitrary[AstNode] = Arbitrary(genDComp)

  def genDComp: Gen[AstNode] = Gen.lzy(Gen.frequency((3, genLoad),
                                                     (1, genParallelDo),
                                                     (1, genFlatten),
                                                     (1, genGroupByKey),
                                                     (1, genCombine)))

  def genLoad       = Gen.oneOf(load, load)
  def genParallelDo = genDComp map parallelDo
  def genFlatten    = Gen.listOfN(3, genDComp).map(l => flatten(l:_*))
  def genCombine    = genDComp map cb
  def genGroupByKey = genDComp map gbk

}
