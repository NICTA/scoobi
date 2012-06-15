package com.nicta.scoobi
package impl
package plan
package smart

import testing.mutable.UnitSpecification
import io.ConstantStringDataSource
import org.kiama.rewriting._
import core.{Emitter, BasicDoFn}
import org.specs2.matcher.{Matcher, DataTables}
import org.specs2.ScalaCheck
import org.scalacheck.{Prop, Arbitrary, Gen}
import org.kiama.attribution.Attributable

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

  "2. A Combiner which doesn't have a GroupByKey as an Input must be transformed to a ParallelDo" >> {
    pending
  }

  val (l1, l2) = (load, load)
  val f1       = flatten(l1)
  val flattens = flatten(l1, l2)
}

import Rewriter._
trait Optimiser {
  type Term = AstNode

  def flattenSplit = everywhere(rule {
    case f @ Flatten(_) => f.clone
  })

  def flattenSink = everywhere(rule {
    case p @ ParallelDo(Flatten(ins),_,_,_,_) => Flatten(ins.map(in => p.copy(in)))
  })

  val isFlatten: DComp[_,_] => Boolean = { case Flatten(_) => true; case other => false }

  def collectNestedFlatten = collectl {
    case f @ Flatten(ins) if ins exists isFlatten => f
  }

  def flattenFuse = repeat(sometd(rule {
    case Flatten(ins) if ins exists isFlatten => Flatten(ins.flatMap { case Flatten(nodes) => nodes; case other => List(other) })
  }))

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
  val fn = new BasicDoFn[String, String] { def process(input: String, emitter: Emitter[String]) { emitter.emit(input) } }


  val show = (_:AstNode).toVerboseString.replaceAll("\\d","")

  implicit def arbitraryDComp: Arbitrary[AstNode] = Arbitrary(genDComp)

  def genDComp: Gen[AstNode] = Gen.lzy(Gen.frequency((10, genLoad), (1, genParallelDo), (1, genFlatten)))

  def genLoad       = Gen.oneOf(load, load)
  def genParallelDo = genDComp map parallelDo
  def genFlatten    = Gen.listOfN(3, genDComp).map(l => flatten(l:_*))

}
