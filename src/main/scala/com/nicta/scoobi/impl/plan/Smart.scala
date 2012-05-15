/**
  * Copyright 2011 National ICT Australia Limited
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
package com.nicta.scoobi.impl.plan

import scala.collection.mutable.{Map => MMap}

import com.nicta.scoobi.DoFn
import com.nicta.scoobi.Emitter
import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.Grouping
import com.nicta.scoobi.io.DataSource
import com.nicta.scoobi.io.DataSink
import com.nicta.scoobi.impl.exec.BridgeStore
import com.nicta.scoobi.impl.exec.TaggedIdentityMapper
import com.nicta.scoobi.impl.util.UniqueInt


/** Abstract syntax of tree of primitive "language". */
object Smart {

  object Id extends UniqueInt

  type CopyFn[T] = (DList[T], CopyTable) => (DList[T], CopyTable, Boolean)
  type CopyTable = Map[DList[_], DList[_]]


  /** GADT for distributed list computation graph. */
  sealed abstract class DList[A : Manifest : WireFormat] {

    /* We don't want structural equality */
    override def equals(arg0: Any): Boolean = eq(arg0.asInstanceOf[AnyRef])

    val id = Id.get

    def toVerboseString: String


    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //  Optimisation strategies:
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    /** An optimisation strategy that any ParallelDo that is connected to an output is marked
      * with a fuse barrier. */
    def optAddFuseBar(copied: CopyTable, outputs: Set[DList[_]]): (DList[A], CopyTable, Boolean) =
      copyOnceWith(copied, _.optAddFuseBar(_, outputs))

    /** An optimisation strategy that replicates any Flatten nodes that have multiple outputs
      * such that in the resulting AST Flatten nodes only have single outputs. */
    def optSplitFlattens(copied: CopyTable): (DList[A], CopyTable, Boolean) =
      copyOnceWith(copied, _.optSplitFlattens(_))

    /** An optimisation strategy that sinks a Flatten node that is an input to a ParallelDo node
      * such that in the resulting AST replicated ParallelDo nodes are inputs to the Flatten
      * node. */
    def optSinkFlattens(copied: CopyTable): (DList[A], CopyTable, Boolean) =
      copyOnceWith(copied, _.optSinkFlattens(_))

    /** An optimisation strategy that fuse any Flatten nodes that are inputs to Flatten nodes. */
    def optFuseFlattens(copied: CopyTable): (DList[A], CopyTable, Boolean) =
      copyOnceWith(copied, _.optFuseFlattens(_))

    /** An optimisation strategy that morphs a Combine node into a ParallelDo node if it does not
      * follow a GroupByKey node. */
    def optCombinerToParDos(copied: CopyTable): (DList[A], CopyTable, Boolean) =
      copyOnceWith(copied, _.optCombinerToParDos(_))

    /** An optimisation strategy that fuses the functionality of a ParallelDo node that is an input
      * to another ParallelDo node. */
    def optFuseParDos(copied: CopyTable): (DList[A], CopyTable, Boolean) =
      copyOnceWith(copied, _.optFuseParDos(_))

    /** An optimisation strategy that replicates any GroupByKey nodes that have multiple outputs
      * such that in the resulting AST, GroupByKey nodes have only single outputs. */
    def optSplitGbks(copied: CopyTable): (DList[A], CopyTable, Boolean) =
      copyOnceWith(copied, _.optSplitGbks(_))

    /** An optimisation strategy that replicates any Combine nodes that have multiple outputs
      * such that in the resulting AST, Combine nodes have only single outputs. */
    def optSplitCombines(copied: CopyTable): (DList[A], CopyTable, Boolean) =
      copyOnceWith(copied, _.optSplitCombines(_))


    /** Perform a depth-first traversal copy of the DList node. When copying the input DList
      * node(s), a the CopyFn function is used (somewhat like a callback). */
    protected def justCopy(copied: CopyTable, cf: CopyFn[_]): (DList[A], CopyTable, Boolean)

    /** A helper method that checks whether the node has already been copied, and if so returns the copy, else
      * invokes a user provided code implementing the copy. */
    protected def copyOnce(copied: CopyTable)(newCopy: => (DList[A], CopyTable, Boolean)): (DList[A], CopyTable, Boolean) =
      copied.get(this) match {
        case Some(copy) => (copy.asInstanceOf[DList[A]], copied, false)
        case None       => newCopy
      }

    /* Helper for performing optimisation with depth-first traversal once-off copying. */
    private def copyOnceWith(copied: CopyTable, cf: CopyFn[A]): (DList[A], CopyTable, Boolean) =
      copyOnce(copied) { justCopy(copied, cf) }


    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //  Conversion:
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    final def insert(ci: ConvertInfo, n: AST.Node[A]): AST.Node[A] = {
      ci.astMap += ((this, n))
      n
    }

    final def insert2[K : Manifest : WireFormat : Grouping,
                V : Manifest : WireFormat]
                (ci: ConvertInfo, n: AST.Node[(K,V)] with KVLike[K,V]):
                AST.Node[(K,V)] with KVLike[K,V] = {
      ci.astMap += ((this,n))
      n
    }

    def dataSource(ci: ConvertInfo): DataSource[_,_,_] = ci.getBridgeStore(this)

    final def convert(ci: ConvertInfo): AST.Node[A]  = {
      val maybeN: Option[AST.Node[_]] = ci.astMap.get(this)
      maybeN match {
        case Some(n) => n.asInstanceOf[AST.Node[A]] // Run-time cast. Shouldn't fail though.
        case None    => convertNew(ci)
      }
    }

    final def convert2[K : Manifest : WireFormat : Grouping,
                 V : Manifest : WireFormat]
                 (ci: ConvertInfo): AST.Node[(K,V)] with KVLike[K,V]  = {
      val maybeN: Option[AST.Node[_]] = ci.astMap.get(this)
      maybeN match {
        case Some(n) => n.asInstanceOf[AST.Node[(K,V)] with KVLike[K,V]] // Run-time cast. Shouldn't fail though.
        case None    => convertNew2(ci)
      }
    }


    def convertNew(ci: ConvertInfo): AST.Node[A]

    def convertNew2[K : Manifest : WireFormat : Grouping,
                    V : Manifest : WireFormat]
                    (ci: ConvertInfo): AST.Node[(K,V)] with KVLike[K,V]

    def convertParallelDo[B : Manifest : WireFormat](ci: ConvertInfo, pd: ParallelDo[A,B]): AST.Node[B] = {
      val n: AST.Node[A] = convert(ci)
      pd.insert(ci, AST.Mapper(n, pd.dofn))
    }
  }


  /** The Load node type specifies the creation of a DList from some source other than another DList.
    * A DataSource object specifies how the loading is performed. */
  case class Load[A : Manifest : WireFormat](source: DataSource[_, _, A]) extends DList[A] {

    override val toString = "Load" + id

    val toVerboseString = toString

    // Optimisation
    // ~~~~~~~~~~~~
    def justCopy(copied: CopyTable, cf: CopyFn[_]): (DList[A], CopyTable, Boolean) =
      (this, copied, false)


    // Conversion
    // ~~~~~~~~~~
    def convertNew(ci: ConvertInfo) = {
      insert(ci, AST.Load())
    }

    def convertNew2[K : Manifest : WireFormat : Grouping,
                    V : Manifest : WireFormat]
                    (ci: ConvertInfo): AST.Node[(K,V)] with KVLike[K,V] = {

      insert2(ci, new AST.Load[(K,V)]() with KVLike[K,V] {
                    def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K,V](tags)})
    }

    override def dataSource(ci: ConvertInfo): DataSource[_,_,_] = source
  }


  /** The ParallelDo node type specifies the building of a DList as a result of applying a function to
    * all elements of an existing DList and concatenating the results. */
  case class ParallelDo[A, B]
      (in: DList[A],
       dofn: DoFn[A, B],
       groupBarrier: Boolean = false,
       fuseBarrier: Boolean = false)
      (implicit val mA: Manifest[A], val wtA: WireFormat[A], mB: Manifest[B], wtB: WireFormat[B])
    extends DList[B] {

    override val toString = "ParallelDo" + id + (if (groupBarrier) "*" else "") + (if (fuseBarrier) "%" else "")

    val toVerboseString = toString + "(" + in.toVerboseString + ")"

    // Optimisation
    // ~~~~~~~~~~~~
    def justCopy(copied: CopyTable, cf: CopyFn[_])  = justCopy(copied, cf, fuseBarrier)

    def justCopy(copied: CopyTable, cf: CopyFn[_], fb: Boolean): (DList[B], CopyTable, Boolean) = {
      val cfA = cf.asInstanceOf[CopyFn[A]]
      val (inUpd, copiedUpd, b) = cfA(in, copied)
      val pd = ParallelDo(inUpd, dofn, groupBarrier, fb)
      (pd, copiedUpd + (this -> pd), b)
    }

    /** If this ParallelDo is connected to an output, replicate it with a fuse barrier. */
    override def optAddFuseBar(copied: CopyTable, outputs: Set[DList[_]]): (DList[B], CopyTable, Boolean) = copyOnce(copied) {
      val requireFuseBarrier = outputs.contains(this)
      justCopy(copied, (n: DList[A], ct: CopyTable) => n.optAddFuseBar(ct, outputs), requireFuseBarrier)
    }

    /** If the input to this ParallelDo is a Flatten node, re-write the tree as a Flatten node with the
      * ParallelDo replicated on each of its inputs. Otherwise just perform a normal copy. */
    override def optSinkFlattens(copied: CopyTable): (DList[B], CopyTable, Boolean) = copyOnce(copied) {
      in match {
        case Flatten(ins) => {
          val (copiedUpd, insUpd) = ins.foldLeft((copied, Nil: List[DList[A]])) { case ((ct, copies), in) =>
            val (inUpd, ctUpd, _) = in.optSinkFlattens(ct)
            (ctUpd + (in -> inUpd), copies :+ inUpd)
          }

          val flat: DList[B] = Flatten(insUpd.map(ParallelDo(_, dofn, groupBarrier, fuseBarrier)))
          (flat, copiedUpd + (this -> flat), true)
        }
        case _            => justCopy(copied, (n: DList[A], ct: CopyTable) => n.optSinkFlattens(ct))
      }
    }

    /** If the input to this ParallelDo is another ParallelDo node, re-write this ParallelDo with the preceeding's
      * ParallelDo's "mapping function" fused in. Otherwise just perform a normal copy. */
    override def optFuseParDos(copied: CopyTable): (DList[B], CopyTable, Boolean) = copyOnce(copied) {

      /* Create a new ParallelDo function that is the fusion of two connected ParallelDo functions. */
      def fuse[X, Y, Z](f: DoFn[X, Y], g: DoFn[Y, Z]): DoFn[X, Z] = new DoFn[X, Z] {
        def setup() = {
          f.setup()
          g.setup()
        }

        def process(input: X, emitter: Emitter[Z]) = {
          f.process(input, new Emitter[Y] { def emit(value: Y) = g.process(value, emitter) } )
        }

        def cleanup(emitter: Emitter[Z]) = {
          f.cleanup(new Emitter[Y] { def emit(value: Y) = g.process(value, emitter) } )
          g.cleanup(emitter)
        }
      }

      in match {
        case ParallelDo(_, _, _, false) => {
          val (inUpd, copiedUpd, _) = in.optFuseParDos(copied)
          val prev@ParallelDo(inPrev, dofnPrev, gbPrev, _) = inUpd
          val pd = new ParallelDo(inPrev, fuse(dofnPrev, dofn), groupBarrier || gbPrev, fuseBarrier)(prev.mA, prev.wtA, mB, wtB)
          (pd, copiedUpd + (this -> pd), true)
        }
        case _                => justCopy(copied, (n: DList[A], ct: CopyTable) => n.optFuseParDos(ct))
      }
    }


    // Conversion
    // ~~~~~~~~~~
    def convertNew(ci: ConvertInfo): AST.Node[B] = {
      in.convert(ci)
      in.convertParallelDo(ci, this)
    }

    def convertNew2[K : Manifest : WireFormat : Grouping,
                    V : Manifest : WireFormat](ci: ConvertInfo):
                    AST.Node[(K,V)] with KVLike[K,V] = {

      if (ci.mscrs.exists(_.containsGbkReducer(this))) {
        val pd: ParallelDo[(K, Iterable[V]),(K,V)] = this.asInstanceOf[ParallelDo[(K, Iterable[V]),(K,V)]]
        val n: AST.Node[(K, Iterable[V])] = pd.in.convert(ci)

        pd.insert2(ci, new AST.GbkReducer(n, pd.dofn) with KVLike[K,V] {
          def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K,V](tags)})

      } else {
        val pd: ParallelDo[A,(K,V)] = this.asInstanceOf[ParallelDo[A,(K,V)]]
        val n: AST.Node[A] = pd.in.convert(ci)

        if ( ci.mscrs.exists(_.containsGbkMapper(pd)) ) {
          pd.insert2(ci, new AST.GbkMapper(n, pd.dofn) with KVLike[K,V] {
            def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K,V](tags)})
        } else {
          pd.insert2(ci, new AST.Mapper(n, pd.dofn) with KVLike[K,V] {
            def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K,V](tags)})
        }
      }
    }
  }


  /** The GroupByKey node type specifies the building of a DList as a result of partitioning an exiting
    * key-value DList by key. */
  case class GroupByKey[K : Manifest : WireFormat : Grouping,
                        V : Manifest : WireFormat]
      (in: DList[(K, V)])
    extends DList[(K, Iterable[V])] {

    override val toString = "GroupByKey" + id

    val toVerboseString = toString + "(" + in.toVerboseString + ")"

    // Optimisation
    // ~~~~~~~~~~~~
    def justCopy(copied: CopyTable, cf: CopyFn[_]): (DList[(K, Iterable[V])], CopyTable, Boolean) = {
      val cfKV = cf.asInstanceOf[CopyFn[(K, V)]]
      val (inUpd, copiedUpd, b) = cfKV(in, copied)
      val gbk = GroupByKey(inUpd)
      (gbk, copiedUpd + (this -> gbk), b)
    }

    /** Perform a normal copy of this Flatten node but do not mark it as copied in the CopyTable. This will
      * mean subsequent encounters of this node (that is, other outputs of this GroupByKey node) will result
      * in another copy, thereby replicating GroupByKeys with multiple outputs. If this GroupByKey node has a
      * Flatten as its input, replicate the Flatten node as well using the same technique. */
    override def optSplitGbks(copied: CopyTable): (DList[(K, Iterable[V])], CopyTable, Boolean) = copyOnce(copied) {
      in match {
        case Flatten(ins) => {
          val (insUpd, copiedUpd, b) = ins.foldLeft((Nil: List[DList[(K, V)]], copied, false)) { case ((cps, ct, b), n) =>
            val (nUpd, ctUpd, bb) = n.optSplitGbks(ct)
            (cps :+ nUpd, ctUpd + (n -> nUpd), bb || b)
          }
          val flat = Flatten(insUpd)
          val gbk = GroupByKey(flat)
          (gbk, copiedUpd, b)
        }
        case _            => {
          val (inUpd, copiedUpd, b) = in.optSplitGbks(copied)
          val gbk = GroupByKey(inUpd)
          (gbk, copiedUpd, b)
        }
      }
    }


    // Conversion
    // ~~~~~~~~~~
    def convertNew(ci: ConvertInfo) = {
      insert(ci, AST.GroupByKey(in.convert2(ci)))
    }

    def convertAux[A: Manifest : WireFormat : Grouping,
              B: Manifest : WireFormat]
              (ci: ConvertInfo, d: DList[(A,B)]):
              AST.Node[(A, Iterable[B])] with KVLike[A, Iterable[B]] = {
         insert2(ci, new AST.GroupByKey[A,B](d.convert2(ci)) with KVLike[A, Iterable[B]] {
           def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[A,Iterable[B]](tags)})
      }

    def convertNew2[K1 : Manifest : WireFormat : Grouping,
                    V1 : Manifest : WireFormat]
                    (ci: ConvertInfo): AST.Node[(K1,V1)] with KVLike[K1,V1] = {
      convertAux(ci, in).asInstanceOf[AST.Node[(K1,V1)] with KVLike[K1,V1]]


    }

    override def convertParallelDo[B : Manifest : WireFormat]
                               (ci: ConvertInfo, pd: ParallelDo[(K,Iterable[V]), B]): AST.Node[B] = {
      val n: AST.Node[(K, Iterable[V])] = convert(ci)
      if ( ci.mscrs.exists(_.containsGbkReducer(pd)) ) {
        pd.insert(ci, AST.GbkReducer(n, pd.dofn))
      } else {
        pd.insert(ci, AST.Mapper(n, pd.dofn))
      }
    }
  }


  /** The Combine node type specifies the building of a DList as a result of applying an associative
    * function to the values of an existing key-values DList. */
  case class Combine[K : Manifest : WireFormat : Grouping,
                     V : Manifest : WireFormat]
      (in: DList[(K, Iterable[V])],
       f: (V, V) => V)
    extends DList[(K, V)] {

    override val toString = "Combine" + id

    val toVerboseString = toString + "(" + in.toVerboseString + ")"

    // Optimisation
    // ~~~~~~~~~~~~
    def justCopy(copied: CopyTable, cf: CopyFn[_]): (DList[(K, V)], CopyTable, Boolean) = {
      val cfKIV = cf.asInstanceOf[CopyFn[(K, Iterable[V])]]
      val (inUpd, copiedUpd, b) = cfKIV(in, copied)
      val comb = Combine(inUpd, f)
      (comb, copiedUpd + (this -> comb), b)
    }

    /** If this Combine node's input is a GroupByKey node, perform a normal copy. Otherwise, create a
      * ParallelDo node whose mapping function performs the combining functionality. */
    override def optCombinerToParDos(copied: CopyTable): (DList[(K, V)], CopyTable, Boolean) = copyOnce(copied) {
      in match {
        case GroupByKey(inKV) => justCopy(copied, (n: DList[(K, V)], ct: CopyTable) => n.optCombinerToParDos(ct))
        case _                => {
          val (inUpd, copiedUpd, _) = in.optCombinerToParDos(copied)

          val dofn = new DoFn[(K, Iterable[V]), (K, V)] {
            def setup() = {}
            def process(input: (K, Iterable[V]), emitter: Emitter[(K, V)]) = {
              val key = input._1
              val values = input._2
              emitter.emit(key, values.tail.foldLeft(values.head)(f))
            }
            def cleanup(emitter: Emitter[(K, V)]) = {}
          }
          val pd = ParallelDo(inUpd, dofn, false, false)
          (pd, copiedUpd + (this -> pd), true)
        }
      }
    }

    /** Perform a normal copy of this Combine node, along with subsequent GroupByKey and Flatten nodes, but
      * do not mark it as copied in the CopyTable. This will mean subsequent encounters of this node (that
      * is, other outputs of this Combine node) will result in another copy, thereby replicating Combine
      * nodes with multiple outputs. */
    override def optSplitCombines(copied: CopyTable): (DList[(K, V)], CopyTable, Boolean) = copyOnce(copied) {
      in match {
        case GroupByKey(groupIn) => groupIn match {

          case Flatten(ins) => {
            val (insUpd, copiedUpd, b) = ins.foldLeft((Nil: List[DList[(K, V)]], copied, false)) { case ((cps, ct, b), n) =>
              val (nUpd, ctUpd, bb) = n.optSplitCombines(ct)
              (cps :+ nUpd, ctUpd + (n -> nUpd), bb || b)
            }
            val flat = Flatten(insUpd)
            val gbk = GroupByKey(flat)
            val cv = Combine(gbk, f)
            (cv, copiedUpd, b)
          }

          case _            => {
            val (inUpd, copiedUpd, b) = groupIn.optSplitCombines(copied)
            val gbk = GroupByKey(inUpd)
            val cv = Combine(gbk, f)
            (cv, copiedUpd, b)
          }
        }

        case _ => sys.error("Expecting Combine input to be a GroupByKey")
      }
    }



    // Conversion
    // ~~~~~~~~~~
    def convertNew(ci: ConvertInfo) = insert(ci, AST.Combiner(in.convert(ci), f))

    /* An almost exact copy of convertNew */
    def convertNew2[K1 : Manifest : WireFormat : Grouping,
                    V1 : Manifest : WireFormat]
                    (ci: ConvertInfo): AST.Node[(K1,V1)] with KVLike[K1,V1] = {
       val c: Combine[K1,V1] = this.asInstanceOf[Combine[K1,V1]]

       insert2(ci, new AST.Combiner[K1,V1](c.in.convert2(ci), c.f) with KVLike[K1,V1] {
          def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K1,V1](tags)})

    }

    override def convertParallelDo[B : Manifest : WireFormat]
                               (ci: ConvertInfo, pd: ParallelDo[(K,V), B]): AST.Node[B] = {
      val n: AST.Node[(K, V)] = convert(ci)
      if ( ci.mscrs.exists(_.containsReducer(pd)) ) {
        pd.insert(ci, AST.Reducer(n, pd.dofn))
      } else {
        pd.insert(ci, AST.Mapper(n, pd.dofn))
      }
    }
  }


  /** The Flatten node type spcecifies the building of a DList that contains all the elements from
    * one or more exsiting DLists of the same type. */
  case class Flatten[A : Manifest : WireFormat](ins: List[DList[A]]) extends DList[A] {

    override val toString = "Flatten" + id

    val toVerboseString = toString + "([" + ins.map(_.toVerboseString).mkString(",") + "])"

    // Optimisation
    // ~~~~~~~~~~~~
    def justCopy(copied: CopyTable, cf: CopyFn[_]): (DList[A], CopyTable, Boolean) = {
      val (insUpd, copiedUpd, b) = justCopyInputs(ins, copied, cf.asInstanceOf[CopyFn[A]])
      val flat = Flatten(insUpd)
      (flat, copiedUpd + (this -> flat), b)
    }

    /** Perform a normal copy of this Flatten node but do not mark it as copied in the CopyTable. This will
      * mean that subsequent encounters of this node (that is, other outputs of this Flatten node) will result
      * in another copy, thereby replicating Flattens with multiple outputs. */
    override def optSplitFlattens(copied: CopyTable): (DList[A], CopyTable, Boolean) = copyOnce(copied) {
      val (insUpd, copiedUpd, b) = justCopyInputs(ins, copied, _.optSplitFlattens(_))
      val flat = Flatten(insUpd)
      (flat, copiedUpd, b)
    }

    /** If this Flatten node has any inputs that are also Flatten nodes, make all the inputs of the input Flatten
      * node inputs of this Flatten node. */
    override def optFuseFlattens(copied: CopyTable): (DList[A], CopyTable, Boolean) = copyOnce(copied) {
      val (insUpd, copiedUpd, b) = justCopyInputs(ins, copied, _.optSplitFlattens(_))
      val insUpdMerged = insUpd flatMap {
        case Flatten(otherIns) => otherIns
        case x                 => List(x)
      }
      val flat = Flatten(insUpdMerged)
      (flat, copiedUpd + (this -> flat), b)
    }

    private def justCopyInputs(ins: List[DList[A]], copied: CopyTable, cf: CopyFn[A]): (List[DList[A]], CopyTable, Boolean) = {
      ins.foldLeft((Nil: List[DList[A]], copied, false)) { case ((cps, ct, b), n) =>
        val (nUpd, ctUpd, bb) = cf(n, ct)
        (cps :+ nUpd, ctUpd + (n -> nUpd), bb || b)
      }
    }


    // Conversion
    // ~~~~~~~~~~
    def convertNew(ci: ConvertInfo) = insert(ci, AST.Flatten(ins.map(_.convert(ci))))

    def convertNew2[K : Manifest : WireFormat : Grouping,
                    V : Manifest : WireFormat]
                    (ci: ConvertInfo): AST.Node[(K,V)] with KVLike[K,V] = {
      val d: Flatten[(K,V)] = this.asInstanceOf[Flatten[(K,V)]]
      val ns: List[AST.Node[(K,V)]] = d.ins.map(_.convert2[K,V](ci))
      d.insert2(ci, new AST.Flatten(ns) with KVLike[K,V] {
                      def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K,V](tags)})
    }
  }


  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  //  Graph optimisations:
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /** Apply a series of stategies to optimise the strucuture of the logical plan (that
    * is the graph). The purpose of this is to transform the graph into a form that
    * is suitable for forming MSCRs. The strategies are:
    *   - splitting of Flattens with multiple outputs
    *   - sinking of Flattens
    *   - fusing Flattens
    *   - morphing Combine into ParallelDos when they don't follow a GroupByKey
    *   - fusing of ParallelDos
    *   - splitting of GroupByKeys with multiple outputs
    *   - splitting of Combine with multiple outputs. */
  def optimisePlan(outputs: List[DList[_]]): List[DList[_]] = {

    /** Perform a computation 'f' recursively starting with input 'x'. Return the
      * result of 'f' once the outputs are no longer changing, and an indication as
      * to whether there was ever a change. */
    def untilChanged[A](x: A, f: A => (A, Boolean)): (A, Boolean) = {

      def fWrapper(x: A, ec: Boolean): (A, Boolean, Boolean) = {
        val (y, changed) = f(x)
        (y, changed, ec)
      }

      def untilChangedWrapper(x: A, ec: Boolean): (A, Boolean) = {
        val (xUpd, changed, ecUpd) = fWrapper(x, ec)
        if (changed)
          untilChangedWrapper(xUpd, true)
        else
          (xUpd, ecUpd)
      }

      untilChangedWrapper(x, false)
    }

    /* Apply Flatten optimisation strategies in the following order:
     *    1. Split flattens.
     *    2. Sink flattens.
     *    3. Merge flattens.
     * For each strategy, do not move to the next until there are no changes.
     * Finish when running through all strategies results in no changes. */
    def flattenStrategies(in: List[DList[_]]): (List[DList[_]], Boolean) = {
      val (out1, c1) = untilChanged(in, travOnce(_.optSplitFlattens(_)))
      val (out2, c2) = untilChanged(out1, travOnce(_.optSinkFlattens(_)))
      val (out3, c3) = untilChanged(out2, travOnce(_.optFuseFlattens(_)))
      (out3, c1 || c2 || c3)
    }

    /* Run the strategies to produce the optimised graph. */
    def allStrategies(in: List[DList[_]]): (List[DList[_]], Boolean) = {
      val (flattenOpt, c1)         = untilChanged(in, flattenStrategies)
      val (combinerToParDoOpt, c2) = untilChanged(flattenOpt, travOnce(_.optCombinerToParDos(_)))
      val (parDoOpt, c3)           = untilChanged(combinerToParDoOpt, travOnce(_.optFuseParDos(_)))
      val (splitGbkOpt, c4)        = untilChanged(parDoOpt, travOnce(_.optSplitGbks(_)))
      val (splitCombineOpt, c5)    = untilChanged(splitGbkOpt, travOnce(_.optSplitCombines(_)))
      (splitCombineOpt, c1 || c2 || c3 || c4 || c5)
    }

    /* Any outputs that are ParallelDo's should be marked with fuse barriers. */
    val (outputsWithBarriers, _) = travOnce(_.optAddFuseBar(_, outputs.toSet))(outputs)

    /* Run all strategies. */
    val (optOutputs, _) = untilChanged(outputsWithBarriers, allStrategies)
    optOutputs
  }


  /** Helper method for traversing the graph from */
  private def travOnce
      (optFn: (DList[_], CopyTable) => (DList[_], CopyTable, Boolean))
      (outputs: List[DList[_]])
    : (List[DList[_]], Boolean) = {

    val emptyCopyTable: CopyTable = Map()
    val noCopies: List[DList[_]] = Nil

    val (_, copies, changes) = outputs.foldLeft((emptyCopyTable, noCopies, false)) { case ((ct, cps, b), n) =>
      val (nCopy, ctUpd, bb) = optFn(n, ct)
      (ctUpd, cps :+ nCopy, bb || b)
    }

    (copies, changes)
  }


  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Graph query helper functions:
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  def parentsOf(d: DList[_]): List[DList[_]] = {
    d match {
      case Load(_)                 => List()
      case ParallelDo(in, _, _, _) => List(in)
      case GroupByKey(in)          => List(in)
      case Combine(in, _)          => List(in)
      case Flatten(ins)            => ins
    }
  }

  def getParallelDo(d: DList[_]): Option[ParallelDo[_,_]] = {
    d match {
      case parDo@ParallelDo(_, _, _, _) => Some(parDo)
      case _                            => None
    }
  }

  def getFlatten(d: DList[_]): Option[Flatten[_]] = {
    d match {
      case flatten@Flatten(_) => Some(flatten)
      case _                  => None
    }
  }

  def getGroupByKey(d: DList[_]): Option[GroupByKey[_,_]] = {
    d match {
      case gbk@GroupByKey(_) => Some(gbk)
      case _                 => None
    }
  }

  def getCombine(d: DList[_]): Option[Combine[_,_]] = {
    d match {
      case c@Combine(_,_) => Some(c)
      case _              => None
    }
  }

  def isParallelDo(d: DList[_]): Boolean = getParallelDo(d).isDefined
  def isFlatten(d: DList[_]):    Boolean = getFlatten(d).isDefined
  def isGroupByKey(d: DList[_]): Boolean = getGroupByKey(d).isDefined
  def isCombine(d: DList[_]):    Boolean = getCombine(d).isDefined


  /*
   * Class that maintains state while the Smart.DList abstract syntax tree
   * is transformed to an AST.Node abstract syntax tree.
   *
   * Contains mutable maps. Beware, many uses of this data structure may look purely
   * functional but aren't.
   *
   */
  class ConvertInfo(val outMap: Map[Smart.DList[_], Set[DataSink[_,_,_]]],
                    val mscrs: Iterable[Intermediate.MSCR],
                    val g: DGraph,
                    val astMap: MMap[Smart.DList[_], AST.Node[_]],
                    /* A map of AST nodes to BridgeStores*/
                    val bridgeStoreMap: MMap[AST.Node[_], BridgeStore[_]]
                    ) {

    def getASTNode[A](d: Smart.DList[_]): AST.Node[A] = astMap.get(d) match {
       case Some(n) => n.asInstanceOf[AST.Node[A]]
       case None    => throw new RuntimeException("Node not found in map: " + d + "\n" + astMap)
    }

    def getASTCombiner[A,B](d: Smart.DList[_]): AST.Combiner[A,B] = astMap.get(d) match {
      case Some(n) => n.asInstanceOf[AST.Combiner[A,B]]
      case None    => throw new RuntimeException("Node not found in map: " + d + "\n" + astMap)
    }

    def getASTReducer[A,B,C](d: Smart.DList[_]): AST.Reducer[A,B,C] = astMap.get(d) match {
      case Some(n) => n.asInstanceOf[AST.Reducer[A,B,C]]
      case None    => throw new RuntimeException("Node not found in map: " + d + "\n" + astMap)
    }

    def getASTGbkReducer[A,B,C](d: Smart.DList[_]): AST.GbkReducer[A,B,C] = astMap.get(d) match {
      case Some(n) => n.asInstanceOf[AST.GbkReducer[A,B,C]]
      case None    => throw new RuntimeException("Node not found in map: " + d + "\n" + astMap)
    }

    def getASTFlatten[A](d: Smart.DList[_]): AST.Flatten[A] = astMap.get(d) match {
      case Some(n) => n.asInstanceOf[AST.Flatten[A]]
      case None    => throw new RuntimeException("Node not found in map: " + d + "\n" + astMap)
    }

    def getASTGroupByKey[K,V](d: Smart.DList[_]): AST.GroupByKey[K,V] = astMap.get(d) match {
      case Some(n) => n.asInstanceOf[AST.GroupByKey[K,V]]
      case None    => throw new RuntimeException("Node not found in map: " + d + "\n" + astMap)
    }

    def getBridgeStore(d: Smart.DList[_]): BridgeStore[_] = {
      val n: AST.Node[_] = getASTNode(d)
      bridgeStoreMap.get(n) match {
        case Some(bs) => bs
        case None     => {
          val newBS: BridgeStore[_] = BridgeStore(bridgeStoreMap.size)
          bridgeStoreMap += ((n, newBS))
          newBS
        }
      }
    }


  }

  object ConvertInfo {
    def apply(outMap: Map[Smart.DList[_], Set[DataSink[_,_,_]]],
              mscrs: Iterable[Intermediate.MSCR],
              g: DGraph): ConvertInfo = {
      new ConvertInfo(outMap, mscrs, g, MMap(), MMap())
    }
  }
}


trait KVLike[K,V] {
  def mkTaggedIdentityMapper(tags: Set[Int]): TaggedIdentityMapper[K,V]
}
