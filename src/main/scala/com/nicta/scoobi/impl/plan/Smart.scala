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
package impl
package plan

import scala.collection.mutable.{Map => MMap}

import core._
import application.ScoobiConfiguration
import io.DataSource
import io.DataSink
import exec.Env
import exec.BridgeStore
import exec.TaggedIdentityMapper
import util.UniqueInt


/** Abstract syntax of tree of primitive "language". */
object Smart {

  object Id extends UniqueInt

  type CopyFn[A, Sh <: Shape] = (DComp[A, Sh], CopyTable) => (DComp[A, Sh], CopyTable, Boolean)
  type CopyTable = Map[DComp[_, _ <: Shape], DComp[_, _ <: Shape]]


  /** GADT for distributed list computation graph. */
  sealed abstract class DComp[A : Manifest : WireFormat, Sh <: Shape] {

    val id = Id.get

    override def equals(other: Any) = {
      other match {
        case dc: DComp[_, _] => dc.id == this.id
        case _               => false
      }
    }

    override def hashCode = id

    def toVerboseString: String


    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //  Optimisation strategies:
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    /** An optimisation strategy that any ParallelDo that is connected to an output is marked
      * with a fuse barrier. */
    def optAddFuseBar(copied: CopyTable, outputs: Set[DComp[_, _ <: Shape]]): (DComp[A, Sh], CopyTable, Boolean) =
      copyOnceWith(copied, _.optAddFuseBar(_, outputs))

    /** An optimisation strategy that replicates any Flatten nodes that have multiple outputs
      * such that in the resulting AST Flatten nodes only have single outputs. */
    def optSplitFlattens(copied: CopyTable): (DComp[A, Sh], CopyTable, Boolean) =
      copyOnceWith(copied, _.optSplitFlattens(_))

    /** An optimisation strategy that sinks a Flatten node that is an input to a ParallelDo node
      * such that in the resulting AST replicated ParallelDo nodes are inputs to the Flatten
      * node. */
    def optSinkFlattens(copied: CopyTable): (DComp[A, Sh], CopyTable, Boolean) =
      copyOnceWith(copied, _.optSinkFlattens(_))

    /** An optimisation strategy that fuse any Flatten nodes that are inputs to Flatten nodes. */
    def optFuseFlattens(copied: CopyTable): (DComp[A, Sh], CopyTable, Boolean) =
      copyOnceWith(copied, _.optFuseFlattens(_))

    /** An optimisation strategy that morphs a Combine node into a ParallelDo node if it does not
      * follow a GroupByKey node. */
    def optCombinerToParDos(copied: CopyTable): (DComp[A, Sh], CopyTable, Boolean) =
      copyOnceWith(copied, _.optCombinerToParDos(_))

    /** An optimisation strategy that fuses the functionality of a ParallelDo node that is an input
      * to another ParallelDo node. */
    def optFuseParDos(copied: CopyTable): (DComp[A, Sh], CopyTable, Boolean) =
      copyOnceWith(copied, _.optFuseParDos(_))

    /** An optimisation strategy that replicates any GroupByKey nodes that have multiple outputs
      * such that in the resulting AST, GroupByKey nodes have only single outputs. */
    def optSplitGbks(copied: CopyTable): (DComp[A, Sh], CopyTable, Boolean) =
      copyOnceWith(copied, _.optSplitGbks(_))

    /** An optimisation strategy that replicates any Combine nodes that have multiple outputs
      * such that in the resulting AST, Combine nodes have only single outputs. */
    def optSplitCombines(copied: CopyTable): (DComp[A, Sh], CopyTable, Boolean) =
      copyOnceWith(copied, _.optSplitCombines(_))


    /** Perform a depth-first traversal copy of the DComp node. When copying the input DComp
      * node(s), a the CopyFn function is used (somewhat like a callback). */
    protected def justCopy(copied: CopyTable, cf: CopyFn[_,_]): (DComp[A, Sh], CopyTable, Boolean)

    /** A helper method that checks whether the node has already been copied, and if so returns the copy, else
      * invokes a user provided code implementing the copy. */
    protected def copyOnce(copied: CopyTable)(newCopy: => (DComp[A, Sh], CopyTable, Boolean)): (DComp[A, Sh], CopyTable, Boolean) =
      copied.get(this) match {
        case Some(copy) => (copy.asInstanceOf[DComp[A, Sh]], copied, false)
        case None       => newCopy
      }

    /* Helper for performing optimisation with depth-first traversal once-off copying. */
    private def copyOnceWith(copied: CopyTable, cf: CopyFn[A, Sh]): (DComp[A, Sh], CopyTable, Boolean) =
      copyOnce(copied) { justCopy(copied, cf) }


    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //  Conversion:
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    final def insert(ci: ConvertInfo, n: AST.Node[A, Sh]): AST.Node[A, Sh] = {
      ci.astMap += ((this, n))
      n
    }

    final def insert2[K : Manifest : WireFormat : Grouping,
                V : Manifest : WireFormat]
                (ci: ConvertInfo, n: AST.Node[(K,V), Sh] with KVLike[K,V]):
                AST.Node[(K,V), Sh] with KVLike[K,V] = {
      ci.astMap += ((this,n))
      n
    }

    def dataSource(ci: ConvertInfo): DataSource[_,_,_] = ci.getBridgeStore(this)

    final def convert(ci: ConvertInfo): AST.Node[A, Sh]  = {
      val maybeN: Option[AST.Node[_,_]] = ci.astMap.get(this)
      maybeN match {
        case Some(n) => n.asInstanceOf[AST.Node[A, Sh]] // Run-time cast. Shouldn't fail though.
        case None    => convertNew(ci)
      }
    }

    final def convert2[K : Manifest : WireFormat : Grouping,
                 V : Manifest : WireFormat]
                 (ci: ConvertInfo): AST.Node[(K,V), Sh] with KVLike[K,V]  = {
      val maybeN: Option[AST.Node[_,_]] = ci.astMap.get(this)
      maybeN match {
        case Some(n) => n.asInstanceOf[AST.Node[(K,V), Sh] with KVLike[K,V]] // Run-time cast. Shouldn't fail though.
        case None    => convertNew2(ci)
      }
    }


    def convertNew(ci: ConvertInfo): AST.Node[A, Sh]

    def convertNew2[K : Manifest : WireFormat : Grouping,
                    V : Manifest : WireFormat]
                    (ci: ConvertInfo): AST.Node[(K,V), Sh] with KVLike[K,V]

    def convertParallelDo[B : Manifest : WireFormat,
                          E : Manifest : WireFormat]
        (ci: ConvertInfo,
         pd: ParallelDo[A, B, E])
      : AST.Node[B, Arr]
  }


  /** The Load node type specifies the creation of a DComp from some source other than another DComp.
    * A DataSource object specifies how the loading is performed. */
  case class Load[A](source: DataSource[_, _, A])(implicit mA: Manifest[A], val wtA: WireFormat[A]) extends DComp[A, Arr] {

    override val toString = "Load" + id

    lazy val toVerboseString = toString

    // Optimisation
    // ~~~~~~~~~~~~
    def justCopy(copied: CopyTable, cf: CopyFn[_,_]): (DComp[A, Arr], CopyTable, Boolean) =
      (this, copied, false)


    // Conversion
    // ~~~~~~~~~~
    def convertNew(ci: ConvertInfo) = {
      insert(ci, AST.Load())
    }

    def convertNew2[K : Manifest : WireFormat : Grouping,
                    V : Manifest : WireFormat]
                    (ci: ConvertInfo): AST.Node[(K,V), Arr] with KVLike[K,V] = {

      insert2(ci, new AST.Load[(K,V)]() with KVLike[K,V] {
                    def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K,V](tags)})
    }

    def convertParallelDo[B : Manifest : WireFormat, E : Manifest : WireFormat]
        (ci: ConvertInfo,
         pd: ParallelDo[A, B, E])
      : AST.Node[B, Arr] = {

      val n: AST.Node[A, Arr] = convert(ci)
      val e: AST.Node[E, Exp] = pd.env.convert(ci)
      pd.insert(ci, AST.Mapper(n, e, pd.dofn))
    }

    override def dataSource(ci: ConvertInfo): DataSource[_,_,_] = source
  }


  /** The ParallelDo node type specifies the building of a DComp as a result of applying a function to
    * all elements of an existing DComp and concatenating the results. */
  case class ParallelDo[A, B, E]
      (in: DComp[A, Arr],
       env: DComp[E, Exp],
       dofn: EnvDoFn[A, B, E],
       groupBarrier: Boolean = false,
       fuseBarrier: Boolean = false)
      (implicit val mA: Manifest[A], val wtA: WireFormat[A],
                mB: Manifest[B], wtB: WireFormat[B],
                val mE: Manifest[E], val wtE: WireFormat[E])
    extends DComp[B, Arr] {

    override val toString = "ParallelDo" + id + (if (groupBarrier) "*" else "") + (if (fuseBarrier) "%" else "")

    lazy val toVerboseString = toString + "(" + env.toVerboseString + "," + in.toVerboseString + ")"

    // Optimisation
    // ~~~~~~~~~~~~
    def justCopy(copied: CopyTable, cf: CopyFn[_,_]): (DComp[B, Arr], CopyTable, Boolean) =
      justCopy(copied, cf, fuseBarrier)

    def justCopy(copied: CopyTable, cf: CopyFn[_,_], fb: Boolean): (DComp[B, Arr], CopyTable, Boolean) = {
      val cfA = cf.asInstanceOf[CopyFn[A, Arr]]
      val (inUpd, copiedUpd1, b1) = cfA(in, copied)

      val cfE = cf.asInstanceOf[CopyFn[E, Exp]]
      val (envUpd, copiedUpd2, b2) = cfE(env, copiedUpd1)

      val pd = ParallelDo(inUpd, envUpd, dofn, groupBarrier, fb)
      (pd, copiedUpd2 + (this -> pd), b1 || b2)
    }

    /** If this ParallelDo is connected to an output, replicate it with a fuse barrier. */
    override def optAddFuseBar(copied: CopyTable, outputs: Set[DComp[_, _ <: Shape]]): (DComp[B, Arr], CopyTable, Boolean) = copyOnce(copied) {
      val requireFuseBarrier = outputs.contains(this)
      justCopy(copied, (n: DComp[A, Arr], ct: CopyTable) => n.optAddFuseBar(ct, outputs), requireFuseBarrier)
    }

    /** If the input to this ParallelDo is a Flatten node, re-write the tree as a Flatten node with the
      * ParallelDo replicated on each of its inputs. Otherwise just perform a normal copy. */
    override def optSinkFlattens(copied: CopyTable): (DComp[B, Arr], CopyTable, Boolean) = copyOnce(copied) {
      in match {
        case Flatten(ins) => {
          val (copiedUpd, insUpd) = ins.foldLeft((copied, Nil: List[DComp[A, Arr]])) { case ((ct, copies), in) =>
            val (inUpd, ctUpd, _) = in.optSinkFlattens(ct)
            (ctUpd + (in -> inUpd), copies :+ inUpd)
          }

          val flat: DComp[B, Arr] = Flatten(insUpd.map(ParallelDo(_, env, dofn, groupBarrier, fuseBarrier)))
          (flat, copiedUpd + (this -> flat), true)
        }
        case _            => justCopy(copied, (n: DComp[A, Arr], ct: CopyTable) => n.optSinkFlattens(ct))
      }
    }

    /** If the input to this ParallelDo is another ParallelDo node, re-write this ParallelDo with the preceeding's
      * ParallelDo's "mapping function" fused in. Otherwise just perform a normal copy. */
    override def optFuseParDos(copied: CopyTable): (DComp[B, Arr], CopyTable, Boolean) = copyOnce(copied) {

      /* Create a new ParallelDo function that is the fusion of two connected ParallelDo functions. */
      def fuseDoFn[X, Y, Z, F, G](f: EnvDoFn[X, Y, F], g: EnvDoFn[Y, Z, G]): EnvDoFn[X, Z, (F, G)] = new EnvDoFn[X, Z, (F, G)] {
        def setup(env: (F, G)) = {
          f.setup(env._1)
          g.setup(env._2)
        }

        def process(env: (F, G), input: X, emitter: Emitter[Z]) = {
          f.process(env._1, input, new Emitter[Y] { def emit(value: Y) = g.process(env._2, value, emitter) } )
        }

        def cleanup(env: (F, G), emitter: Emitter[Z]) = {
          f.cleanup(env._1, new Emitter[Y] { def emit(value: Y) = g.process(env._2, value, emitter) } )
          g.cleanup(env._2, emitter)
        }
      }

      /* Create a new environment by forming a tuple from two seperate evironments.*/
      def fuseEnv[F : Manifest : WireFormat, G : Manifest : WireFormat](fExp: DComp[F, Exp], gExp: DComp[G, Exp]): DComp[(F, G), Exp] =
        Op(fExp, gExp, (f: F, g: G) => (f, g))

      def fuseParallelDos[X : Manifest : WireFormat,
                          Y : Manifest : WireFormat,
                          Z : Manifest : WireFormat,
                          F : Manifest : WireFormat,
                          G : Manifest : WireFormat]
          (pd1: ParallelDo[X, Y, F],
           pd2: ParallelDo[Y, Z, G])
        : ParallelDo[X, Z, (F, G)] = {

          val ParallelDo(in1, env1, dofn1, gb1, _) = pd1
          val ParallelDo(in2, env2, dofn2, gb2, fb2) = pd2

          val fusedDoFn = fuseDoFn(dofn1, dofn2)
          val fusedEnv = fuseEnv(env1, env2)

          implicit val mFG: Manifest[(F, G)] =
            Manifest.classType(classOf[Tuple2[F,G]], implicitly[Manifest[F]], implicitly[Manifest[G]])
          implicit val wtFG: WireFormat[(F, G)] =
            WireFormat.Tuple2Fmt(implicitly[WireFormat[F]], implicitly[WireFormat[G]])

          new ParallelDo(in1, fusedEnv, fusedDoFn, gb1 || gb2, fb2)
      }

      in match {
        case ParallelDo(_, _, _, _, false) => {
          val (inUpd, copiedUpd, _) = in.optFuseParDos(copied)
          val prev@ParallelDo(_, _, _, _, _) = inUpd
          val pd = fuseParallelDos(prev, this)(prev.mA, prev.wtA, mA, wtA, mB, wtB, prev.mE, prev.wtE, mE, wtE)
          (pd, copiedUpd + (this -> pd), true)
        }
        case _ => justCopy(copied, (n: DComp[A, Arr], ct: CopyTable) => n.optFuseParDos(ct))
      }
    }


    // Conversion
    // ~~~~~~~~~~
    def convertNew(ci: ConvertInfo): AST.Node[B, Arr] = {
      in.convert(ci)
      in.convertParallelDo(ci, this)
    }

    def convertNew2[K : Manifest : WireFormat : Grouping,
                    V : Manifest : WireFormat](ci: ConvertInfo):
                    AST.Node[(K,V), Arr] with KVLike[K,V] = {

      if (ci.mscrs.exists(_.containsGbkReducer(this))) {
        val pd: ParallelDo[(K, Iterable[V]), (K,V), E] = this.asInstanceOf[ParallelDo[(K, Iterable[V]), (K,V), E]]
        val n: AST.Node[(K, Iterable[V]), Arr] = pd.in.convert(ci)
        val e: AST.Node[E, Exp] = pd.env.convert(ci)

        pd.insert2(ci, new AST.GbkReducer(n, e, pd.dofn) with KVLike[K,V] {
          def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K,V](tags)})

      } else {
        val pd: ParallelDo[A, (K,V), E] = this.asInstanceOf[ParallelDo[A, (K,V), E]]
        val n: AST.Node[A, Arr] = pd.in.convert(ci)
        val e: AST.Node[E, Exp] = pd.env.convert(ci)

        if ( ci.mscrs.exists(_.containsGbkMapper(pd)) ) {
          pd.insert2(ci, new AST.GbkMapper(n, e, pd.dofn) with KVLike[K,V] {
            def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K,V](tags)})
        } else {
          pd.insert2(ci, new AST.Mapper(n, e, pd.dofn) with KVLike[K,V] {
            def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K,V](tags)})
        }
      }
    }

    def convertParallelDo[C : Manifest : WireFormat,
                          F : Manifest : WireFormat]
        (ci: ConvertInfo,
         pd: ParallelDo[B, C, F])
      : AST.Node[C, Arr] = {

      val n: AST.Node[B, Arr] = pd.in.convert(ci)
      val e: AST.Node[F, Exp] = pd.env.convert(ci)
      pd.insert(ci, AST.Mapper(n, e, pd.dofn))
    }
  }


  /** The GroupByKey node type specifies the building of a DComp as a result of partitioning an exiting
    * key-value DComp by key. */
  case class GroupByKey[K, V]
      (in: DComp[(K, V), Arr])
      (implicit mK: Manifest[K], wtK: WireFormat[K], val grpK: Grouping[K],
                mV: Manifest[V], wtV: WireFormat[V])

    extends DComp[(K, Iterable[V]), Arr] {

    override val toString = "GroupByKey" + id

    lazy val toVerboseString = toString + "(" + in.toVerboseString + ")"

    // Optimisation
    // ~~~~~~~~~~~~
    def justCopy(copied: CopyTable, cf: CopyFn[_,_]): (DComp[(K, Iterable[V]), Arr], CopyTable, Boolean) = {
      val cfKV = cf.asInstanceOf[CopyFn[(K, V), Arr]]
      val (inUpd, copiedUpd, b) = cfKV(in, copied)
      val gbk = GroupByKey(inUpd)
      (gbk, copiedUpd + (this -> gbk), b)
    }

    /** Perform a normal copy of this Flatten node but do not mark it as copied in the CopyTable. This will
      * mean subsequent encounters of this node (that is, other outputs of this GroupByKey node) will result
      * in another copy, thereby replicating GroupByKeys with multiple outputs. If this GroupByKey node has a
      * Flatten as its input, replicate the Flatten node as well using the same technique. */
    override def optSplitGbks(copied: CopyTable): (DComp[(K, Iterable[V]), Arr], CopyTable, Boolean) = copyOnce(copied) {
      in match {
        case Flatten(ins) => {
          val (insUpd, copiedUpd, b) = ins.foldLeft((Nil: List[DComp[(K, V), Arr]], copied, false)) { case ((cps, ct, b), n) =>
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
              (ci: ConvertInfo, d: DComp[(A, B), Arr]):
              AST.Node[(A, Iterable[B]), Arr] with KVLike[A, Iterable[B]] = {
         insert2(ci, new AST.GroupByKey[A,B](d.convert2(ci)) with KVLike[A, Iterable[B]] {
           def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[A,Iterable[B]](tags)})
      }

    def convertNew2[K1 : Manifest : WireFormat : Grouping,
                    V1 : Manifest : WireFormat]
                    (ci: ConvertInfo): AST.Node[(K1,V1), Arr] with KVLike[K1,V1] = {
      convertAux(ci, in).asInstanceOf[AST.Node[(K1,V1), Arr] with KVLike[K1,V1]]


    }

    override def convertParallelDo[B : Manifest : WireFormat,
                                   E : Manifest : WireFormat]
        (ci: ConvertInfo,
         pd: ParallelDo[(K,Iterable[V]), B, E])
      : AST.Node[B, Arr] = {

      val n: AST.Node[(K, Iterable[V]), Arr] = convert(ci)
      val e: AST.Node[E, Exp] = pd.env.convert(ci)
      if ( ci.mscrs.exists(_.containsGbkReducer(pd)) ) {
        pd.insert(ci, AST.GbkReducer(n, e, pd.dofn))
      } else {
        pd.insert(ci, AST.Mapper(n, e, pd.dofn))
      }
    }
  }


  /** The Combine node type specifies the building of a DComp as a result of applying an associative
    * function to the values of an existing key-values DComp. */
  case class Combine[K : Manifest : WireFormat : Grouping,
                     V : Manifest : WireFormat]
      (in: DComp[(K, Iterable[V]), Arr],
       f: (V, V) => V)
    extends DComp[(K, V), Arr] {

    override val toString = "Combine" + id

    lazy val toVerboseString = toString + "(" + in.toVerboseString + ")"

    // Optimisation
    // ~~~~~~~~~~~~
    def justCopy(copied: CopyTable, cf: CopyFn[_,_]): (DComp[(K, V), Arr], CopyTable, Boolean) = {
      val cfKIV = cf.asInstanceOf[CopyFn[(K, Iterable[V]), Arr]]
      val (inUpd, copiedUpd, b) = cfKIV(in, copied)
      val comb = Combine(inUpd, f)
      (comb, copiedUpd + (this -> comb), b)
    }

    /** If this Combine node's input is a GroupByKey node, perform a normal copy. Otherwise, create a
      * ParallelDo node whose mapping function performs the combining functionality. */
    override def optCombinerToParDos(copied: CopyTable): (DComp[(K, V), Arr], CopyTable, Boolean) = copyOnce(copied) {
      in match {
        case GroupByKey(inKV) => justCopy(copied, (n: DComp[(K, V), Arr], ct: CopyTable) => n.optCombinerToParDos(ct))
        case _                => {
          val (inUpd, copiedUpd, _) = in.optCombinerToParDos(copied)

          val dofn = new EnvDoFn[(K, Iterable[V]), (K, V), Unit] {
            def setup(env: Unit) = {}
            def process(env: Unit, input: (K, Iterable[V]), emitter: Emitter[(K, V)]) = {
              val key = input._1
              val values = input._2.toStream
              emitter.emit(key, values.reduce(f))
            }
            def cleanup(env: Unit, emitter: Emitter[(K, V)]) = {}
          }
          val pd = ParallelDo(inUpd, Return(()), dofn, false, false)
          (pd, copiedUpd + (this -> pd), true)
        }
      }
    }

    /** Perform a normal copy of this Combine node, along with subsequent GroupByKey and Flatten nodes, but
      * do not mark it as copied in the CopyTable. This will mean subsequent encounters of this node (that
      * is, other outputs of this Combine node) will result in another copy, thereby replicating Combine
      * nodes with multiple outputs. */
    override def optSplitCombines(copied: CopyTable): (DComp[(K, V), Arr], CopyTable, Boolean) = copyOnce(copied) {
      in match {
        case GroupByKey(groupIn) => groupIn match {

          case Flatten(ins) => {
            val (insUpd, copiedUpd, b) = ins.foldLeft((Nil: List[DComp[(K, V), Arr]], copied, false)) { case ((cps, ct, b), n) =>
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
                    (ci: ConvertInfo): AST.Node[(K1,V1), Arr] with KVLike[K1,V1] = {
       val c: Combine[K1,V1] = this.asInstanceOf[Combine[K1,V1]]

       insert2(ci, new AST.Combiner[K1,V1](c.in.convert2(ci), c.f) with KVLike[K1,V1] {
          def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K1,V1](tags)})

    }

    override def convertParallelDo[B : Manifest : WireFormat,
                                   E : Manifest : WireFormat]
        (ci: ConvertInfo,
         pd: ParallelDo[(K,V), B, E])
      : AST.Node[B, Arr] = {

      val n: AST.Node[(K, V), Arr] = convert(ci)
      val e: AST.Node[E, Exp] = pd.env.convert(ci)
      if ( ci.mscrs.exists(_.containsReducer(pd)) ) {
        pd.insert(ci, AST.Reducer(n, e, pd.dofn))
      } else {
        pd.insert(ci, AST.Mapper(n, e, pd.dofn))
      }
    }
  }


  /** The Flatten node type spcecifies the building of a DComp that contains all the elements from
    * one or more exsiting DLists of the same type. */
  case class Flatten[A : Manifest : WireFormat](ins: List[DComp[A, Arr]]) extends DComp[A, Arr] {

    override val toString = "Flatten" + id

    lazy val toVerboseString = toString + "([" + ins.map(_.toVerboseString).mkString(",") + "])"

    // Optimisation
    // ~~~~~~~~~~~~
    def justCopy(copied: CopyTable, cf: CopyFn[_,_]): (DComp[A, Arr], CopyTable, Boolean) = {
      val (insUpd, copiedUpd, b) = justCopyInputs(ins, copied, cf.asInstanceOf[CopyFn[A, Arr]])
      val flat = Flatten(insUpd)
      (flat, copiedUpd + (this -> flat), b)
    }

    /** Perform a normal copy of this Flatten node but do not mark it as copied in the CopyTable. This will
      * mean that subsequent encounters of this node (that is, other outputs of this Flatten node) will result
      * in another copy, thereby replicating Flattens with multiple outputs. */
    override def optSplitFlattens(copied: CopyTable): (DComp[A, Arr], CopyTable, Boolean) = copyOnce(copied) {
      val (insUpd, copiedUpd, b) = justCopyInputs(ins, copied, _.optSplitFlattens(_))
      val flat = Flatten(insUpd)
      (flat, copiedUpd, b)
    }

    /** If this Flatten node has any inputs that are also Flatten nodes, make all the inputs of the input Flatten
      * node inputs of this Flatten node. */
    override def optFuseFlattens(copied: CopyTable): (DComp[A, Arr], CopyTable, Boolean) = copyOnce(copied) {
      val (insUpd, copiedUpd, b) = justCopyInputs(ins, copied, _.optFuseFlattens(_))
      val insUpdMerged = insUpd flatMap {
        case Flatten(otherIns) => otherIns
        case x                 => List(x)
      }
      val flat = Flatten(insUpdMerged)
      (flat, copiedUpd + (this -> flat), b)
    }

    private def justCopyInputs(ins: List[DComp[A, Arr]], copied: CopyTable, cf: CopyFn[A, Arr]): (List[DComp[A, Arr]], CopyTable, Boolean) = {
      ins.foldLeft((Nil: List[DComp[A, Arr]], copied, false)) { case ((cps, ct, b), n) =>
        val (nUpd, ctUpd, bb) = cf(n, ct)
        (cps :+ nUpd, ctUpd + (n -> nUpd), bb || b)
      }
    }


    // Conversion
    // ~~~~~~~~~~
    def convertNew(ci: ConvertInfo) = insert(ci, AST.Flatten(ins.map(_.convert(ci))))

    def convertNew2[K : Manifest : WireFormat : Grouping,
                    V : Manifest : WireFormat]
                    (ci: ConvertInfo): AST.Node[(K,V), Arr] with KVLike[K,V] = {
      val d: Flatten[(K,V)] = this.asInstanceOf[Flatten[(K,V)]]
      val ns: List[AST.Node[(K,V), Arr]] = d.ins.map(_.convert2[K,V](ci))
      d.insert2(ci, new AST.Flatten(ns) with KVLike[K,V] {
                      def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K,V](tags)})
    }

    def convertParallelDo[B : Manifest : WireFormat,
                          E : Manifest : WireFormat]
        (ci: ConvertInfo,
         pd: ParallelDo[A, B, E])
      : AST.Node[B, Arr] = {

      val n: AST.Node[A, Arr] = convert(ci)
      val e: AST.Node[E, Exp] = pd.env.convert(ci)
      pd.insert(ci, AST.Mapper(n, e, pd.dofn))
    }
  }


  /** The Materialize node type specifies the conversion of an Arr DComp to an Exp DComp. */
  case class Materialize[A : Manifest : WireFormat](in: DComp[A, Arr]) extends DComp[Iterable[A], Exp] {

    override val toString = "Materialize" + id

    lazy val toVerboseString = toString + "(" + in.toVerboseString + ")"

    // Optimisation
    // ~~~~~~~~~~~~
    def justCopy(copied: CopyTable, cf: CopyFn[_,_]): (DComp[Iterable[A], Exp], CopyTable, Boolean) = {
      val cfA = cf.asInstanceOf[CopyFn[A, Arr]]
      val (inUpd, copiedUpd, b) = cfA(in, copied)
      val mat = Materialize(inUpd)
      (mat, copiedUpd + (this -> mat), b)
    }


    // Conversion
    // ~~~~~~~~~~
    def convertNew(ci: ConvertInfo): AST.Node[Iterable[A], Exp] = {
      val node = AST.Materialize(in.convert(ci))
      ci.envMap += (node -> Env(implicitly[WireFormat[Iterable[A]]], ci.conf))
      insert(ci, node)
    }

    def convertNew2[K : Manifest : WireFormat : Grouping,
                    V : Manifest : WireFormat]
                    (ci: ConvertInfo): AST.Node[(K,V), Exp] with KVLike[K,V] =
      sys.error("Materialize can not be in an input to a GroupByKey")

    def convertParallelDo[B : Manifest : WireFormat,
                          E : Manifest : WireFormat]
        (ci: ConvertInfo,
         pd: ParallelDo[Iterable[A], B, E])
      : AST.Node[B, Arr] = sys.error("Materialize can not be an input to ParallelDo")
  }


  /** The Op node type specifies the building of Exp DComp by applying a function to the values
    * of two other Exp DComp nodes. */
  case class Op[A : Manifest : WireFormat,
                B : Manifest : WireFormat,
                C : Manifest : WireFormat]
      (in1: DComp[A, Exp],
       in2: DComp[B, Exp],
       f: (A, B) => C)
    extends DComp[C, Exp] {

    override val toString = "Op" + id

    lazy val toVerboseString = toString + "[" + in1.toVerboseString + "," + in2.toVerboseString + "]"


    // Optimisation
    // ~~~~~~~~~~~~
    def justCopy(copied: CopyTable, cf: CopyFn[_,_]): (DComp[C, Exp], CopyTable, Boolean) = {
      val cfA = cf.asInstanceOf[CopyFn[A, Exp]]
      val (inUpd1, copiedUpd1, b1) = cfA(in1, copied)
      val cfB = cf.asInstanceOf[CopyFn[B, Exp]]
      val (inUpd2, copiedUpd2, b2) = cfB(in2, copiedUpd1)
      val op = Op(inUpd1, inUpd2, f)
      (op, copiedUpd2 + (this -> op), b1 || b2)
    }


    // Conversion
    // ~~~~~~~~~~
    def convertNew(ci: ConvertInfo): AST.Node[C, Exp] = {
      val exp1 = in1.convert(ci)
      val exp2 = in2.convert(ci)
      val node = AST.Op(exp1, exp2, f)
      ci.envMap += (node -> Env(implicitly[WireFormat[C]], ci.conf))
      insert(ci, node)
    }

    def convertNew2[K : Manifest : WireFormat : Grouping,
                    V : Manifest : WireFormat]
                    (ci: ConvertInfo): AST.Node[(K,V), Exp] with KVLike[K,V] =
      sys.error("Op can not be in an input to a GroupByKey")

    def convertParallelDo[D : Manifest : WireFormat,
                          E : Manifest : WireFormat]
        (ci: ConvertInfo,
         pd: ParallelDo[C, D, E])
      : AST.Node[D, Arr] = sys.error("Op can not be an input to ParallelDo")
  }


  /** The Return node type specifies the building of a Exp DComp from an "ordinary" value. */
  case class Return[A : Manifest : WireFormat](x: A) extends DComp[A, Exp] {

    override val toString = "Return" + id

    /**
     * we don't represent the value contained in this Return node because it is potentially very large
     * and could trigger and OutOfMemoryError
     */
    lazy val toVerboseString = toString

    // Optimisation
    // ~~~~~~~~~~~~
    def justCopy(copied: CopyTable, cf: CopyFn[_,_]): (DComp[A, Exp], CopyTable, Boolean) =
      (this, copied, false)


    // Conversion
    // ~~~~~~~~~~
    def convertNew(ci: ConvertInfo): AST.Node[A, Exp] = {
      val node = AST.Return(x)
      ci.envMap += (node -> Env(implicitly[WireFormat[A]], ci.conf))
      insert(ci, node)
    }

    def convertNew2[K : Manifest : WireFormat : Grouping,
                    V : Manifest : WireFormat]
                    (ci: ConvertInfo): AST.Node[(K,V), Exp] with KVLike[K,V] =
      sys.error("Return can not be in an input to a GroupByKey")

    def convertParallelDo[B : Manifest : WireFormat,
                          E : Manifest : WireFormat]
        (ci: ConvertInfo,
         pd: ParallelDo[A, B, E])
      : AST.Node[B, Arr] = sys.error("Return can not be an input to ParallelDo")
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
  def optimisePlan(outputs: List[DComp[_, _ <: Shape]]): List[DComp[_, _ <: Shape]] = {

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
    def flattenStrategies(in: List[DComp[_, _ <: Shape]]): (List[DComp[_, _ <: Shape]], Boolean) = {
      val (out1, c1) = untilChanged(in, travOnce(_.optSplitFlattens(_)))
      val (out2, c2) = untilChanged(out1, travOnce(_.optSinkFlattens(_)))
      val (out3, c3) = untilChanged(out2, travOnce(_.optFuseFlattens(_)))
      (out3, c1 || c2 || c3)
    }

    /* Run the strategies to produce the optimised graph. */
    def allStrategies(in: List[DComp[_, _ <: Shape]]): (List[DComp[_, _ <: Shape]], Boolean) = {
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
      (optFn: (DComp[_, _ <: Shape], CopyTable) => (DComp[_, _ <: Shape], CopyTable, Boolean))
      (outputs: List[DComp[_, _ <: Shape]])
    : (List[DComp[_, _ <: Shape]], Boolean) = {

    val emptyCopyTable: CopyTable = Map.empty
    val noCopies: List[DComp[_, _ <: Shape]] = Nil

    val (_, copies, changes) = outputs.foldLeft((emptyCopyTable, noCopies, false)) { case ((ct, cps, b), n) =>
      val (nCopy, ctUpd, bb) = optFn(n, ct)
      (ctUpd, cps :+ nCopy, bb || b)
    }

    (copies, changes)
  }


  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Graph query helper functions:
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  def parentsOf(d: DComp[_, _ <: Shape]): List[DComp[_, _ <: Shape]] = {
    d match {
      case Load(_)                      => Nil
      case ParallelDo(in, env, _, _, _) => List(in, env)
      case GroupByKey(in)               => List(in)
      case Combine(in, _)               => List(in)
      case Flatten(ins)                 => ins
      case Materialize(in)              => List(in)
      case Op(in1, in2, f)              => List(in1, in2)
      case Return(_)                    => Nil
    }
  }

  def getParallelDo(d: DComp[_, _ <: Shape]): Option[ParallelDo[_,_,_]] = {
    d match {
      case parDo@ParallelDo(_, _, _, _, _) => Some(parDo)
      case _                               => None
    }
  }

  def getFlatten(d: DComp[_, _ <: Shape]): Option[Flatten[_]] = {
    d match {
      case flatten@Flatten(_) => Some(flatten)
      case _                  => None
    }
  }

  def getGroupByKey(d: DComp[_, _ <: Shape]): Option[GroupByKey[_,_]] = {
    d match {
      case gbk@GroupByKey(_) => Some(gbk)
      case _                 => None
    }
  }

  def getCombine(d: DComp[_, _ <: Shape]): Option[Combine[_,_]] = {
    d match {
      case c@Combine(_,_) => Some(c)
      case _              => None
    }
  }

  def getMaterialize(d: DComp[_, _ <: Shape]): Option[Materialize[_]] = {
    d match {
      case m@Materialize(_) => Some(m)
      case _                => None
    }
  }

  def isParallelDo(d: DComp[_, _ <: Shape]):  Boolean = getParallelDo(d).isDefined
  def isFlatten(d: DComp[_, _ <: Shape]):     Boolean = getFlatten(d).isDefined
  def isGroupByKey(d: DComp[_, _ <: Shape]):  Boolean = getGroupByKey(d).isDefined
  def isCombine(d: DComp[_, _ <: Shape]):     Boolean = getCombine(d).isDefined
  def isMaterialize(d: DComp[_, _ <: Shape]): Boolean = getMaterialize(d).isDefined


  /*
   * Class that maintains state while the Smart.DComp abstract syntax tree
   * is transformed to an AST.Node abstract syntax tree.
   *
   * Contains mutable maps. Beware, many uses of this data structure may look purely
   * functional but aren't.
   *
   */
  case class ConvertInfo(
      conf: ScoobiConfiguration,
      outMap: Map[Smart.DComp[_, _ <: Shape], Set[DataSink[_,_,_]]],
      mscrs: Iterable[Intermediate.MSCR],
      g: DGraph,
      astMap: MMap[Smart.DComp[_, _ <: Shape], AST.Node[_, _ <: Shape]],
      envMap: MMap[AST.Node[_, _ <: Shape], Env[_]],
      /* A map of AST nodes to BridgeStores*/
      bridgeStoreMap: MMap[AST.Node[_, _ <: Shape], BridgeStore[_]]) {

    def getASTNode[A](d: Smart.DComp[_, _ <: Shape]): AST.Node[A, _ <: Shape] = astMap.get(d) match {
       case Some(n) => n.asInstanceOf[AST.Node[A, _ <: Shape]]
       case None    => throw new RuntimeException("Node not found in map: " + d + "\n" + astMap)
    }

    def getASTCombiner[A, B](d: Smart.DComp[_, _ <: Shape]): AST.Combiner[A, B] = astMap.get(d) match {
      case Some(n) => n.asInstanceOf[AST.Combiner[A,B]]
      case None    => throw new RuntimeException("Node not found in map: " + d + "\n" + astMap)
    }

    def getASTReducer[A, B, C, D](d: Smart.DComp[_, _ <: Shape]): AST.Reducer[A, B, C, D] = astMap.get(d) match {
      case Some(n) => n.asInstanceOf[AST.Reducer[A, B, C, D]]
      case None    => throw new RuntimeException("Node not found in map: " + d + "\n" + astMap)
    }

    def getASTGbkReducer[A, B, C, D](d: Smart.DComp[_, _ <: Shape]): AST.GbkReducer[A, B, C, D] = astMap.get(d) match {
      case Some(n) => n.asInstanceOf[AST.GbkReducer[A, B, C, D]]
      case None    => throw new RuntimeException("Node not found in map: " + d + "\n" + astMap)
    }

    def getASTFlatten[A](d: Smart.DComp[_, _ <: Shape]): AST.Flatten[A] = astMap.get(d) match {
      case Some(n) => n.asInstanceOf[AST.Flatten[A]]
      case None    => throw new RuntimeException("Node not found in map: " + d + "\n" + astMap)
    }

    def getASTGroupByKey[K, V](d: Smart.DComp[_, _ <: Shape]): AST.GroupByKey[K, V] = astMap.get(d) match {
      case Some(n) => n.asInstanceOf[AST.GroupByKey[K,V]]
      case None    => throw new RuntimeException("Node not found in map: " + d + "\n" + astMap)
    }

    def getBridgeStore(d: Smart.DComp[_, _ <: Shape]): BridgeStore[_] = {
      val n: AST.Node[_, _ <: Shape] = getASTNode(d)
      bridgeStoreMap.getOrElseUpdate(n, BridgeStore())
    }
  }


  object ConvertInfo {
    def apply(
        conf: ScoobiConfiguration,
        outMap: Map[Smart.DComp[_, _ <: Shape], Set[DataSink[_,_,_]]],
        mscrs: Iterable[Intermediate.MSCR],
        g: DGraph)
      : ConvertInfo = {

      new ConvertInfo(conf, outMap, mscrs, g, MMap.empty, MMap.empty, MMap.empty)
    }
  }
}


trait KVLike[K,V] {
  def mkTaggedIdentityMapper(tags: Set[Int]): TaggedIdentityMapper[K,V]
}
