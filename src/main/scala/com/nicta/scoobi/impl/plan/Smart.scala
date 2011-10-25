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


import com.nicta.scoobi.HadoopWritable
import com.nicta.scoobi.io.Loader
import com.nicta.scoobi.io.Persister
import com.nicta.scoobi.io.DataStore
import com.nicta.scoobi.io.DataSource
import com.nicta.scoobi.impl.exec.BridgeStore
import com.nicta.scoobi.impl.exec.TaggedIdentityMapper
import com.nicta.scoobi.impl.exec.TaggedIdentityReducer
import com.nicta.scoobi.impl.util.UniqueInt


/** Abstract syntax of tree of primitive "language". */
object Smart {

  object Id extends UniqueInt

  type CopyFn[T] = (DList[T], CopyTable) => (DList[T], CopyTable, Boolean)
  type CopyTable = Map[DList[_], DList[_]]


  /** GADT for distributed list computation graph. */
  sealed abstract class DList[A : Manifest : HadoopWritable] {

    /* We don't want structural equality */
    override def equals(arg0: Any): Boolean = eq(arg0.asInstanceOf[AnyRef])

    def name: String

    val id = Id.get

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //  Optimisation strategies:
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    /** An optimisation strategy that replicates any Flatten nodes that have multiple outputs
      * such that in the resulting AST Flatten nodes only have single outputs. */
    def optSplitFlattens(copied: CopyTable): (DList[A], CopyTable, Boolean) =
      copyOnceWith(copied, _.optSplitFlattens(_))

    /** An optimisation strategy that sinks a Flatten node that is an input to a FlatMap node
      * such that in the resulting AST replicated FlatMap nodes are inputs to the Flatten
      * node. */
    def optSinkFlattens(copied: CopyTable): (DList[A], CopyTable, Boolean) =
      copyOnceWith(copied, _.optSinkFlattens(_))

    /** An optimisation strategy that fuse any Flatten nodes that are inputs to Flatten nodes. */
    def optFuseFlattens(copied: CopyTable): (DList[A], CopyTable, Boolean) =
      copyOnceWith(copied, _.optFuseFlattens(_))

    /** An optimisation strategy that morphs a Combine node into a FlatMap node if it does not
      * follow a GroupByKey node. */
    def optCombinersToMaps(copied: CopyTable): (DList[A], CopyTable, Boolean) =
      copyOnceWith(copied, _.optCombinersToMaps(_))

    /** An optimisation strategy that fuses the functionality of a FlatMap node that is an input
      * to another FlatMap node. */
    def optFuseMaps(copied: CopyTable): (DList[A], CopyTable, Boolean) =
      copyOnceWith(copied, _.optFuseMaps(_))

    /** An optimisation strategy that replicates any GroupByKey nodes that have multiple outputs
      * such that in the resulting AST, GroupByKey nodes have only single outputs. */
    def optSplitGbks(copied: CopyTable): (DList[A], CopyTable, Boolean) =
      copyOnceWith(copied, _.optSplitGbks(_))


    /** Perform a depth-first traversal copy of the DList node. When copying the input DList
      * node(s), a the CopyFn function is used (somewhat like a callback). */
    protected def justCopy(copied: CopyTable, cf: CopyFn[_]): (DList[A], CopyTable, Boolean)

    /** A helper method that checks whether the node has already been copied, and if so returns the copy, else
      * invokes a user provided code implmenting the copy. */
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

    final def insert2[K : Manifest : HadoopWritable : Ordering,
                V : Manifest : HadoopWritable]
                (ci: ConvertInfo, n: AST.Node[(K,V)] with KVLike[K,V]):
                AST.Node[(K,V)] with KVLike[K,V] = {
      ci.astMap += ((this,n))
      n
    }

    def dataSource(ci: ConvertInfo): DataStore with DataSource = ci.getBridgeStore(this)

    final def convert(ci: ConvertInfo): AST.Node[A]  = {
      val maybeN: Option[AST.Node[_]] = ci.astMap.get(this)
      maybeN match {
        case Some(n) => n.asInstanceOf[AST.Node[A]] // Run-time cast. Shouldn't fail though.
        case None    => convertNew(ci)
      }
    }

    final def convert2[K : Manifest : HadoopWritable : Ordering,
                 V : Manifest : HadoopWritable]
                 (ci: ConvertInfo): AST.Node[(K,V)] with KVLike[K,V]  = {
      val maybeN: Option[AST.Node[_]] = ci.astMap.get(this)
      maybeN match {
        case Some(n) => n.asInstanceOf[AST.Node[(K,V)] with KVLike[K,V]] // Run-time cast. Shouldn't fail though.
        case None    => convertNew2(ci)
      }
    }


    def convertNew(ci: ConvertInfo): AST.Node[A]

    def convertNew2[K : Manifest : HadoopWritable : Ordering,
                    V : Manifest : HadoopWritable]
                    (ci: ConvertInfo): AST.Node[(K,V)] with KVLike[K,V]

    def convertFlatMap[B : Manifest : HadoopWritable](ci: ConvertInfo, fm: FlatMap[A,B]): AST.Node[B] = {
      val n: AST.Node[A] = convert(ci)
      fm.insert(ci, AST.Mapper(n, fm.f))
    }
  }


  /** The Load node type specifies the materialization of a DList. A Loader object specifies how
    * the materialization is performed. */
  case class Load[A : Manifest : HadoopWritable](loader: Loader[A]) extends DList[A] {

    def name = "Load" + id

    override def toString = name

    // Optimisation
    // ~~~~~~~~~~~~
    def justCopy(copied: CopyTable, cf: CopyFn[_]): (DList[A], CopyTable, Boolean) =
      (this, copied, false)


    // Conversion
    // ~~~~~~~~~~
    def convertNew(ci: ConvertInfo) = {
      insert(ci, AST.Load())
    }

    def convertNew2[K : Manifest : HadoopWritable : Ordering,
                    V : Manifest : HadoopWritable]
                    (ci: ConvertInfo): AST.Node[(K,V)] with KVLike[K,V] = {

      insert2(ci, new AST.Load[(K,V)]() with KVLike[K,V] {
                    def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K,V](tags)})
    }

    override def dataSource(ci: ConvertInfo): DataStore with DataSource =
      loader.mkInputStore(ci.getASTNode(this).asInstanceOf[AST.Load[A]])
  }


  /** The FlatMap node type specifies the building of a DList as a result of applying a function to
    * all elements of an existing DList and concatenating the results. */
  case class FlatMap[A, B]
      (in: DList[A],
       f: A => Iterable[B])
      (implicit val mA: Manifest[A], val wtA: HadoopWritable[A], mB: Manifest[B], wtB: HadoopWritable[B])
    extends DList[B] {

    def name = "FlatMap" + id

    override def toString = name + "(" + in + ")"

    // Optimisation
    // ~~~~~~~~~~~~
    def justCopy(copied: CopyTable, cf: CopyFn[_]): (DList[B], CopyTable, Boolean) = {
      val cfA = cf.asInstanceOf[CopyFn[A]]
      val (inUpd, copiedUpd, b) = cfA(in, copied)
      val fm = FlatMap(inUpd, f)
      (fm, copiedUpd + (this -> fm), b)
    }

    /** If the input to this FlatMap is a Flatten node, re-write the tree as a Flatten node with the
      * FlatMap replicated on each of its inputs. Otherwise just perform a normal copy. */
    override def optSinkFlattens(copied: CopyTable): (DList[B], CopyTable, Boolean) = copyOnce(copied) {
      in match {
        case Flatten(ins) => {
          val (copiedUpd, insUpd) = ins.foldLeft((copied, Nil: List[DList[A]])) { case ((ct, copies), in) =>
            val (inUpd, ctUpd, _) = in.optSinkFlattens(ct)
            (ctUpd + (in -> inUpd), copies :+ inUpd)
          }

          val flat: DList[B] = Flatten(insUpd.map(FlatMap(_, f)))
          (flat, copiedUpd + (this -> flat), true)
        }
        case _            => justCopy(copied, (n: DList[A], ct: CopyTable) => n.optSinkFlattens(ct))
      }
    }

    /** If the input to this FlatMap is another FlatMap node, re-write this FlatMap with the preceeding's
      * FlatMap's "mapping function" fused in. Otherwise just perform a normal copy. */
    override def optFuseMaps(copied: CopyTable): (DList[B], CopyTable, Boolean) = copyOnce(copied) {

      /* Create a new FlatMap function that is the fusion of two connected FlatMap functions. */
      def fuse[X, Y, Z](f: X => Iterable[Y], g: Y => Iterable[Z]): X => Iterable[Z] =
        (x: X) => f(x) flatMap g

      in match {
        case FlatMap(_, _) => {
          val (inUpd, copiedUpd, _) = in.optFuseMaps(copied)
          val prev@FlatMap(inPrev, fPrev) = inUpd
          val fm = new FlatMap(inPrev, fuse(fPrev, f))(prev.mA, prev.wtA, mB, wtB)
          (fm, copiedUpd + (this -> fm), true)
        }
        case _             => justCopy(copied, (n: DList[A], ct: CopyTable) => n.optFuseMaps(ct))
      }
    }


    // Conversion
    // ~~~~~~~~~~
    def convertNew(ci: ConvertInfo): AST.Node[B] = {
      in.convert(ci)
      in.convertFlatMap(ci, this)
    }

    def convertNew2[K : Manifest : HadoopWritable : Ordering,
                    V : Manifest : HadoopWritable](ci: ConvertInfo):
                    AST.Node[(K,V)] with KVLike[K,V] = {
      val fm: FlatMap[A,(K,V)] = this.asInstanceOf[FlatMap[A,(K,V)]]
      val n: AST.Node[A] = fm.in.convert(ci)

      if ( ci.mscrs.exists(_.containsGbkMapper(fm)) ) {
        fm.insert2(ci, new AST.GbkMapper(n,fm.f) with KVLike[K,V] {
          def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K,V](tags)})
      } else {
        fm.insert2(ci, new AST.Mapper(n, fm.f) with KVLike[K,V] {
          def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K,V](tags)})
      }
    }
  }


  /** The GroupByKey node type specifies the building of a DList as a result of partitioning an exiting
    * key-value DList by key. */
  case class GroupByKey[K : Manifest : HadoopWritable : Ordering,
                        V : Manifest : HadoopWritable]
      (in: DList[(K, V)])
    extends DList[(K, Iterable[V])] {

    def name = "GroupByKey" + id

    override def toString = name + "(" + in + ")"

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

    def convertAux[A: Manifest : HadoopWritable : Ordering,
              B: Manifest : HadoopWritable]
              (ci: ConvertInfo, d: DList[(A,B)]):
              AST.Node[(A, Iterable[B])] with KVLike[A, Iterable[B]] = {
         insert2(ci, new AST.GroupByKey[A,B](d.convert2(ci)) with KVLike[A, Iterable[B]] {
           def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[A,Iterable[B]](tags)})
      }

    def convertNew2[K1 : Manifest : HadoopWritable : Ordering,
                    V1 : Manifest : HadoopWritable]
                    (ci: ConvertInfo): AST.Node[(K1,V1)] with KVLike[K1,V1] = {
      convertAux(ci, in).asInstanceOf[AST.Node[(K1,V1)] with KVLike[K1,V1]]


    }

    override def convertFlatMap[B : Manifest : HadoopWritable]
                               (ci: ConvertInfo, fm: FlatMap[(K,Iterable[V]), B]): AST.Node[B] = {
      val n: AST.Node[(K, Iterable[V])] = convert(ci)
      if ( ci.mscrs.exists(_.containsGbkReducer(fm)) ) {
        fm.insert(ci, AST.GbkReducer(n, fm.f))
      } else {
        fm.insert(ci, AST.Mapper(n, fm.f))
      }
    }
  }


  /** The Combine node type specifies the building of a DList as a result of applying an associative
    * function to the values of an existing key-values DList. */
  case class Combine[K : Manifest : HadoopWritable : Ordering,
                     V : Manifest : HadoopWritable]
      (in: DList[(K, Iterable[V])],
       f: (V, V) => V)
    extends DList[(K, V)] {

    def name = "Combine" + id

    override def toString = name + "(" + in + ")"

    // Optimisation
    // ~~~~~~~~~~~~
    def justCopy(copied: CopyTable, cf: CopyFn[_]): (DList[(K, V)], CopyTable, Boolean) = {
      val cfKIV = cf.asInstanceOf[CopyFn[(K, Iterable[V])]]
      val (inUpd, copiedUpd, b) = cfKIV(in, copied)
      val comb = Combine(inUpd, f)
      (comb, copiedUpd + (this -> comb), b)
    }

    /** If this Combine node's input is a GroupByKey node, perform a normal copy. Otherwise, create a
      * FlatMap node whose mapping function performs the combining functionality. */
    override def optCombinersToMaps(copied: CopyTable): (DList[(K, V)], CopyTable, Boolean) = copyOnce(copied) {
      in match {
        case GroupByKey(inKV) => justCopy(copied, (n: DList[(K, V)], ct: CopyTable) => n.optCombinersToMaps(ct))
        case _                => {
          val (inUpd, copiedUpd, _) = in.optCombinersToMaps(copied)
          val mapF = (kvs: (K, Iterable[V])) => {
            val key = kvs._1
            val values = kvs._2
            List((key, values.tail.foldLeft(values.head)(f)))
          }
          val fm = FlatMap(inUpd, mapF)
          (fm, copiedUpd + (this -> fm), true)
        }
      }
    }


    // Conversion
    // ~~~~~~~~~~
    def convertNew(ci: ConvertInfo) = insert(ci, AST.Combiner(in.convert(ci), f))

    /* An almost exact copy of convertNew */
    def convertNew2[K1 : Manifest : HadoopWritable : Ordering,
                    V1 : Manifest : HadoopWritable]
                    (ci: ConvertInfo): AST.Node[(K1,V1)] with KVLike[K1,V1] = {
       val c: Combine[K1,V1] = this.asInstanceOf[Combine[K1,V1]]

       insert2(ci, new AST.Combiner[K1,V1](c.in.convert2(ci), c.f) with KVLike[K1,V1] {
          def mkTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper[K1,V1](tags)})

    }

    override def convertFlatMap[B : Manifest : HadoopWritable]
                               (ci: ConvertInfo, fm: FlatMap[(K,V), B]): AST.Node[B] = {
      val n: AST.Node[(K, V)] = convert(ci)
      if ( ci.mscrs.exists(_.containsReducer(fm)) ) {
        fm.insert(ci, AST.Reducer(n, fm.f))
      } else {
        fm.insert(ci, AST.Mapper(n, fm.f))
      }
    }
  }


  /** The Flatten node type spcecifies the building of a DList that contains all the elements from
    * one or more exsiting DLists of the same type. */
  case class Flatten[A : Manifest : HadoopWritable](ins: List[DList[A]]) extends DList[A] {

    def name = "Flatten" + id

    override def toString = name + "([" + ins.mkString(",") + "])"

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

    def convertNew2[K : Manifest : HadoopWritable : Ordering,
                    V : Manifest : HadoopWritable]
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
    *   - morphing CombineValues into FlatMaps when they don't follow a GroupByKey
    *   - fusing of FlatMaps
    *   - splitting of GroupByKeys with multiple outputs. */
  def optimisePlan(outputs: List[DList[_]]): List[DList[_]] = {

    /** Perform a computation 'f' recursively stating with input 'x'. Return the
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
      val (flattenOpt, c1)       = untilChanged(in, flattenStrategies)
      val (combinerToMapOpt, c2) = untilChanged(flattenOpt, travOnce(_.optCombinersToMaps(_)))
      val (flatMapOpt, c3)       = untilChanged(combinerToMapOpt, travOnce(_.optFuseMaps(_)))
      val (splitGbkOpt, c4)      = untilChanged(flatMapOpt, travOnce(_.optSplitGbks(_)))
      (splitGbkOpt, c1 || c2 || c3 || c4)
    }

    val (optOutputs, _) = untilChanged(outputs, allStrategies)
    optOutputs
  }


  /** Helper method for traversing the graph from */
  private def travOnce
      (fuseFn: (DList[_], CopyTable) => (DList[_], CopyTable, Boolean))
      (outputs: List[DList[_]])
    : (List[DList[_]], Boolean) = {

    val emptyCopyTable: CopyTable = Map()
    val noCopies: List[DList[_]] = Nil

    val (_, copies, changes) = outputs.foldLeft((emptyCopyTable, noCopies, false)) { case ((ct, cps, b), n) =>
      val (nCopy, ctUpd, bb) = fuseFn(n, ct)
      (ctUpd + (n -> nCopy), cps :+ nCopy, bb || b)
    }

    (copies, changes)
  }


  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Graph query helper functions:
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  def parentsOf(d: DList[_]): List[DList[_]] = {
    d match {
      case Load(_)        => List()
      case FlatMap(in,_)  => List(in)
      case GroupByKey(in) => List(in)
      case Combine(in,_)  => List(in)
      case Flatten(ins)   => ins
    }
  }

  def getFlatMap(d: DList[_]): Option[FlatMap[_,_]] = {
    d match {
      case flatMap@FlatMap(_,_) => Some(flatMap)
      case _                    => None
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

  def isFlatMap(d: DList[_]):    Boolean = getFlatMap(d).isDefined
  def isFlatten(d: DList[_]):    Boolean = getFlatten(d).isDefined
  def isGroupByKey(d: DList[_]): Boolean = getGroupByKey(d).isDefined
  def isCombine(d: DList[_]):    Boolean = getCombine(d).isDefined


  /*
   * Class that maintains state while the Smart.DList abstract syntax tree
   * is transformed to an AST.Node abstract syntax tree.
   *
   * Contains mutable maps. Beware, many uses of this data structure may look pureley
   * functional but aren't.
   *
   */
  class ConvertInfo(val outMap: Map[Smart.DList[_], Set[Persister[_]]],
                    val mscrs: Iterable[Intermediate.MSCR],
                    val g: DGraph,
                    val astMap: MMap[Smart.DList[_], AST.Node[_]],
                    /* A map of AST nodes to BridgeStores*/
                    val bridgeStoreMap: MMap[AST.Node[_], BridgeStore]
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

    def getBridgeStore(d: Smart.DList[_]): BridgeStore = {
      val n: AST.Node[_] = getASTNode(d)
      bridgeStoreMap.get(n) match {
        case Some(bs) => bs
        case None     => {
          val newBS: BridgeStore = BridgeStore(n)
          bridgeStoreMap += ((n, newBS))
          newBS
        }
      }
    }


  }

  object ConvertInfo {
    def apply(outMap: Map[Smart.DList[_], Set[Persister[_]]],
              mscrs: Iterable[Intermediate.MSCR],
              g: DGraph): ConvertInfo = {
      new ConvertInfo(outMap, mscrs, g, MMap(), MMap())
    }
  }
}


trait KVLike[K,V] {
  def mkTaggedIdentityMapper(tags: Set[Int]): TaggedIdentityMapper[K,V]
}
