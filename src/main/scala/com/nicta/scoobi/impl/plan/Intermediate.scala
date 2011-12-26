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

import com.nicta.scoobi.io.DataStore
import com.nicta.scoobi.io.DataSource
import com.nicta.scoobi.io.DataSink
import com.nicta.scoobi.impl.plan.Smart._
import com.nicta.scoobi.impl.util.Pretty
import com.nicta.scoobi.impl.exec.MapperLike

/*
 * Import renamings. (TODO: Perhaps wrap the final MSCR data structures in a namespace?)
 *
 * The C suffix on CGbkOutputChannel, etc, is for converted.
 */
import com.nicta.scoobi.impl.plan.{GbkOutputChannel    => CGbkOutputChannel,
                                   BypassOutputChannel => CBypassOutputChannel,
                                   MapperInputChannel  => CMapperInputChannel,
                                   StraightInputChannel => CStraightInputChannel,
                                   FlattenOutputChannel => CFlattenOutputChannel,
                                   InputChannel        => CInputChannel,
                                   OutputChannel       => COutputChannel,
                                   MSCR                => CMSCR }


/*
 * === A high-level overview of MSCR conversion ===
 *
 * This extended comment describes the conversion from the DList data structure
 * to MSCRs containing nodes of type AST.Node. This is a multi-step process that goes
 * as follows:
 *
 * (FIXME:clarify w.r.t. DList.persist method )

 * 1. Creation of a DGraph data structure by introspecting the structure of the
 *    Smart.DList abstract syntax tree. (Class DList provides syntactic sugar for the real
 *    abstract syntax tree of type Smart.DList).
 * 2. Creation of an intermediate MSCRGraph data structure (Intermediate.MSCRGraph).
 *     It requires the Smart.DList abstract syntax tree and DGraph data structures as inputs.
 *
 *
 * 3. Conversion from the Smart.DList abstract syntax to the AST.Node abstract syntax tree.
 * 4. Conversion from intermediate MSCRGraph to the final MSCRGraph (defined in MSCR.scala)
 *
 * Step 1 is required by Step 2. In order to easily discover the nodes that go into the various
 * MSCRs that make up the Intermediate.MSCRGraph a graph data structure (DGraph) is very useful.
 * One needs to be able traverse both forwards and backwards within the Smart.DList abstract syntax
 * tree in order to find out such things as:
 *   - which GroupByKey nodes are related.
 *   - for a GroupByKey what its successors are in order to create output channels of an MSCR.
 *   - for a GroupByKey what its predecessors are in order to create input channels of an MSCR.
 *
 * Step 2 can be thought of as "drawing boxes around" Smart.DList nodes. Intermediate MSCRs
 * (and indeed MSCRs proper) contain input and output channels and these channels contain
 * references to Smart.DList nodes. All Smart.DList nodes in the abstract syntax tree should be
 * part of exactly one MSCR in the Intermediate.MSCRGraph that is the output of this step
 * (i.e. in a "box") This is not guaranteed by construction. The implementation must be
 * carefully written to guarantee this.
 *
 * Step 3 converts from Smart.DList nodes to AST.Node nodes. In the process *extra* type information
 * is recovered. For example, if one is currently on a GroupByKey node then one knows that its
 * predecessor must have output type (K,V) for some key K and value V.
 *
 * Depending on the position for a Smart.ParallelDo node in the abstract syntax tree it could end up
 * being converted to one of the follow AST.Node node types: AST.Mapper, AST.GbkMapper,
 * AST.Combiner, AST.GbkReducer.
 *
 * Step 4 creates the final MSCR data structures (and puts them in an MSCRGraph). During this
 * phase DataStore data structures are created for each input and output channel of the MSCRs.
 * These provide all the information for the Hadoop back-end about where input comes from,
 * which outputs of MSCRs are intermediate data structures (to be consumed only by other MSCRs)
 * and which outputs are written to disk eventually.
 *
 * === Creating Intermediate.MSCR data structures ===
 *
 * The Flume paper defines the notion of _related GroupByKey_ nodes. Two GroupByKey nodes
 * are related if they consume the same input (possibly via a Flatten node). A collection
 * of GroupByKey nodes are related if for each GroupByKey node, n, in the collection, n is related
 * to at least one other GroupByKey node in the collection. See the comments for findRelated.
 */
object Intermediate {

  /*
   *  Abstract InputChannel class.
   *
   *  The methods @hasInput@, @hasOutput@, @containsGbkMapper@, @dataStoreInput@ and
   *  @convert@ methods are all used during Step 4 of conversion.
   *  (See "A high-level overview of MSCR conversion" above)
   */
  sealed abstract class InputChannel {

    def hasInput(d: DList[_]): Boolean

    def hasOutput(d: DList[_]): Boolean

    /*
     * Returns @true@ if this input channel contains a @Smart.ParallelDo@ node
     * that should be converted to a @AST.GbkMapper@ node.
     */
    def containsGbkMapper(d: Smart.ParallelDo[_,_]): Boolean

    /*
     * Creates the @DataStore@ input for this input channel.
     */
    def dataStoreInput(ci: ConvertInfo): DataStore with DataSource

    /*
     * Converts this intermediate input channel into a final input channel (defined in MSCR.scala)
     */
    def convert(ci: ConvertInfo): CInputChannel

  }

  case class MapperInputChannel(parDos: List[ParallelDo[_,_]]) extends InputChannel {

    override def toString = "MapperInputChannel([" + parDos.mkString(", ") + "])"

    /*
     * The methods @hasInput@, @hasOutput@, @containsGbkMapper@, @dataStoreInput@ and
     *  @convert@ methods are all used during Step 4 of conversion.
     *  (See "A high-level overview of MSCR conversion" above)
     *
     *  See descriptions of these methods in super class @InputChannel@
     */
    def hasInput(d: DList[_]): Boolean = parDos(0).in ==d

    def hasOutput(d: DList[_]): Boolean = parDos.exists(_==d)

    def containsGbkMapper(d: ParallelDo[_,_]) = parDos.exists{_ == d}

    def dataStoreInput(ci: ConvertInfo): DataStore with DataSource = {
      // This should be safe since there should be at least one parallelDo in @parDos@
      parDos(0).in.dataSource(ci)
    }

    def convert(ci: ConvertInfo): CMapperInputChannel[DataStore with DataSource] = {
      // TODO: Yet another asInstanceOf. Don't like them.
      def f(d: DList[_]): AST.Node[_] with MapperLike[_,_,_] =
        ci.getASTNode(d).asInstanceOf[AST.Node[_] with MapperLike[_,_,_]]
      val ns: Set[AST.Node[_] with MapperLike[_,_,_]] = parDos.map(f).toSet
      CMapperInputChannel(dataStoreInput(ci), ns)
    }

  }

  case class IdInputChannel(input: DList[_]) extends InputChannel {

    override def toString = "IdInputChannel("+ input + ")"
    /*
     * The methods @hasInput@, @hasOutput@, @containsGbkMapper@, @dataStoreInput@ and
     *  @convert@ methods are all used during Step 4 of conversion.
     *  (See "A high-level overview of MSCR conversion" above)
     *
     *  See descriptions of these methods in super class @InputChannel@
     */
    def hasInput(d: DList[_]): Boolean = d == input

    def hasOutput(d: DList[_]): Boolean = hasInput(d)

    def containsGbkMapper(d: ParallelDo[_,_]) = d == input

    def convert(ci: ConvertInfo): BypassInputChannel[DataStore with DataSource] = {
      // TODO. Yet another asInstanceOf
      BypassInputChannel(dataStoreInput(ci), ci.getASTNode(input)
                                               .asInstanceOf[AST.Node[_] with KVLike[_,_]])
    }

    def dataStoreInput(ci: ConvertInfo): DataStore with DataSource = input.dataSource(ci)

  }

  case class StraightInputChannel(input: DList[_]) extends InputChannel {
    override def toString = "StraightInputChannel(" + input + ")"
    override def hasInput(d: DList[_]): Boolean = input == d
    override def hasOutput(d: DList[_]): Boolean = input == d
    override def containsGbkMapper(d: ParallelDo[_,_]) = false
    override def convert(ci: ConvertInfo): CStraightInputChannel[DataStore with DataSource] =
      CStraightInputChannel(dataStoreInput(ci), ci.getASTNode(input).asInstanceOf[AST.Node[_]])
    override def dataStoreInput(ci: ConvertInfo): DataStore with DataSource =
      input.dataSource(ci)
  }

  /*
   * Abstract OutputChannel class.
   *
   *  The methods @hasInput@, @hasOutput@, @containsGbkMapper@, @dataStoreOutputs@ and
   *  @convert@ methods are all used during Step 4 of conversion.
   *  (See "A high-level overview of MSCR conversion" above)
   */
  sealed abstract class OutputChannel {

    /*
     * Returns the @Smart.DList@ output for this output channel.
     */
    def output: DList[_]


    def hasInput(d: DList[_]): Boolean
    def hasOutput(d: DList[_]): Boolean

    /*
     * Creates the @DataStore@ outputs for this channel.
     *
     * This method relies on the @bridgeStoreMap@ attribute of the given @ConvertInfo@
     * parameter. It ensures that for each output @AST.Node@ of the converted
     * output channel there is at most one @BridgeStore@ object.
     */
    final def dataStoreOutputs(parentMSCR: MSCR, ci: ConvertInfo): Set[DataStore with DataSink] = {
      val d: DList[_] = this.output
      val bridgeStores:Set[DataStore with DataSink] =
        if ( parentMSCR.connectsToOtherMSCR(d, ci.mscrs) ) {
          Set(ci.getBridgeStore(d))
        } else {
          Set()
        }

      val outputStores: Set[DataStore with DataSink] = ci.outMap.get(d) match {
        case Some(persisters) => persisters.map(_.mkOutputStore(ci.getASTNode(d)))
        case None             => Set()
      }

      bridgeStores ++ outputStores
    }


    /*
     * Converts this intermediate output channel into a final output channel (defined in MSCR.scala)
     */
    def convert(parentMSCR: MSCR, ci: ConvertInfo): COutputChannel
  }



  object GbkOutputChannel {

    /*
     * Given a @Smart.GroupByKey@ node creates a new (incomplete) intermediate @GbkOutputChannel@.
     */
    def apply(d: DList[_]): GbkOutputChannel = {
      d match {
        case gbk@GroupByKey(_) => GbkOutputChannel(None, gbk, None, None)
        case _                 => throw new RuntimeException("This is not a GBK")
      }
    }
  }

  /*
   * A @GbkOutputChannel@ is the standard output channel of an MSCR.
   *
   * They always contains a @GroupByKey@ node. Optionally they are preceded by a
   * @Flatten@ node, and optionally succeeded by @Combine@ and/or @ParallelDo@ node.
   */
  case class GbkOutputChannel(flatten:    Option[Flatten[_]],
                              groupByKey: GroupByKey[_,_],
                              combiner:   Option[Combine[_,_]],
                              reducer:    Option[ParallelDo[_,_]]) extends OutputChannel {

     /*
      * Adds a @Flatten@ node to this output channel returning a new channel.
      */
     def addFlatten(flatten: Flatten[_]): GbkOutputChannel =
       new GbkOutputChannel(Some(flatten), this.groupByKey, this.combiner, this.reducer)

     /*
      * Adds a @ParallelDo@ node to this output channel returning a new channel.
      */
     def addReducer(reducer: ParallelDo[_,_]): GbkOutputChannel =
       new GbkOutputChannel(this.flatten, this.groupByKey, this.combiner, Some(reducer))

     /*
      * Adds a @Combine@ node to this output channel returning a new channel.
      */
     def addCombiner(combiner: Combine[_,_]): GbkOutputChannel =
       new GbkOutputChannel(this.flatten, this.groupByKey, Some(combiner), this.reducer)


     def hasInput(d: DList[_]): Boolean = flatten match {
       case Some(f) => f.ins.exists(_ == d)
       case None    => groupByKey.in == d
     }

     def hasOutput(d: DList[_]): Boolean = output == d

     /*
      * Find the inputs to this channel. If there is a Flatten node then it is the parents of this
      * node. Otherwise it is the parent of the GroupByKey node
      *
      * TODO: This could just as easily have been done by pattern matching on the nodes.
      * In fact, predecessors in the DGraph data structure are only there for
      * convenience rather than necessity.
      */
     def inputs(g: DGraph): Iterable[DList[_]] = {
       (this.flatten match {
         case Some(fltn) => g.preds.getOrElse(fltn,
           throw new RuntimeException("Flatten can't have no parents in GbkOutputChannel"))
         case None => g.preds.getOrElse(this.groupByKey,
           throw new RuntimeException("GroupByKey can't have no parents in GbkOutputChannel"))
       }) toList
     }

    override def toString = {
      val header = "GbkOutputChannel("

      List(Pretty.indent("flatten: ",flatten.toString),
           "groupByKey: " + groupByKey.toString,
           "combiner: " + combiner.toString,
           "reducer: " + reducer.toString).
        mkString(header,",\n" + " " * header.length , ")")
      }

    def output = reducer match {
      case Some(r) => r
      case None => combiner match {
        case Some(c) => c
        case None => groupByKey
      }
    }

    def convert(parentMSCR: MSCR, ci: ConvertInfo): CGbkOutputChannel[DataStore with DataSink] = {
      val crPipe:CRPipe = combiner match {
        case Some(c) => {
          val nc: AST.Combiner[_,_] = ci.getASTCombiner(c)
          reducer match {
            case Some(r) => CombinerReducer(nc, ci.getASTReducer(r))
            case None    => JustCombiner(nc)
          }
        }
        case None    => {
          reducer match {
            case Some(r) => JustReducer(ci.getASTGbkReducer(r))
            case None    => Empty
          }
        }
      }
      val fltn: Option[AST.Flatten[_]] = flatten.map{ci.getASTFlatten(_)}
      val gbk: AST.GroupByKey[_,_]     = ci.getASTGroupByKey(groupByKey)
      val outputs: Set[DataStore with DataSink] = dataStoreOutputs(parentMSCR, ci)

      CGbkOutputChannel(outputs, fltn, gbk, crPipe)
    }
  }

  case class BypassOutputChannel(input: ParallelDo[_,_]) extends OutputChannel {
    def hasInput(d: DList[_]) = d == input
    def hasOutput(d: DList[_]) = d == input

    override def toString = "BypassOutputChannel(" + input.toString + ")"

    def output: DList[_] = input

    def convert(parentMSCR: MSCR, ci: ConvertInfo): CBypassOutputChannel[DataStore with DataSink] = {
      val n = ci.getASTNode(input)
      if (n.isInstanceOf[AST.GbkMapper[_,_,_]])
        CBypassOutputChannel(dataStoreOutputs(parentMSCR, ci), n.asInstanceOf[AST.GbkMapper[_,_,_]])
      else if (n.isInstanceOf[AST.Mapper[_,_]])
        CBypassOutputChannel(dataStoreOutputs(parentMSCR, ci), n.asInstanceOf[AST.Mapper[_,_]])
      else
        throw new RuntimeException("Expecting GbkMapper or Mapper node.")
    }
  }

  case class FlattenOutputChannel(input: Flatten[_]) extends OutputChannel {
    override def hasInput(d: DList[_]) = input.ins.exists(_ == d)
    override def hasOutput(d: DList[_]) = d == output
    override def toString = "MultiOutputChannel(" + input.toString + ")"
    override def output: DList[_] = input
    override def convert(parentMSCR: MSCR, ci: ConvertInfo): CFlattenOutputChannel[DataStore with DataSink] = {
      CFlattenOutputChannel(dataStoreOutputs(parentMSCR, ci), ci.getASTNode(input).asInstanceOf[AST.Flatten[_]])
    }
  }


  class MSCR(val inputChannels: Set[InputChannel], val outputChannels: Set[OutputChannel]) {

    def hasInput(d: DList[_]): Boolean = this.inputChannels.exists(_.hasInput(d))
    def hasOutput(d: DList[_]): Boolean = this.outputChannels.exists(_.hasOutput(d))

    override def toString = {
      Pretty.indent(
        "MSCR(",
          List(Pretty.indent("inputChannels:  { ", inputChannels.mkString(",\n")) + "}",
               Pretty.indent("outputChannels: { ", outputChannels.mkString(",\n")) + "}").
          mkString(",\n"))
    }

    /* Used during the translation from Smart.DList to AST */
    def containsGbkMapper(d: Smart.ParallelDo[_,_]): Boolean =
      inputChannels.map{_.containsGbkMapper(d)}.exists(identity)

    /* Used during the translation from Smart.DList to AST */
    def containsGbkReducer(d: Smart.ParallelDo[_,_]): Boolean = {
      def pred(oc: OutputChannel): Boolean = oc match {
        case BypassOutputChannel(_) => false
        case FlattenOutputChannel(_) => false
        case gbkOC@GbkOutputChannel(_,_,_,_) =>
          gbkOC.combiner.isEmpty && gbkOC.reducer.map{_ == d}.getOrElse(false)
      }
      outputChannels.exists(pred)
    }

    /* Used during the translation from Smart.DList to AST */
    def containsReducer(d: Smart.ParallelDo[_,_]): Boolean = {
      def pred(oc: OutputChannel): Boolean = oc match {
        case BypassOutputChannel(_) => false
        case gbkOC@GbkOutputChannel(_,_,_,_) =>
          gbkOC.combiner.isDefined && gbkOC.reducer.map{_ == d}.getOrElse(false)
        case FlattenOutputChannel(_) => false
      }
      outputChannels.exists(pred)
    }

    /*
     * Checks whether a given node is an output from this MSCR and is input to another another.
     * The parameter @mscrs@ may or may not include this MSCR
     */
    def connectsToOtherMSCR(d: DList[_], mscrs: Iterable[MSCR]) =
      mscrs.exists(_.hasInput(d)) && !hasInput(d) && hasOutput(d)

    def convert(ci: ConvertInfo): CMSCR = {
      val cInputChannels:  Set[CInputChannel]  = inputChannels.map{_.convert(ci)}
      val cOutputChannels: Set[COutputChannel] = outputChannels.map{_.convert(this,ci)}
      CMSCR(cInputChannels, cOutputChannels)
    }
  }


  class MSCRGraph(val mscrs: Iterable[MSCR], val g: DGraph)

  object MSCRGraph {

    /** Construct multiple MSCRs for the logical plan/graph represented by a set of distributed
      * lists. */
    def apply(outputs: Iterable[DList[_]]): MSCRGraph = {

      val g = DGraph(outputs)

      /* Step1.
       *
       * Construct MSCRs that are based on related GroupByKey nodes. First group together GroupByKeys
       * that are considered "related", then for each group, construct an MSCR with input and output
       * channels. */
      val gbkMSCRs = relatedNodes(g) map { related =>

        /* Create a GbkOutputChannel for each GroupByKey and the related group. */
        val gbkOCs = related.gbks map { outputChannelForGbk(_, g) }

        /* Create a MapperInputChannel for each group of ParallelDo nodes, "belonging" to this
         * set of related GBKs, that share the same input. */
        val pdos = related.pdos map { getParallelDo(_).orNull }
        val mapperICs = pdos.toList.groupBy { case ParallelDo(i, _) => i }
                                   .values
                                   .map { MapperInputChannel(_) }
                                   .toSet

        /* Create an IdInputChannel for any GbkOutputChannel's input that is not in the set of
         * related ParallelDos. */
        val allInputs = gbkOCs.flatMap(_.inputs(g))
        val idICs = (allInputs -- related.pdos) map { IdInputChannel }

        /* Create a BypassOutputChannel for a related ParallelDo if the number of its successors
         * (consumers) is greater than the number of related GbkOutputChannels that are successors. */
        val bypassOCs = pdos filter { pdo =>
          val numConsumers = g.succs(pdo).size
          val numOCs = gbkOCs filter { _.inputs(g) exists (pdo ==) } size
          val reqBypass = numConsumers > numOCs
          reqBypass
        } map (BypassOutputChannel(_))

        /* The MSCR for this set of related GroupByKeys. */
        new MSCR(mapperICs ++ idICs, gbkOCs ++ bypassOCs)
      }


      /* Step 2.
       *
       * Construct an MSCR for any nodes not included by the MSCRs constructed from related
       * GroupByKey nodes.
       *
       * Find all outputs that are not within a GBK MSCR. There are 2 cases:
       *
       *   1. The ouput is a ParallelDo node connected directly to a Load node;
       *   2. The output is a Flatten node, connected to the output(s) of an MSCR and/or
       *      Load node(s). */
      val floatingOutputs = outputs filterNot { o => gbkMSCRs.exists(mscr => mscr.hasOutput(o)) }

      val allMSCRs = if (floatingOutputs.isEmpty) gbkMSCRs else {
        val (ics, ocs) = floatingOutputs map {
          case pd@ParallelDo(_, _) => (List(MapperInputChannel(List(pd))), BypassOutputChannel(pd))
          case flat@Flatten(_)     => {
            (flat.ins map {
              case pd@ParallelDo(_,_) => MapperInputChannel(List(pd))
              case other => StraightInputChannel(other)
            }, FlattenOutputChannel(flat))
          }
          case node => sys.error("Not expecting " + node + " as remaining node.")
        } unzip

        gbkMSCRs :+ new MSCR(ics.flatten.toSet, ocs.toSet)
      }

      /* Final MSCR graph contains both GBK MSCRs and Map-only MSCRs. */
      new MSCRGraph(allMSCRs, g)
    }


    /* The primary data structure for determining related GroupByKey and ParallelDo nodes. */
    type DListSet = Set[DList[_]]
    private case class Relation(gbks: DListSet, pdos: DListSet, pdoInputs: DListSet, dependentGbks: DListSet) {

      /** Field-wise union of two relations. */
      def union(that: Relation) =
        Relation(gbks.union(that.gbks),
                 pdos.union(that.pdos),
                 pdoInputs.union(that.pdoInputs),
                 dependentGbks.union(that.dependentGbks))
    }


    /** Find the GroupByKey nodes, and associated ParallelDo nodes, that are related for the
      * purpose of constructing a single MSCR from them. This is a four step process:
      *
      *   1.  For each GroupByKey node in the graph, create a Relation object;
      *   2.  Merge Relation objects that have common ParallelDo nodes but no GroupByKey
      *       dependencies;
      *   3.  Merge Relation objects that have common ParallelDo node inputs but no GroupByKey
      *       dependencies;
      *   4.  Prune from Relation objects ParallelDo nodes and ParallelDo node inputs that are
      *       are present in dependent Relation objects.
      */
    private def relatedNodes(g: DGraph): List[Relation] = {

      /* Step 1. */
      def gbkInputs(gbk: DList[_]): Relation = {

        def parallelDos(dlist: DList[_]): Set[DList[_]] = {
          dlist match {
            case Flatten(ds)      => ds.flatMap(parallelDos(_)).toSet
            case ParallelDo(_, _) => Set(dlist)
            case _                => Set()
          }
        }

        def parallelDoInputs(dlist: DList[_]): Set[DList[_]] = {
          dlist match {
            case Flatten(ds)     => ds.filter(isParallelDo).flatMap(parallelDoInputs(_)).toSet
            case ParallelDo(d,_) => Set(d)
            case _               => Set()
          }
        }

        def dependentGbks(dlist: DList[_]): Set[DList[_]] = dlist match {
          case Load(_)            => Set.empty
          case ParallelDo(in, _)  => dependentGbks(in)
          case gbk@GroupByKey(in) => Set(gbk) ++ dependentGbks(in)
          case Combine(in, _)     => dependentGbks(in)
          case Flatten(ins)       => ins.map(dependentGbks(_).toList).flatten.toSet
        }

        val (GroupByKey(d)) = gbk
        Relation(Set(gbk), parallelDos(d), parallelDoInputs(d), dependentGbks(d))
      }

      /* Step 2 + 3.
       * Merge a list of relations into a minimal list based on an intersection predicate.
       */
      def merge(p: (Relation, Relation) => Boolean)(rs: List[Relation]): List[Relation] = {

        def oneStepMerge(r: Relation, rs: List[Relation]): (Relation, List[Relation]) = {
          val (overlapped, disjoint) = rs.partition(p(r, _))
          val newPair = overlapped.foldLeft(r)(_.union(_))
          (newPair, disjoint)
        }

        rs match {
          case r :: rs => {
            val (r_, rs_) = oneStepMerge(r, rs)
            if (r == r_) // Fixed point reached. No more relatedness for r.
              r_ :: merge(p)(rs_)
            else
              merge(p)(r_ :: rs_)
          }
          case _ => List()
        }
      }

      /* Step 4.
       * For each relation, prune from the set of related ParallelDos and ParallelDo inputs those
       * nodes that are present in any dependent relations. In the final list of relations, no
       * ParallelDo node will be present in more than one relation. */
      def pruneUnrelated(rs: List[Relation]): List[Relation] = rs map { r =>
        rs.foldLeft(r) { case (r1, r2) =>
          if (r1.dependentGbks.intersect(r2.gbks).isEmpty) {
            r1
          } else {
            val pds = r1.pdos.intersect(r2.pdos)
            val inputs = r1.pdoInputs.intersect(r2.pdoInputs)
            Relation(r1.gbks, r1.pdos -- pds, r1.pdoInputs -- inputs, r1.dependentGbks)
          }
        }
      }

      /* Find related nodes by reducing set of relations. */
      def hasGbkDependency(r1: Relation, r2: Relation): Boolean =
        (r1.gbks.intersect(r2.dependentGbks).nonEmpty) || (r2.gbks.intersect(r1.dependentGbks).nonEmpty)

      def relatedByParallelDo(r1: Relation, r2: Relation): Boolean =
        r1.pdos.intersect(r2.pdos).nonEmpty && !hasGbkDependency(r1, r2)

      def relatedByParallelDoInputs(r1: Relation, r2: Relation): Boolean =
        (r1.pdoInputs.intersect(r2.pdoInputs).nonEmpty) && !hasGbkDependency(r1, r2)

      val findRelated =
        (merge(relatedByParallelDo) _)       andThen
        (merge(relatedByParallelDoInputs) _) andThen
        (pruneUnrelated _)

      val gbks: List[DList[_]] = g.nodes.filter(isGroupByKey).toList
      findRelated(gbks.map(gbkInputs(_)))
    }


    /** Create a @GbkOutputChannel@ for each set of related @GroupByKey@s.
     *
     * First we create a collection of "initial" @GbkOutputChannels@. They
     * only contain @GroupByKey@ nodes. For each of these @GbkOutputChannel@s, oc, we
     * optionally added @Flatten@, @Combine@ and @ParallelDo@ (Reducer) nodes to the
     * channels.
     *
     * We add a @Flatten@ node if the @GroupByKey@ has this as a predecessor.
     *
     * We add a @Combine@ node if the direct successor of the @GroupByKey@ node is a
     * @Combine@ node. Otherwise we check if there is a @ParallelDo@ following it, in which
     * case we add it as a "reducer", but only if it satisfies some checks (see below).
     *
     * If we have added a @Combine@ node we then check if its successor is a @ParallelDo@. If this
     * satisifies the following checks we add it as a "reducer".
     *  - it has no successors. If it does then it should be in the input channel of another
     *    MSCR
     *  - it has no sibling @ParallelDo@ nodes. Again, this means it should be in
     *    another MSCR (in a MapperInputChannel)
     *
     * (Note: Perhaps this condition is too restrictive!) */
    private def outputChannelForGbk(gbk: DList[_], g: DGraph): GbkOutputChannel = {

      def getSingleSucc(d: DList[_]): Option[DList[_]] =
        g.succs.get(d) match {
          case Some(s) => Some(s.toList.head)
          case None    => None
        }

      def addFlatten(oc: GbkOutputChannel): GbkOutputChannel = {
        val maybeOC =
          for { s       <- g.preds.get(oc.groupByKey)
                p       <- Some(s.toList.head) // Assumes GBK has predecessor - it must; TODO - is this a fair assumption?
                flatten <- getFlatten(p)
          } yield oc.addFlatten(flatten)
        maybeOC.getOrElse(oc)
      }

      def addCombinerAndOrReducer(oc: GbkOutputChannel): GbkOutputChannel = {
        def addTheReducer(d: DList[_], oc: GbkOutputChannel): GbkOutputChannel = {
          val maybeOC =
            for { d_              <- getSingleSucc(d)
                  reducer         <- getParallelDo(d_)
                  hasNoSuccessors <- Some(!g.succs.get(reducer).isDefined)
            } yield (if (hasNoSuccessors) { oc.addReducer(reducer)} else { oc })
          maybeOC.getOrElse(oc)
        }

        val maybeOC =
          for {
            d <- getSingleSucc(oc.groupByKey)
            combiner <- getCombine(d)
          } yield addTheReducer(combiner, oc.addCombiner(combiner))
        maybeOC.getOrElse(addTheReducer(oc.groupByKey, oc))
      }

      addFlatten(addCombinerAndOrReducer(GbkOutputChannel(gbk)))
    }
  }
}
