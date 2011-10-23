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
package com.nicta.scoobi


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
 * Depending on the position for a Smart.FlatMap node in the abstract syntax tree it could end up
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
 *
 * The Flume paper defines the notion of _related GroupByKey_ nodes. Two GroupByKey nodes
 * are related if they consume the same input (possibly via a Flatten node). A collection
 * of GroupByKey nodes are related if for each GroupByKey node, n, in the collection, n is related
 * to at least one other GroupByKey node in the collection.
 *
 * See the comments on functions @mergeRelated@ and @oneStepMerge@ in object @MSCRGraph@ for
 * more detail.
 *
 * Once a set of related GroupByKey nodes has been found one can create an MSCR for it.
 *
 * See method @apply@ of the companion object @MSCR@ for details.
 *
 *
 */
object Intermediate {


  /*
   * Import renamings. (TODO: Perhaps wrap the final MSCR data structures in a namespace?)
   *
   * The C suffix on CGbkOutputChannel, etc, is for converted.
   */
  import com.nicta.scoobi.{GbkOutputChannel    => CGbkOutputChannel,
                           BypassOutputChannel => CBypassOutputChannel,
                           MapperInputChannel  => CMapperInputChannel,
                           InputChannel        => CInputChannel,
                           OutputChannel       => COutputChannel,
                           MSCR                => CMSCR }


  import com.nicta.scoobi.Smart._

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
     * Returns @true@ if this input channel contains a @Smart.FlatMap@ node
     * that should be converted to a @AST.GbkMapper@ node.
     */
    def containsGbkMapper(d: Smart.FlatMap[_,_]): Boolean

    /*
     * Creates the @DataStore@ input for this input channel.
     */
    def dataStoreInput(ci: ConvertInfo): DataStore with DataSource

    /*
     * Converts this intermediate input channel into a final input channel (defined in MSCR.scala)
     */
    def convert(ci: ConvertInfo): CInputChannel

  }

  case class MapperInputChannel(flatMaps: List[FlatMap[_,_]]) extends InputChannel {

    override def toString = "MapperInputChannel([" + flatMaps.mkString(", ")+ "])"

    /*
     * The methods @hasInput@, @hasOutput@, @containsGbkMapper@, @dataStoreInput@ and
     *  @convert@ methods are all used during Step 4 of conversion.
     *  (See "A high-level overview of MSCR conversion" above)
     *
     *  See descriptions of these methods in super class @InputChannel@
     */
    def hasInput(d: DList[_]): Boolean = flatMaps(0).in ==d

    def hasOutput(d: DList[_]): Boolean = flatMaps.exists(_==d)

    def containsGbkMapper(d: FlatMap[_,_]) = flatMaps.exists{_ == d}

    def dataStoreInput(ci: ConvertInfo): DataStore with DataSource = {
      // This should be safe since there should be at least one flatMap in @flatMaps@
      flatMaps(0).in.dataSource(ci)
    }

    def convert(ci: ConvertInfo): CMapperInputChannel[DataStore with DataSource] = {
      // TODO: Yet another asInstanceOf. Don't like them.
      def f(d: DList[_]): AST.Node[_] with MapperLike[_,_,_] =
        ci.getASTNode(d).asInstanceOf[AST.Node[_] with MapperLike[_,_,_]]
      val ns: Set[AST.Node[_] with MapperLike[_,_,_]] = flatMaps.map(f).toSet
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

    def containsGbkMapper(d: FlatMap[_,_]) = d == input

    def convert(ci: ConvertInfo): BypassInputChannel[DataStore with DataSource] = {
      // TODO. Yet another asInstanceOf
      BypassInputChannel(dataStoreInput(ci), ci.getASTNode(input)
                                               .asInstanceOf[AST.Node[_] with KVLike[_,_]])
    }

    def dataStoreInput(ci: ConvertInfo): DataStore with DataSource = input.dataSource(ci)

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
   * @Flatten@ node, and optionally succeeded by @Combine@ and/or @FlatMap@ node.
   */
  case class GbkOutputChannel(flatten:    Option[Flatten[_]],
                              groupByKey: GroupByKey[_,_],
                              combiner:   Option[Combine[_,_]],
                              reducer:    Option[FlatMap[_,_]]) extends OutputChannel {

     /*
      * Adds a @Flatten@ node to this output channel returning a new channel.
      */
     def addFlatten(flatten: Flatten[_]): GbkOutputChannel =
       new GbkOutputChannel(Some(flatten), this.groupByKey, this.combiner, this.reducer)

     /*
      * Adds a @FlatMap@ node to this output channel returning a new channel.
      */
     def addReducer(reducer: FlatMap[_,_]): GbkOutputChannel =
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

  case class BypassOutputChannel(input: FlatMap[_,_]) extends OutputChannel {
    def hasInput(d: DList[_]) = d == input
    def hasOutput(d: DList[_]) = d == input

    override def toString = "BypassOutputChannel(" + input.toString + ")"

    def output: DList[_] = input

    def convert(parentMSCR: MSCR, ci: ConvertInfo): CBypassOutputChannel[DataStore with DataSink] = {
       val n = ci.getASTNode(input)
       CBypassOutputChannel(dataStoreOutputs(parentMSCR, ci), n)
    }
  }

  class MSCR(val inputChannels: Set[InputChannel],
             val outputChannels: Set[OutputChannel]) {

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
    def containsGbkMapper(d: Smart.FlatMap[_,_]): Boolean =
      inputChannels.map{_.containsGbkMapper(d)}.exists(identity)

    /* Used during the translation from Smart.DList to AST */
    def containsGbkReducer(d: Smart.FlatMap[_,_]): Boolean = {
      def pred(oc: OutputChannel): Boolean = oc match {
        case BypassOutputChannel(_) => false
        case gbkOC@GbkOutputChannel(_,_,_,_) =>
          gbkOC.combiner.isEmpty && gbkOC.reducer.map{_ == d}.getOrElse(false)
      }
      outputChannels.exists(pred)
    }

    /* Used during the translation from Smart.DList to AST */
    def containsReducer(d: Smart.FlatMap[_,_]): Boolean = {
      def pred(oc: OutputChannel): Boolean = oc match {
        case BypassOutputChannel(_) => false
        case gbkOC@GbkOutputChannel(_,_,_,_) =>
          gbkOC.combiner.isDefined && gbkOC.reducer.map{_ == d}.getOrElse(false)
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


  object MSCR {

   /*
    * Creating an MSCR is a 2 step process:
    *
    * 1. Create an output channel for each GroupByKey node of type @GbkOutputChannel@. MSCRs can also
    *    have bypass output channels but they are discovered in step 2.
    *    See method @outputChannelsForRelatedGBKs@ for more detail.
    *
    * 2. Create input channels for each GroupByKey node as well as @BypassOutputChannel@s. The input
    *    channels will be of type
    *    @MapperInputChannel@ or @IdInputChannel@ depending on whether the inputs to the
    *    GroupByKey node are FlatMap nodes (possibly through a Flatten) or not. The
    *    @BypassOutputChannel@s are created for outputs of the @InputChannel@s that are
    *    required by other MSCRs or as outputs of the Execution Plan.
    */
    def apply(g: DGraph, relatedGBKs: Set[DList[_]]): MSCR = {

      def flatMapSiblings(d: DList[_], g: DGraph): List[FlatMap[_,_]] = {
        d match {
          case FlatMap(input,_) => {
            g.succs.get(input) match {
              case Some(succs) => succs.toList.flatMap(getFlatMap(_).toList)
              case None        => List()
            }
          }
          case _ => throw new RuntimeException("Can't call flatMapSiblings on non-flatMap node")
        }
      }

      def hasSiblings(d: DList[_], g: DGraph): Boolean = flatMapSiblings(d,g).length > 1

      /*
       * This method creates a @GbkOutputChannel@ for each set of related @GroupByKey@s.
       *
       * First we create a collection of "initial" @GbkOutputChannels@. They
       * only contain @GroupByKey@ nodes. For each of these @GbkOutputChannel@s, oc, we
       * optionally added @Flatten@, @Combine@ and @FlatMap@ (Reducer) nodes to the
       * channels.
       *
       * We add a @Flatten@ node if the @GroupByKey@ has this as a predecessor.
       *
       * We add a @Combine@ node if the direct successor of the @GroupByKey@ node is a
       * @Combine@ node. Otherwise we check if there is a @FlatMap@ following it, in which
       * case we add it as a "reducer", but only if it satisfies some checks (see below).
       *
       * If we have added a @Combine@ node we then check if its successor is a @FlatMap@. If this
       * satisifies the following checks we add it as a "reducer".
       *  - it has no successors. If it does then it should be in the input channel of another
       *    MSCR
       *  - it has no sibling @FlatMap@ nodes. Again, this means it should be in
       *    another MSCR (in a MapperInputChannel)
       *
       * (Note: Perhaps this condition is too restrictive!)
       *
       */
      def outputChannelsForRelatedGBKs(gbks: Set[DList[_]], g: DGraph): Set[GbkOutputChannel] = {
        val initOCs = gbks.foldLeft(Set(): Set[GbkOutputChannel])
                                   { (s,gbk) => s + (GbkOutputChannel(gbk)) }

        def getSingleSucc(d: DList[_]): Option[DList[_]] =
          g.succs.get(d) match {
            case Some(s) => Some(s.toList.head)
            case None    => None
          }

        def addFlatten(oc: GbkOutputChannel): GbkOutputChannel = {
          val maybeOC =
            for { s       <- g.preds.get(oc.groupByKey)
                  p       <- Some(s.toList.head) // Assumes GBK has predecessor. It must.
                  flatten <- getFlatten(p)
            } yield oc.addFlatten(flatten)
          maybeOC.getOrElse(oc)
        }

        def addCombinerAndOrReducer(oc: GbkOutputChannel): GbkOutputChannel = {
          def addTheReducer(d: DList[_], oc: GbkOutputChannel): GbkOutputChannel = {
            val maybeOC =
              for { d_              <- getSingleSucc(d)
                    reducer         <- getFlatMap(d_)
                    hasNoSuccessors <- Some(!g.succs.get(reducer).isDefined)
                    canAdd          <- Some(!hasSiblings(reducer,g) && hasNoSuccessors)
              } yield (if (canAdd) { oc.addReducer(reducer)} else { oc })
            maybeOC.getOrElse(oc)
          }

          val maybeOC =
            for {
              d <- getSingleSucc(oc.groupByKey)
              combiner <- getCombine(d)
            } yield addTheReducer(combiner, oc.addCombiner(combiner))
          maybeOC.getOrElse(addTheReducer(oc.groupByKey, oc))
        }

        /*
         * Adds Flattens, Combiners, Reducers for output channels
         */
        def addNodes(oc: GbkOutputChannel): GbkOutputChannel = {
          val newOC = addCombinerAndOrReducer(oc)
          addFlatten(newOC)
        }

        initOCs.foldLeft(Set(): Set[GbkOutputChannel]){case (s,oc) => s + addNodes(oc)}

      }

      /*
       * This is Step 2 of creating an MSCR.
       *
       * Adding input channels and creating extra bypass output channels is done concurrently.
       *
       * A @BypassOutputChannel@ is created if the output of an input channel is in
       * the Execution Plan outputs OR it it is NOT the input to any of the @GbkOutputChannel@s
       *
       */

       // FIXME: Avoid double-up of MapperInputChannels and BypassOutputChannels


      def addInputChannels(g: DGraph, ocs: Set[GbkOutputChannel], inputs: Set[DList[_]]):
                          (Set[InputChannel], Set[BypassOutputChannel]) = {


        def isBypass(fm: FlatMap[_,_]): Boolean = {
          def isInExecutionPlanOutputs(d: DList[_]) = g.outputs.contains(d)
          !ocs.exists(_.hasInput(fm)) || isInExecutionPlanOutputs(fm)
        }

        def bypassChan(fm: FlatMap[_,_]) =
          if ( isBypass(fm) ) { Some (BypassOutputChannel(fm))} else { None }


        /* One of the first times I've used vars in Scala code */
        var siblingSets: Set[Set[FlatMap[_,_]]] = Set()
        var nonFlatMaps: Set[DList[_]]          = Set()

        def addSiblingsSetOrNonFlatMap(d: DList[_]): Unit = {
          d match {
            case fm@FlatMap(_,_) => {
               val siblings = flatMapSiblings(d, g).toSet
               siblingSets += siblings
            }
            case _ => nonFlatMaps += d
          }
        }

        inputs.foreach(addSiblingsSetOrNonFlatMap)

        def mkInputChannelAndBypassOutputChannels(siblings: Set[FlatMap[_,_]]):
                                                 (InputChannel, Set[BypassOutputChannel]) = {
          val bypassChannels = siblings.flatMap(bypassChan(_))
           (MapperInputChannel(siblings.toList), bypassChannels)
        }

        val idInputChannels = nonFlatMaps.map{IdInputChannel}

        val (ics, bpocs) = siblingSets.map{mkInputChannelAndBypassOutputChannels}.unzip
        (ics ++ idInputChannels, bpocs.flatten)
      }

      /* Step 1 */
      val gbkOCs    = outputChannelsForRelatedGBKs(relatedGBKs, g)
      // Get all the inputs of the output channels.
      val allInputs = gbkOCs.flatMap(_.inputs(g))
      /* Step 2 */
      val (inputChannels, bypassOutputChannels) = addInputChannels(g, gbkOCs, allInputs)

      new MSCR(inputChannels, gbkOCs ++ bypassOutputChannels)

    }
  }

  object MSCRGraph {
    def apply(outputs: Iterable[DList[_]]): MSCRGraph = {

      /*
       * Two nodes are related if they share at least one input. Let A be the type of nodes
       * and B the type of inputs.
       *
       * A set of nodes (i.e. a node set) is are related if for each A in the set there exists as
       * B in set A where A has at least one input in common with B.
       *
       * Let a *relate-node-record* be a pair of sets. The first element
       * is a *node set* and the second element is an *input set*
       *
       * Here is an example of a relate-node-record:
       *   (Set(A,B), Set(1,2,3))
       *
       * This means nodes A and B, collectively, have inputs 1,2, and 3.
       *
       * @mergeRelated@ takes a list of relate-node-records and merges them together.
       * yielding another list of relate-node-records. Each node set of each relate-node-record
       * is not related to all other node sets of all other relate-node-records.
       *
       * Example:
       *   Input:
       *     List( (Set(A,B), Set(1,2)), (Set(C), Set(2,3)), (Set(D), Set(3,4)), (Set(E), Set(5,6)))
       *
       *   A,B,C,D are all related since they (collectively) all share inputs.
       *   Although D and A and D and B are not directly related they are related through C.
       *
       *   Output:
       *     List( (Set(A,B,C,D), Set(1,2,3,4)), (Set(E), Set(5,6)) )
       */

      def mergeRelated[A,B](ps: List[(Set[A], Set[B])]): List[(Set[A], Set[B])] = {

        ps match {
          case p :: ps => {
            val (p_, ps_) = oneStepMerge(p, ps)
            if (p == p_) // Fixed point reached. No more relatedness for p.
              p_ :: mergeRelated(ps_)
            else
              mergeRelated(p_ :: ps_)
          }
          case _       => List()
        }
      }

      /*
       * @oneStepMerge@ is a helper function which helps create maximally sized relate-node-records.
       * The first argument @p@ is the _current_ relate-node-record. @oneStepMerge@ will
       * traverse through the second argument @ps@ (a list of relate-node-records)
       * seeing if the input sets of each relate-node-record overlaps with the current
       * relate-node-records input set.
       *
       * If any do these are removed and *merged* with the _current_ related-node-record.
       * The output of this function is a pair of the new current related-node-record and
       * the remanining disjoint related-node-records.
       *
       * @oneStepMerge@ is repeatedly called by @mergeRelated@ until a fixed-point is reached
       * i.e. until the current related-node-record does not change in size.
       *
       * Example:
       *   oneStepMerge((Set(A), Set(1,2)), List( (Set(B), Set(2,3)), (Set(C), Set(3,4)) ))
       * evaluates to:
       *   ((Set(A,B), Set(1,2,3)), List( (Set(C), Set(3,4))))
       *
       * Note that element @(Set(C), Set(3,4))@ did *NOT* overlap with @(Set(A), Set(1,2))@
       * so it was not removed from the list. (A subsequent call to @oneStepMerge@ of course
       * would remove it since it @(Set(A,B), Set(1,2,3))@ overlaps with it.)
       *
       */
      def oneStepMerge[A,B](p: (Set[A], Set[B]), ps: List[(Set[A], Set[B])]):
        ((Set[A], Set[B]), List[(Set[A], Set[B])]) = {

        def merge(p1: (Set[A], Set[B]), p2: (Set[A], Set[B])): (Set[A], Set[B]) = {
          (p1._1.union(p2._1), p1._2.union(p2._2))
        }

        def intersect(p1: (Set[A], Set[B]), p2: (Set[A], Set[B])): Boolean = {
          !p1._2.intersect(p2._2).isEmpty
        }

        val (overlapped, disjoint) = ps.partition(intersect(p,_))
        val newPair = overlapped.foldLeft(p)(merge)
        (newPair, disjoint)
      }

      def findRelated(g: DGraph): List[Set[DList[_]]] = {
        val gbks: List[DList[_]] = g.nodes.filter(isGroupByKey).toList

        def gbkInputs(gbk: DList[_]): (Set[DList[_]], Set[DList[_]]) = {
          val (GroupByKey(d)) = gbk
          (Set(gbk), flatMapInputs(d))
        }

        def flatMapInputs(dlist: DList[_]): Set[DList[_]] = {
          dlist match {
            case Flatten(ds)  => ds.filter(isFlatMap).flatMap(flatMapInputs(_)).toSet
            case FlatMap(d,_) => Set(d)
            case _            => Set()
          }
        }
        mergeRelated(gbks.map(gbkInputs)).map(_._1)
      }

      val g = DGraph(outputs)

      /* Construct MSCRs based on related GroupByKey nodes. */
      val relatedGBKSets = findRelated(g)
      val gbkMSCRs = relatedGBKSets.map(MSCR(g,_))


      /** Find all outputs that are not within a GBK MSCR. There are 2 cases:
        *
        *   1. The ouput is a FlatMap node connected directly to a Load node;
        *   2. The output is a Flatten node, connected to the output(s) of an MSCR and/or
        *      Load node(s).
        *
        * For case 1, all FlatMaps can be added to the same MSCR. Each FlatMap will be
        * added to its own input channel and will feed directly into its own bypass
        * output channel.
        *
        * TODO - handle case 2 by allowing bypass output channels to contain
        * optional Flatten nodes.
        *
        * TODO - investigate adding remaining FlatMaps to an exisitng MSCR. */
      val floatingOutputs = outputs filterNot { o => gbkMSCRs.exists(mscr => mscr.hasOutput(o)) }

      val allMSCRs =
        if (floatingOutputs.size > 0) {
          val (ics, ocs) = floatingOutputs map {
            case fm@FlatMap(_, _) => (MapperInputChannel(List(fm)), BypassOutputChannel(fm))
            case flat@Flatten(_)  => sys.error("Trivial MSCR from Flattens not yet supported.")
            case node             => sys.error("Not expecting " + node.name + " as remaining node.")
          } unzip

          val mapOnlyMSCRMSCR = new MSCR(ics.toSet, ocs.toSet)

          gbkMSCRs :+ mapOnlyMSCRMSCR
        } else {
          gbkMSCRs
        }

      /* Final MSCR graph contains both GBK MSCRs and Map-only MSCRs. */
      new MSCRGraph(allMSCRs, g)
    }
  }

  class MSCRGraph(val mscrs: Iterable[MSCR], val g: DGraph)
}
