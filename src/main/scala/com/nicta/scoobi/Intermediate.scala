/**
  * Copyright: [2011] Sean Seefried
  *
  * An Intermediate MSCR form used while we're still determining
  * where the boundaries of MSCRs are.
  *
  */

package com.nicta.scoobi

object Intermediate {

  import com.nicta.scoobi.Smart._

  /*
   *  InputChannel
   */
  sealed abstract class InputChannel {
    def contains(d: DList[_]): Boolean
  }


  case class MapperInputChannel(flatMaps: List[DList[_]]) extends InputChannel {

    def contains(d: DList[_]) = flatMaps.exists(_==d)

    override def toString = "MapperInputChannel([" + flatMaps.mkString(", ")+ "])"
  }

  case class IdInputChannel extends InputChannel {
    def contains(d: DList[_]) = false

    override def toString = "IdInputChannel"
  }

  /*
   * OutputChannel
   */
  sealed abstract class OutputChannel {
    def contains(d: DList[_]): Boolean
  }

  object GbkOutputChannel {
    def apply(d: DList[_]): GbkOutputChannel = {
      d match {
        case gbk@GroupByKey(_) => GbkOutputChannel(None, gbk, None, None)
        case _                 => throw new RuntimeException("This is not a GBK")
      }
    }

  }

  case class GbkOutputChannel(flatten:    Option[Flatten[_]],
                              groupByKey: GroupByKey[_,_],
                              combiner:   Option[Combine[_,_]],
                              reducer:    Option[FlatMap[_,_]]) extends OutputChannel {

     def addFlatten(flatten: Flatten[_]): GbkOutputChannel =
       new GbkOutputChannel(Some(flatten), this.groupByKey, this.combiner, this.reducer)

     def addReducer(reducer: FlatMap[_,_]): GbkOutputChannel =
       new GbkOutputChannel(this.flatten, this.groupByKey, this.combiner, Some(reducer))

     def addCombiner(combiner: Combine[_,_]): GbkOutputChannel =
       new GbkOutputChannel(this.flatten, this.groupByKey, Some(combiner), this.reducer)

     def contains(d: DList[_]): Boolean = {
       def sameAs(f: GbkOutputChannel => Option[DList[_]]) = f(this).map{_ == d}.getOrElse(false)
       /* Fix function is necessary to help Scala's type inferencer */
       def fix(a: GbkOutputChannel => Option[DList[_]]) = a
       val l: List[GbkOutputChannel => Option[DList[_]]] =
         List(fix(_.flatten), fix(x => Some(x.groupByKey)), fix(_.combiner), fix(_.reducer))
       l.exists(sameAs)
     }

     /*
      * Find the inputs to this channel. If there is a Flatten node then it is the parents of this
      * node. Otherwise it is the parent of the GroupByKey node
      */
     def inputs(g: DGraph): List[DList[_]] = {
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
  }

  case class BypassInputChannel(input: FlatMap[_,_]) extends OutputChannel {
    def contains(d: DList[_]) = false

    override def toString = "BypassInputChannel(" + input.toString + ")"
  }

  class MSCR(val inputChannels: Set[InputChannel],
             val outputChannels: Set[OutputChannel]) {

    def contains(d: DList[_]) =
      this.inputChannels.exists(_.contains(d)) || this.outputChannels.exists(_.contains(d))

    override def toString = {
      Pretty.indent(
        "MSCR(",
          List(Pretty.indent("inputChannels:  { ", inputChannels.mkString(",\n")) + "}",
               Pretty.indent("outputChannels: { ", outputChannels.mkString(",\n")) + "}").
          mkString(",\n"))
    }
  }

  object MSCR {

    def apply(g: DGraph, relatedGBKs: Set[DList[_]]): MSCR = {

      def flatMapsToFuse(d: DList[_], g: DGraph): List[FlatMap[_,_]] = {
        d match {
          case FlatMap(input,_) => {
            g.succs.get(input) match {
              case Some(succs) => succs.toList.map(getFlatMap(_).toList).flatten
              case None        => List()
            }
          }
          case _ => throw new RuntimeException("Can't call flatMapsToFuse on non-flatMap node")
        }
      }

      def canFuse(d: DList[_], g: DGraph): Boolean = flatMapsToFuse(d,g).length > 1

      def outputChannelsForRelatedGBKs(gbks: Set[DList[_]], g: DGraph): Set[GbkOutputChannel] = {
        val initOCs = gbks.foldLeft(Set(): Set[(GbkOutputChannel,DList[_])])
                                   { (s,gbk) => s + ((GbkOutputChannel(gbk), gbk)) }

        def getSingleSucc(d: DList[_]): Option[DList[_]] =
          g.succs.get(d) match {
            case Some(s) => Some(s.toList.head)
            case None    => None
          }

        def addFlatten(gbk: DList[_], oc: GbkOutputChannel): GbkOutputChannel = {
          val maybeOC =
            for { s       <- g.preds.get(gbk)
                  p       <- Some(s.toList.head) // Assumes GBK has predecessor. It must.
                  flatten <- getFlatten(p)
            } yield oc.addFlatten(flatten)
          maybeOC.getOrElse(oc)
        }

        def addCombinerAndOrReducer(gbk: DList[_], oc: GbkOutputChannel): GbkOutputChannel = {
          def addReducer(d: DList[_], oc: GbkOutputChannel): GbkOutputChannel = {
            val maybeOC =
              for { d_              <- getSingleSucc(d)
                    reducer         <- getFlatMap(d_)
                    hasNoSuccessors <- Some(!g.succs.get(reducer).isDefined)
                    canAdd          <- Some(!canFuse(reducer,g) && hasNoSuccessors)
              } yield (if (canAdd) { oc.addReducer(reducer)} else { oc })
            maybeOC.getOrElse(oc)
          }

          val maybeOC =
            for {
              d <- getSingleSucc(gbk)
              combiner <- getCombine(d)
            } yield addReducer(combiner, oc.addCombiner(combiner))
          maybeOC.getOrElse(addReducer(gbk,oc))
        }


        /*
         * Adds Flattens, Combiners, Reducers for output channels
         */
        def addNodes(oc: GbkOutputChannel, gbk: DList[_]): GbkOutputChannel = {
          addFlatten(gbk, addCombinerAndOrReducer(gbk, oc))
        }

        initOCs.foldLeft(Set(): Set[GbkOutputChannel]){case (s,(oc,gbk)) => s + addNodes(oc,gbk)}

      }

      def addInputChannel(g: DGraph, ocs: Set[GbkOutputChannel], d: DList[_]) = {

        def isBypass(fm: FlatMap[_,_]): Boolean = {
          val maybeSuccessors = g.succs.get(d)
          def connectsToOutputChannel(d: DList[_]) = ocs.exists(_.contains(d))
          def connectsToNoOutputChannel(ss: Set[DList[_]]) =
            ss.toList.forall(!connectsToOutputChannel(_))
          def isInExecutionPlanOutputs(d: DList[_]) = g.outputs.contains(d)

          maybeSuccessors.map(connectsToNoOutputChannel).getOrElse(true) ||
          isInExecutionPlanOutputs(d)
        }


        def bypassChan(fm: FlatMap[_,_]) =
          if ( isBypass(fm) ) { None } else { Some (BypassInputChannel(fm))}

        d match {
          case fm@FlatMap(_,_) => {
            val fused = flatMapsToFuse(d, g)
            val bypassChannels = fused.map(bypassChan(_).toList).flatten
            (MapperInputChannel(fused), bypassChannels)
          }
          case _ => (IdInputChannel(), List())
        }
      }

      val ocs    = outputChannelsForRelatedGBKs(relatedGBKs, g)
      val allInputs = ocs.flatMap(_.inputs(g))
      val (inputChannels, extraChannels) = allInputs.map(addInputChannel(g, ocs, _)).unzip

      new MSCR(inputChannels.toSet, ocs ++ extraChannels.flatten.toSet)

    }
  }

  object MSCRGraph {
    def apply(outputs: List[DList[_]]): MSCRGraph = {

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
            if (p == p_)
              p_ :: mergeRelated(ps_)
            else
              mergeRelated(p_ :: ps_)
          }
          case _       => List()
        }
      }

      /*
       *  @oneStepMerge@ is a helper function which helps create maximally sized relate-node-records.
       * The first argument @p@ is the _current_ relate-node-record. @oneStepMerge@ will
       * traverse through the second argument @ps@ (a list of relate-node-records)
       * seeing if the input sets of each relate-node-record overlaps with the current
       * relate-node-records' input set.
       *
       * If any do these are removed and *merged* with the _current_ related-node-record.
       * The output of this function is a pair of the new current related-node-record and
       * the remanining disjoint related-node-records.
       *
       * @oneStepMerge@ is  repeatedly called by @mergeRelated@ until a fixed-point is reached
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
          p1._2.intersect(p2._2).isEmpty
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
            case Flatten(ds)  => ds.filter(isFlatMap).toSet
            case FlatMap(d,_) => Set(d)
            case _            => Set()
          }
        }
        mergeRelated(gbks.map(gbkInputs)).map(_._1)
      }

      val g = DGraph(outputs)
      val relatedGBKSets = findRelated(g)

      new MSCRGraph(relatedGBKSets.map(MSCR(g,_)))
    }
  }

  class MSCRGraph(mscrs: List[MSCR]) {

    override def toString =
      Pretty.indent("MSCRGraph([", mscrs.mkString(",\n")) + "])"
  }

}
