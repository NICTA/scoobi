package com.nicta.scoobi
package impl
package plan
package graph

import comp._
import Optimiser._
import org.kiama.attribution.Attribution
import org.kiama.attribution.Attribution._
import collection._
import IdSet._
import scala.collection.immutable.SortedSet
import io.DataSink

/**
 * This trait computes the Mscr for a given nodes graph.
 *
 * It does so by collecting:
 *
 *  - the input channels going into a "related" set of GroupByKey nodes
 *  - the output channels coming out of a "related" set of GroupByKey nodes
 *
 *  2 GroupByKey nodes are being "related" if they have input ParallelDos sharing the same inputs.
 *  Those ParallelDo nodes are said to be "siblings" in the code below
 */
trait MscrGraph extends CompNodes {

  /** compute the mscr of a node: it is either a Mscr around a gbk or a floating parallelDo or a floating flatten */
  lazy val mscr: CompNode => Mscr = attr { case n => (n -> mscrOpt).getOrElse(Mscr()) }
  def mscrOpt: CompNode => Option[Mscr] = attr {
    case n => (n -> gbkMscr).orElse(n -> parallelDosMscr).orElse(n -> flattenMscr)
  }

  /** A gbk mscr is a mscr for a group by key + input and output channels */
  lazy val gbkMscr: CompNode => Option[Mscr] = attr {
    case g: GroupByKey[_,_] => {
      val related = (g -> relatedGbks)
      (g -> baseGbkMscr).map(m => m.addChannels((g -> inputChannels(g))  ++ related.flatMap(_ -> inputChannels(g)),
                                                (g -> outputChannels) ++ related.flatMap(_ -> outputChannels)))
    }
    case other              => None
  }
  /** there must be a new Mscr for each related GroupByKey */
  lazy val baseGbkMscr: CompNode => Option[Mscr] = circular(Option(Mscr.empty)) {
    case g: GroupByKey[_,_] => (g -> relatedGbks).flatMap(_ -> baseGbkMscr).headOption.orElse(Option(Mscr.empty))
    case other              => None
  }

  /** compute the mscr of "floating" parallelDo nodes, sharing the same input */
  lazy val parallelDosMscr: CompNode => Option[Mscr] = attr {
    case pd: ParallelDo[_,_,_] if pd -> isFloating => Some(Mscr.floatingParallelDosMscr(pd, (pd -> siblings).collect(isAParallelDo)))
    case other                                     => None
  }

  /** compute the mscr of a "floating" flatten node */
  lazy val flattenMscr: CompNode => Option[Mscr] = attr {
    case f : Flatten[_] if f -> isFloating => Some(Mscr.floatingFlattenMscr(f))
    case other                               => None
  }

  /** compute the input channels of a node */
  lazy val inputChannels =
    paramAttr { gbk: GroupByKey[_,_] => { n: CompNode =>
      (n -> mapperInputChannels) ++ (n -> idInputChannels(gbk))
    }}

  /** the mapper input channel of a node is grouping ParallelDo nodes which are siblings */
  lazy val mapperInputChannels: CompNode => Set[MapperInputChannel] =
    attr {
      case pd: ParallelDo[_,_,_] if pd -> isMapper && (pd -> hasSiblings) => Set(MapperInputChannel((pd -> siblings).collect(isAParallelDo) + pd))
      case other                                                          => other.children.asNodes.flatMap(_ -> mapperInputChannels).toSet
    }

  /**
   * compute the id input channels of a node.
   * An IdInputChannel is a channel for an input node which has no siblings and is connected to a GroupByKey */
  lazy val idInputChannels: GroupByKey[_,_] => CompNode => Set[IdInputChannel] =
    paramAttr { gbk: GroupByKey[_,_] => { node: CompNode => node match {
      case n if (n -> isGbkInput) && !(n -> hasSiblings) => Set(IdInputChannel(n, gbk))
      case other                                         => other.children.asNodes.flatMap(_ -> idInputChannels(gbk)).toSet
    }}}

  /** compute the output channels of a node */
  lazy val outputChannels: CompNode => Set[OutputChannel] = attr {
    case n => (n -> gbkOutputChannels) ++ (n -> bypassOutputChannels)
  }

  /**
   * the gbk output channel of a node is associating a given GroupByKey with possibly:
   *  - a Flatten node if it is an input of the GroupByKey
   *  - a Combine node if it is the output of the GroupByKey
   *  - a ParallelDo node (called a "reducer") if it is the output of the GroupByKey
   */
  lazy val gbkOutputChannels: CompNode => Set[GbkOutputChannel] =
    attr {
      case g @ GroupByKey1(in : Flatten[_]) => Set(GbkOutputChannel(g, flatten = Some(in), combiner = g -> combiner, reducer = g -> reducer))
      case g: GroupByKey[_,_]               => Set(GbkOutputChannel(g, combiner = g -> combiner, reducer = g -> reducer))
      case other                            => Set()
    }

  /**
   * compute a ByPassOutputChannel for:
   * - each related ParallelDo if it has outputs other than Gbks
   */
  lazy val bypassOutputChannels: CompNode => Set[BypassOutputChannel] =
    attr {
      case pd: ParallelDo[_,_,_] if (pd -> isMapper) &&
                                    (pd -> outputs).exists(isGroupByKey)  &&
                                   !(pd -> outputs).forall(isGroupByKey) => Set(BypassOutputChannel(pd))
      case other                                                         => other.children.asNodes.flatMap(_ -> bypassOutputChannels).toSet
    }

  /**
   * compute if a node is a reducer of a GroupByKey, i.e. a ParallelDo which
   *  - has a group barrier
   *  - has a fuse barrier
   *  - has a Materialize parent
   *  - has no ancestor
   *
   *  For example, if the graph is: gbk(pd1) then pd1 -> isReducer == false because pd1.parent is defined
   */
  lazy val isReducer: CompNode => Boolean =
    attr {
      case pd @ ParallelDo(_, mt: Materialize[_],_,_,_) if envInSameMscr(pd) => false
      case pd @ ParallelDo(cb: Combine[_,_],_,_,_,Barriers(true,_))          => true
      case pd @ ParallelDo(cb: Combine[_,_],_,_,_,Barriers(_,true))          => true
      case pd: ParallelDo[_,_,_]                                             => (pd -> outputs).isEmpty || (pd -> outputs).exists(isMaterialize)
      case other                                                             => false
    }
  /**
   * @return true if a parallel do and its environment are computed by the same mscr
   */
  def envInSameMscr(pd: ParallelDo[_,_,_]) =
    (pd.env -> descendents).collect(isAGroupByKey).headOption.map { g =>
      (g -> relatedGbks + g).flatMap(_ -> outputs).contains(pd)
    }.getOrElse(false)

  /** compute if a node is a mapper of a GroupByKey, i.e. a ParallelDo which is not a reducer */
  lazy val isMapper: CompNode => Boolean =
    attr {
      case pd: ParallelDo[_,_,_] => !(pd -> isReducer)
      case other                 => false
    }

  /** compute if a node is a combiner of a GroupByKey, i.e. the direct output of a GroupByKey */
  lazy val isCombiner: CompNode => Boolean =
    attr {
      case Combine(g: GroupByKey[_,_],_,_) => true
      case other                           => false
    }

  /** compute the combiner of a Gbk */
  lazy val combiner: GroupByKey[_,_] => Option[Combine[_,_]] =
    attr {
      case g @ GroupByKey1(_) if hasParent(g) && (g.parent.asNode -> isCombiner) => Some(g.parent.asInstanceOf[Combine[_,_]])
      case other                                                                 => None
    }

  /** compute the reducer of a Gbk */
  lazy val reducer: GroupByKey[_,_] => Option[ParallelDo[_,_,_]] =
    attr {
      case g : GroupByKey[_,_] =>
        (g -> parentOpt) match {
          case Some(pd: ParallelDo[_,_,_]) if pd -> isReducer => Some(pd)
          case Some(c:  Combine[_,_])                         => (c -> parentOpt).collect { case pd @ ParallelDo1(_) if pd -> isReducer => pd }
          case other                                          => None
        }
      case other                                              => None
    }

  /** compute if a ParallelDo or a Flatten is 'floating', i.e. it doesn't have a Gbk in it outputs */
  lazy val isFloating: CompNode => Boolean =
    attr {
      case pd: ParallelDo[_,_,_] => !(pd -> isReducer) && !(pd -> outputs).exists(isGroupByKey)
      case f: Flatten[_]         => !(f -> outputs).exists(isGroupByKey)
    }

  /** compute if a node is the input of a Gbk, but not a Gbk */
  lazy val isGbkInput: CompNode => Boolean = attr {
    case GroupByKey1(_) => false
    case node           => (node -> outputs).exists(isGroupByKey)
  }

  /**
   * compute the Gbks related to a given Gbk (through some shared input)
   *
   * This set does not contain the original gbk
   */
  lazy val relatedGbks: GroupByKey[_,_] => SortedSet[GroupByKey[_,_]] =
    attr {
      case g @ GroupByKey1(Flatten1(ins))         => (g -> siblings).collect(isAGroupByKey) ++
                                                      ins.flatMap(_ -> siblings).flatMap(_ -> outputs).flatMap(out => (out -> outputs) + out).collect(isAGroupByKey)
      case g @ GroupByKey1(pd: ParallelDo[_,_,_]) => (g -> siblings).collect(isAGroupByKey) ++
                                                     (pd -> siblings).flatMap(_ -> outputs).collect(isAGroupByKey)
      case g @ GroupByKey1(_)                     => (g -> siblings).collect(isAGroupByKey)
    }

  private def addSinks(sinks: SinksMap) = (m: Mscr) => m.addSinks(sinks)

  /** type synonym to keep the relations between output nodes and datasinks */
  type SinksMap = Map[CompNode, Seq[DataSink[_,_,_]]]

  /** @return the MSCRs accessible from a given node, without optimising the graph first */
  def makeMscrs(node: CompNode, sinks: SinksMap = Map()): Set[Mscr] = {
    // make sure that the parent <-> children relationships are initialized
    Attribution.initTree(node)
    val mscrs = ((node -> descendents) + node).map(mscrOpt).flatten.filterNot(_.isEmpty)
    mscrs.map(addSinks(sinks))
  }

  /** @return the first reachable MSCR for a given node, without optimising the graph first */
  private[plan] def makeMscr(node: CompNode, sinks: SinksMap = Map()) =
    makeMscrs(node, sinks).headOption.getOrElse(Mscr())
  /** @return the MSCR for a given node. Used for testing only because it does the optimisation first */
  private[plan] def mscrFor(node: CompNode) =
    makeMscr(Optimiser.optimise(node))
  /** @return the MSCRs for a list of nodes. Used for testing only */
  private[plan] def mscrsFor(nodes: CompNode*) = (nodes map mscrFor).toSet
}

object MscrGraph extends MscrGraph