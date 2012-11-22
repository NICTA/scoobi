package com.nicta.scoobi
package impl
package plan
package mscr

import org.kiama.attribution.Attribution._
import scala.collection.immutable.SortedSet

import collection._
import comp._
import core._
import control._
import IdSet._
import Functions._

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
trait MscrMaker extends CompNodes {

  /** compute the mscr of a node: it is either a Mscr around a gbk or a floating parallelDo or a floating flatten */
  lazy val mscr: CompNode => Mscr = attr { case n => (n -> mscrOpt).getOrElse(Mscr()) }
  def mscrOpt: CompNode => Option[Mscr] = attr {
    case n => (n -> gbkMscr).orElse(n -> parallelDosMscr).orElse(n -> flattenMscr)
  }

  /** A gbk mscr is a mscr for a group by key + input and output channels */
  lazy val gbkMscr: CompNode => Option[Mscr] = attr {
    case g: GroupByKey[_,_] => (g -> baseGbkMscr).map(m => m.copy().addChannels(g -> inputChannels, g -> outputChannels))
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
  lazy val inputChannels: GroupByKey[_,_] => Set[InputChannel] = attr { gbk =>
    (gbk -> mapperInputChannels) ++ (gbk -> idInputChannels)
  }

  /** the mapper input channel of a node is grouping ParallelDo nodes which are siblings */
  lazy val mapperInputChannels: GroupByKey[_,_] => Set[MapperInputChannel] = attr { gbk: GroupByKey[_,_] =>
    (gbk -> relatedParallelDos).groupBy(_.in.id).values.map(pds => MapperInputChannel(pds.toSeq:_*)).toSet
  }

  /** the set of related paralleldos for a gbk */
  lazy val relatedParallelDos: GroupByKey[_,_] => Set[ParallelDo[_,_,_]] = attr { gbk: GroupByKey[_,_] =>
    val gbks = (gbk -> relatedGbks) + gbk
    // grab all the first parallelDo mapper nodes which accessible from the related gbks
    val accessibleParallelDos = gbks.flatMap(_ -> descendentsWhileUntil((!isGroupByKey, isParallelDo && isMapper))).collect(isAParallelDo)
    // filter out all the parallelDos which have some outputs which are not exclusively in the set of related gbks
    accessibleParallelDos.filterNot(pd => (pd -> ancestors).take(3).forall(o => !gbks.exists(_ == o)))
  }

  /**
   * compute the id input channels of a node.
   * An IdInputChannel is a channel for an input node which has no siblings and is connected to a GroupByKey
   * via a Flatten node
   */
  lazy val idInputChannels: GroupByKey[_,_] => Set[IdInputChannel] = attr { gbk: GroupByKey[_,_] =>
    (gbk -> idInputs).map(in => IdInputChannel(Some(in), gbk))
  }

  lazy val idInputs: GroupByKey[_,_] => Set[CompNode] = attr { gbk: GroupByKey[_,_] =>
    val gbks = (gbk -> relatedGbks) + gbk
    // all the flatten nodes of the related group by keys
    val flattens = gbks.flatMap(_ -> inputs).collect(isAFlatten)
    // accessible inputs from the flattens
    val ins = flattens.flatMap(_.ins)
    // id inputs are the one which are not related to any other parallel do
    val relatedPdos = (gbk -> relatedParallelDos)
    ins.filterNot(isParallelDo) ++ ins.collect(isAParallelDo).filterNot(relatedPdos.contains)
  }

  /** compute the output channels of a node */
  lazy val outputChannels: CompNode => Set[OutputChannel] = attr {
    case g: GroupByKey[_,_] => (g -> relatedGbks + g).flatMap(_ -> gbkOutputChannels)
    case other              => other -> bypassOutputChannels
  }

  /**
   * the gbk output channel of a node is associating a given GroupByKey with possibly:
   *  - a Flatten node if it is an input of the GroupByKey
   *  - a Combine node if it is the output of the GroupByKey
   *  - a ParallelDo node (called a "reducer") if it is the output of the GroupByKey
   */
  lazy val gbkOutputChannels: CompNode => Set[OutputChannel] =
    attr {
      case g @ GroupByKey1(in : Flatten[_]) => Set(GbkOutputChannel(g, flatten = Some(in), combiner = g -> combiner, reducer = g -> reducer))
      case g: GroupByKey[_,_]               => Set(GbkOutputChannel(g, combiner = g -> combiner, reducer = g -> reducer))
      case other                            => Set()
    }

  /**
   * compute a ByPassOutputChannel for:
   * - each related ParallelDo if it has outputs other than Gbks
   */
  lazy val bypassOutputChannels: CompNode => Set[OutputChannel] =
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
    attr { (n: CompNode) => n match {
      case pd @ ParallelDo(_, mt: Materialize[_],_,_,_,_) if envInSameMscr(pd) => false
      case pd @ ParallelDo(cb: Combine[_,_],_,_,_,_,Barriers(true,_))          => true
      case pd @ ParallelDo(cb: Combine[_,_],_,_,_,_,Barriers(_,true))          => true
      case pd @ ParallelDo(cb : Combine[_,_],_,_,_,_,_)                        => (pd -> outputs).isEmpty || (pd -> outputs).exists(isMaterialize)
      case pd @ ParallelDo(gbk: GroupByKey[_,_],_,_,_,_,_)                     => (pd -> outputs).isEmpty || (pd -> outputs).exists(isMaterialize)
      case other                                                               => false
    }}
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
      case Combine(g: GroupByKey[_,_],_,_,_) => true
      case other                             => false
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
          case Some(c:  Combine[_,_])                         => (c -> parentOpt).collect { case pd : ParallelDo[_,_,_] if pd -> isReducer => pd }
          case other                                          => None
        }
      case other                                              => None
    }

  /** compute if a ParallelDo or a Flatten is 'floating', i.e. it is not part of a GbkMscr */
  lazy val isFloating: CompNode => Boolean =
    attr {
      case pd: ParallelDo[_,_,_]                                   => !(pd -> isReducer)
      case Flatten1(ins) if ins.exists(isGroupByKey || isCombiner) => false
      case _                                                       => true
    }

  /** compute if a node is the input of a Gbk, but not a Gbk */
  lazy val isGbkInput: CompNode => Boolean = attr {
    case GroupByKey1(_) => false
    case node           => (node -> outputs).exists(isGroupByKey)
  }

  /**
   * compute the Gbks related to a given Gbk (through some shared input)
   *
   * This set does not contain the original gbk and cannot be parent of the current gbk
   */
  lazy val relatedGbks: GroupByKey[_,_] => SortedSet[GroupByKey[_,_]] = attr { gbk: GroupByKey[_,_] =>
    (gbk -> descendents).flatMap(_ -> parents).collect(isAGroupByKey).filterNot(_ -> isParentOf(gbk))
  }

  /** type synonym to keep the relations between output nodes and datasinks */
  type SinksMap = Map[CompNode, Seq[DataSink[_,_,_]]]

  /** @return the MSCRs accessible from a given node, without optimising the graph first */
  def makeMscrs(node: CompNode): Set[Mscr] = {
    // make sure that the parent <-> children relationships are initialized
    ((initAttributable(node) -> descendents) + node).map(mscrOpt).flatten.filterNot(_.isEmpty)
  }

  /** @return the first reachable MSCR for a given node, without optimising the graph first */
  def makeMscr(node: CompNode) =
    mscrOpt(node).getOrElse(Mscr())

  /** @return the MSCR for a given node. Used for testing only because it does the optimisation first */
  private[plan] def mscrFor(node: CompNode) =
    makeMscr(Optimiser.optimise(node))
  /** @return the MSCRs for a list of nodes. Used for testing only */
  private[plan] def mscrsFor(node: CompNode) = {
    // make sure that the parent <-> children relationships are initialized
    ((initAttributable(Optimiser.optimise(node)) -> descendents) + node).map(mscrOpt).flatten.filterNot(_.isEmpty)
  }

}
object MscrMaker extends MscrMaker