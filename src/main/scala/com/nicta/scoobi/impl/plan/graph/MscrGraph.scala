package com.nicta.scoobi
package impl
package plan
package graph

import comp._
import Optimiser._
import org.kiama.attribution.Attribution
import org.kiama.attribution.Attribution._
import CompNode._

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
trait MscrGraph {

  /** compute the mscr of a node: it is either a Mscr around a gbk or a floating parallelDo or a floating flatten */
  lazy val mscr: CompNode => Mscr = attr { case n => (n -> mscrOpt).getOrElse(Mscr()) }
  lazy val mscrOpt: CompNode => Option[Mscr] = attr {
    case n => (n -> gbkMscr).orElse(n -> parallelDosMscr).orElse(n -> flattenMscr)
  }

  /** there must be a new Mscr for each related GroupByKey */
  lazy val gbkMscr: CompNode => Option[Mscr] = circular(Option(Mscr.empty)) {
    case g @ GroupByKey(in) => (g -> relatedGbks).map(gbkMscr).flatten.headOption.orElse(Some(Mscr.empty)).map(m =>
                               m.addChannels((g -> inputChannels)  ++ (g -> relatedGbks).flatMap(_ -> inputChannels),
                                             (g -> outputChannels) ++ (g -> relatedGbks).flatMap(_ -> outputChannels)))
    case other              => None
  }

  /** compute the mscr of "floating" parallelDo nodes, sharing the same input */
  lazy val parallelDosMscr: CompNode => Option[Mscr] = attr {
    case pd @ ParallelDo(in,_,_,_,_) if pd -> isFloating => Some(Mscr.floatingParallelDosMscr(pd, (pd -> siblings).collect(isAParallelDo)))
    case other                                           => None
  }

  /** compute the mscr of a "floating" flatten node */
  lazy val flattenMscr: CompNode => Option[Mscr] = attr {
    case f @ Flatten(ins) if f -> isFloating => Some(Mscr.floatingFlattenMscr(f))
    case other                               => None
  }

  /** compute the input channels of a node */
  lazy val inputChannels: CompNode => Set[InputChannel] = attr {
    case n => (n -> mapperInputChannels) ++ (n -> idInputChannels)
  }

  /** the mapper input channel of a node is grouping ParallelDo nodes which are siblings */
  lazy val mapperInputChannels: CompNode => Set[MapperInputChannel] =
    attr {
      case pd @ ParallelDo(in,_,_,_,_) if pd -> isMapper => Set(MapperInputChannel((pd -> siblings).collect(isAParallelDo) + pd))
      case other                                         => other.children.asNodes.flatMap(_ -> mapperInputChannels).toSet
    }

  /**
   * compute the id input channels of a node.
   * An IdInputChannel is a channel for an input node which has no siblings and is connected to a GroupByKey */
  lazy val idInputChannels: CompNode => Set[IdInputChannel] =
    attr {
      case n if (n -> isGbkInput) && !(n -> hasSiblings) && (n -> outputs).exists(isGroupByKey) => Set(IdInputChannel(n))
      case other                                                                                => other.children.asNodes.flatMap(_ -> idInputChannels).toSet
    }

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
      case g @ GroupByKey(in @ Flatten(_))                => Set(GbkOutputChannel(g, flatten = Some(in), combiner = g -> combiner, reducer = g -> reducer))
      case g @ GroupByKey(in)                             => Set(GbkOutputChannel(g, combiner = g -> combiner, reducer = g -> reducer))
      case other                                          => Set()
    }

  /**
   * compute a ByPassOutputChannel for:
   * - each related ParallelDo if it has outputs other than Gbks
   */
  lazy val bypassOutputChannels: CompNode => Set[BypassOutputChannel] =
    attr {
      case pd @ ParallelDo(in,_,_,_,_) if  (pd -> isMapper) &&
                                           (pd -> outputs).exists(isGroupByKey)  &&
                                          !(pd -> outputs).forall(isGroupByKey) => Set(BypassOutputChannel(pd))
      case other                                                                => other.children.asNodes.flatMap(_ -> bypassOutputChannels).toSet
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
      case pd @ ParallelDo(_, mt @ Materialize(_),_,_,_) if envInSameMscr(pd) => false
      case pd @ ParallelDo(cb @ Combine(_,_),_,_,true,_)                      => true
      case pd @ ParallelDo(cb @ Combine(_,_),_,_,_,true)                      => true
      case pd @ ParallelDo(_,_,_,_,_)                                         => (pd -> outputs).isEmpty || (pd -> outputs).exists(isMaterialize)
      case other                                                              => false
    }

  /**
   * @return true if a parallel do and its environment are computed by the same mscr
   */
  def envInSameMscr(pd: ParallelDo[_,_,_]) =
    (pd.env -> descendents).collect(isAGroupByKey).headOption.map { g =>
      (g -> relatedGbks).flatMap(_ -> outputs).contains(pd)
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
      case Combine(GroupByKey(_),_) => true
      case other                    => false
    }

  /** compute the combiner of a Gbk */
  lazy val combiner: GroupByKey[_,_] => Option[Combine[_,_]] =
    attr {
      case g @ GroupByKey(_) if hasParent(g) && (g.parent.asNode -> isCombiner) => Some(g.parent.asInstanceOf[Combine[_,_]])
      case other                                                                => None
    }

  /** compute the reducer of a Gbk */
  lazy val reducer: GroupByKey[_,_] => Option[ParallelDo[_,_,_]] =
    attr {
      case g @ GroupByKey(_) =>
        (g -> parentOpt) match {
          case Some(pd @ ParallelDo(_,_,_,_,_)) if pd -> isReducer => Some(pd)
          case Some(c @ Combine(_,_))                              => (c -> parentOpt).collect { case pd @ ParallelDo(_,_,_,_,_) if pd -> isReducer => pd }
          case other                                               => None
        }
      case other                                                   => None
    }

  /** compute if a ParallelDo or a Flatten is 'floating', i.e. it doesn't have a Gbk in it outputs */
  lazy val isFloating: CompNode => Boolean =
    attr {
      case pd: ParallelDo[_,_,_] => !(pd -> isReducer) && !(pd -> outputs).exists(isGroupByKey)
      case f: Flatten[_]         => !(f -> outputs).exists(isGroupByKey)
    }

  /** compute if a node is the input of a Gbk */
  lazy val isGbkInput: CompNode => Boolean = attr { case node => (node -> outputs).exists(isGroupByKey) }

  /**
   * compute the Gbks related to a given Gbk (through some shared input)
   *
   * This set does not contain the original gbk
   */
  lazy val relatedGbks: GroupByKey[_,_] => Set[GroupByKey[_,_]] =
    attr {
      case g @ GroupByKey(Flatten(ins))               => (g -> siblings).collect(isAGroupByKey) ++
                                                         ins.flatMap(_ -> siblings).flatMap(_ -> outputs).flatMap(out => (out -> outputs) + out).collect(isAGroupByKey)
      case g @ GroupByKey(pd @ ParallelDo(_,_,_,_,_)) => (g -> siblings).collect(isAGroupByKey) ++
                                                         (pd -> siblings).flatMap(_ -> outputs).collect(isAGroupByKey)
      case g @ GroupByKey(_)                          => (g -> siblings).collect(isAGroupByKey)
    }

  /** compute the inputs of a given node */
  lazy val inputs : CompNode => Set[CompNode] = attr { case n  => n.children.asNodes.toSet }

  /**
   *  compute the outputs of a given node.
   *  They are all the parents of the node where the parent inputs contain this node.
   */
  lazy val outputs : CompNode => Set[CompNode] =
    attr {
      case node: CompNode => (node -> parents).collect { case a if (a -> inputs).exists(_ eq node) => a }
    }

  /**
   *  compute the shared input of a given node.
   *  They are all the distinct inputs of a node which are also inputs of another node
   */
  lazy val sharedInputs : CompNode => Set[CompNode] =
    attr {
      case node: CompNode => (node -> inputs).collect { case in if (in -> outputs).filterNot(_ eq node).nonEmpty => in }.toSet
    }

  /**
   *  compute the siblings of a given node.
   *  They are all the nodes which share at least one input with this node
   */
  lazy val siblings : CompNode => Set[CompNode] =
    attr { case node: CompNode => (node -> inputs).flatMap(in => (in -> outputs).filterNot(_ eq node)).toSet }

  /** @return true if a node has siblings */
  lazy val hasSiblings : CompNode => Boolean = attr { case node: CompNode => (node -> siblings).nonEmpty }
  /**
   * compute all the descendents of a node
   * They are all the recursive children reachable from this node */
  lazy val descendents : CompNode => Set[CompNode] =
    attr {
      case node: CompNode => (node.children.asNodes ++ node.children.asNodes.flatMap(_ -> descendents)).toSet
    }

  /** @return a function returning true if one node can be reached from another, i.e. it is in the list of its descendents */
  def canReach(n: CompNode): CompNode => Boolean =
    paramAttr { target: CompNode =>
      node: CompNode => descendents(node).map(_.id).contains(target.id)
    }(n)

  /** compute the ancestors of a node, that is all the direct parents of this node up to a root of the graph */
  lazy val ancestors : CompNode => Set[CompNode] =
    circular(Set[CompNode]()) {
      case node: CompNode => {
        val p = Option(node.parent).toSeq.asNodes
        (p ++ p.flatMap { parent => ancestors(parent) }).toSet
      }
    }

  /** compute all the parents of a given node. A node A is parent of a node B if B can be reached from A */
  lazy val parents : CompNode => Set[CompNode] =
    circular(Set[CompNode]()) {
      case node: CompNode => {
        (node -> ancestors).flatMap { ancestor =>
          ((ancestor -> descendents) + ancestor).filter(canReach(node))
        }
      }
    }

  /** @return an option for the potentially missing parent of a node */
  lazy val parentOpt: CompNode => Option[CompNode] = attr { case n => Option(n.parent).map(_.asNode) }

  /** compute the vertices starting from a node */
  lazy val vertices : CompNode => Seq[CompNode] =
    circular(Seq[CompNode]()) {
      case node: CompNode => (node +: node.children.asNodes.flatMap(n => n -> vertices).toSeq) ++ node.children.asNodes
    }

  /** compute all the edges which compose this graph */
  lazy val edges : CompNode => Seq[(CompNode, CompNode)] =
    circular(Seq[(CompNode, CompNode)]()) {
      case node: CompNode => node.children.asNodes.map(n => node -> n) ++ node.children.asNodes.flatMap(n => n -> edges).toSeq
    }

  /** @return the MSCRs accessible from a given node, without optimising the graph first */
  def makeMscrs(node: CompNode): Set[Mscr] = {
    // make sure that the parent <-> children relationships are initialized
    Attribution.initTree(node)
    ((node -> descendents) + node).map(mscrOpt).flatten.filterNot(_.isEmpty)
  }

  /** @return the first reachable MSCR for a given node, without optimising the graph first */
  private[plan] def makeMscr(node: CompNode) = makeMscrs(node).headOption.getOrElse(Mscr())
  /** @return the MSCR for a given node. Used for testing only because it does the optimisation first */
  private[plan] def mscrFor(node: CompNode) = makeMscr(Optimiser.optimise(node))
  /** @return the MSCRs for a list of nodes. Used for testing only */
  private[plan] def mscrsFor(nodes: CompNode*) = (nodes map mscrFor).toSet
}

object MscrGraph extends MscrGraph