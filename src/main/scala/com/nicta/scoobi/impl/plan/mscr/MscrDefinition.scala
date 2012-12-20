package com.nicta.scoobi
package impl
package plan
package mscr

import scala.collection.immutable.Set
import scalaz.Scalaz._
import scalaz.syntax.std.indexedSeq._
import control.Functions._
import comp._
import core._
import util.UniqueId

trait MscrsDefinition extends Layering {

  def selectNode(n: CompNode) = isAGroupByKey.isDefinedAt(n) || (n -> isFloating)

  /** a floating node is a parallelDo node or a flatten node that's not connected to a gbk node */
  lazy val isFloating: CompNode => Boolean = attr("isFloating") {
    case pd: ParallelDo[_,_,_] => !isReducer(pd) &&
                                  outputs(pd).collect(isAGroupByKey).isEmpty &&
                                  outputs(pd).collect(isAFlatten).forall(!isFloating)
    case fl: Flatten[_]        => outputs(fl).collect(isAGroupByKey).isEmpty
    case other                 => false
  }

  /** all the mscrs for a given layer */
  lazy val mscrs: Layer[T] => Seq[Mscr] = attr("mscrs") { case layer =>
    gbkMscrs(layer) ++ pdMscrs(layer) ++ flattenMscrs(layer)
  }

  /** Mscrs for parallel do nodes which are not part of a Gbk mscr */
  lazy val pdMscrs: Layer[T] => Seq[Mscr] = attr("parallelDo mscrs") { case layer =>
    Vector(floatingParallelDos(layer).groupBy(_.in.id).values.toSeq:_*).map { pds =>
      Mscr(MapperInputChannel.create(pds, sources), pds.map(BypassOutputChannel(_)))
    }
  }
  /** Mscrs for flatten nodes which are not part of a Gbk mscr */
  lazy val flattenMscrs: Layer[T] => Seq[Mscr] = attr("flatten mscrs") { case layer =>
    floatingFlattens(layer).map { fl =>
      Mscr(fl.ins.filter {
        case pd: ParallelDo[_,_,_] => !isReducer(pd)
        case other                 => true
      }.map {
         case pd: ParallelDo[_,_,_] if !isReducer(pd) => MapperInputChannel.create(Seq(pd), sources)
         case other                                   => StraightInputChannel(other, sources)
      },
        FlattenOutputChannel(fl))
    }
  }

  /** Mscrs for mscrs built around gbk nodes */
  lazy val gbkMscrs: Layer[T] => Seq[Mscr] = attr("gbk mscrs") { case layer =>
    val (in, out) = (gbkInputChannels(layer), gbkOutputChannels(layer))
    // groups of input channels having at least one tag in common
    val channelsWithCommonTags = in.toIndexedSeq.groupByM[Id]((i1, i2) => (i1.nodesTags intersect i2.nodesTags).nonEmpty)

    // create Mscr for each set of channels with common tags
    channelsWithCommonTags.map { taggedInputChannels =>
      val correspondingOutputTags = taggedInputChannels.flatMap(_.nodesTags)
      Mscr(taggedInputChannels, out.filter(o => correspondingOutputTags.contains(o.tag)))
    }
  }

  /** create a gbk output channel for each gbk in the layer */
  lazy val gbkOutputChannels: Layer[T] => Seq[OutputChannel] = {
    val tagger = new Tagger()
    attr("gbk output channels") { case layer =>
      layer.gbks.map(gbkOutputChannel).map(_.setTag(tagger.newTag))
    }
  }

  /** create a bypass output channel for each parallel do which is an input of a layer but having outputs outside of the layer */
  lazy val bypassOutputChannels: Layer[T] => Seq[OutputChannel] = attr("bypass output channels") { case layer =>
    layerInputs(layer).collect { case pd: ParallelDo[_,_,_] if outputs(pd).filterNot(layerNodes(layer).contains).nonEmpty =>
      BypassOutputChannel(pd)
    }
  }
  lazy val gbkOutputChannel: GroupByKey[_,_] => GbkOutputChannel = {
    attr("gbk output channel") { case g  =>
      val flatten = Seq(g.in).collect(isAFlatten).headOption

      val gbkAncestors = (g -> parents).toList
      gbkAncestors match {
        case (c: Combine[_,_]) :: (p: ParallelDo[_,_,_]) :: rest if !hasComputedEnv(p) => GbkOutputChannel(g, flatten, combiner = Some(c), reducer = Some(p))
        case (p: ParallelDo[_,_,_]) :: rest                      if !hasComputedEnv(p) => GbkOutputChannel(g, flatten, reducer = Some(p))
        case (c: Combine[_,_]) :: rest                                                 => GbkOutputChannel(g, flatten, combiner = Some(c))
        case _                                                                         => GbkOutputChannel(g)
      }
    }
  }

  lazy val gbkInputChannels: Layer[T] => Seq[InputChannel] = attr("gbk input channels") { case layer =>
    val channels = Vector(mapperInputChannels(layer) ++ idInputChannels(layer):_*)
    val outputs = gbkOutputChannels(layer)
    channels.map { in =>
      val groupByKey = outputs.collect { case o: GbkOutputChannel => o.groupByKey }.head
      val inputWithGroupByKey = in match {
        case i: MapperInputChannel => i.copy(gbk = Some(groupByKey))
        case i: IdInputChannel     => i.copy(gbk = Some(groupByKey))
      }
      lazy val tags: CompNode => Set[Int] = attr("tags") {
        case node => outputs.collect { case o if node -> isInputTo(o) => o.tag }.toSet
      }
      inputWithGroupByKey.setTags(tags)
    }
  }

  lazy val idInputChannels: Layer[T] => Seq[IdInputChannel] = attr("id input channels") { case layer =>
    gbkInputs(layer).filter {
      case p: ParallelDo[_,_,_] => isReducer(p)
      case other                => true
    }.map(i => IdInputChannel(i, sources))
  }

  lazy val mapperInputChannels: Layer[T] => Seq[MapperInputChannel] = attr("mapper input channels") { case layer =>
    mappers(layer).groupBy(_.in.id).values.toSeq.map(pds => MapperInputChannel.create(pds, sources))
  }

  lazy val mappers: Layer[T] => Seq[ParallelDo[_,_,_]] = attr("parallelDo mappers") { case layer =>
    gbkInputs(layer).collect(isAParallelDo).filterNot(_ -> isReducer)
  }

  lazy val layerInputs: Layer[T] => Seq[CompNode] = attr("layer inputs") { case layer =>
    layer.nodes.toSeq.flatMap(_ -> inputs).flatMap {
      case Flatten1(ins) => ins
      case other         => Seq(other)
    }.distinct
  }

  /** collect all input nodes to the gbks of this layer */
  lazy val gbkInputs: Layer[T] => Seq[CompNode] = attr("gbk inputs") { case layer =>
    layer.nodes.toSeq.flatMap(_ -> inputs).flatMap {
      case fl: Flatten[_] if layer.gbks.flatMap(_ -> inputs).contains(fl)    => fl.ins
      case other          if layer.gbks.flatMap(_ -> inputs).contains(other) => Seq(other)
      case other                                                             => Seq()
    }.distinct
  }

  lazy val floatingParallelDos: Layer[T] => Seq[ParallelDo[_,_,_]] = floatingNodes(isAParallelDo)

  lazy val floatingFlattens: Layer[T] => Seq[Flatten[_]] = floatingNodes(isAFlatten)

  def floatingNodes[N <: CompNode](pf: PartialFunction[CompNode, N]): Layer[T] => Seq[N] = attr("floating nodes") { case layer =>
    layer.nodes.collect(pf).filter(isFloating).toSeq
  }

  /**
   * all the nodes which are conceptually part of a layer:
   *
   * - the parallel dos before a gbk
   * - the flatten nodes before a gbk
   * - the gbks
   * - the combine, flatten and reducer nodes after the gbk
   */
  lazy val layerNodes: Layer[T] => Seq[CompNode] = attr("layer nodes") { case layer =>
    gbkOutputChannels(layer).flatMap(_.nodes) ++
    layer.gbks.flatMap {
      case GroupByKey1(flatten @ Flatten1(ins)) => flatten +: ins
      case GroupByKey1(pd: ParallelDo[_,_,_])   => Seq(pd)
      case GroupByKey1(other)                   => Seq()
    }.distinct
  }

  /** @return the sources nodes for all mscrs of a layer */
  lazy val layerSourceNodes: Layer[T] => Seq[CompNode] = attr("layer source nodes") { case layer =>
    mscrs(layer).flatMap(mscrSourceNodes).distinct
  }

  /** @return the sources for all mscrs of a layer */
  lazy val layerSources: Layer[T] => Seq[Source] = attr("layer sources") { case layer =>
    mscrs(layer).flatMap(_.inputChannels).flatMap(_.sources)
  }

  /** @return the sinks nodes for all mscrs of a layer */
  lazy val layerSinkNodes: Layer[T] => Seq[CompNode] = attr("layer sink nodes") { case layer =>
    mscrs(layer).flatMap(mscrSinkNodes).distinct
  }

  /** @return the sinks for all mscrs of a layer */
  lazy val layerSinks: Layer[T] => Seq[Sink] = attr("layer sinks") { case layer =>
    mscrs(layer).flatMap(_.outputChannels).flatMap(_.sinks)
  }
  /**
   * @return the nodes which might materialize input sources for a given layer:
   *
   * - load, return or op nodes which are inputs to mscr input channels or output channels (for a pd environment for example)
   *
   */
  lazy val mscrSourceNodes: Mscr => Seq[CompNode] = attr("mscr source nodes") { case mscr =>
    val inputChannelsSources = mscr.inputChannels.flatMap(_.sourceNodes)
    val outputChannelsSources = mscr.outputChannels.flatMap(_.sourceNodes)
    (inputChannelsSources ++ outputChannelsSources).distinct
  }

  lazy val sources: InputChannel => Seq[Source] = attr("input channel sources") { case in =>
    in.nodes.head match {
      case n: Load[_]           => Seq(n.source)
      case n: GroupByKey[_,_]   => Seq(n.bridgeStore).flatten
      case n: Combine[_,_]      => Seq(n.bridgeStore).flatten
      case n: ParallelDo[_,_,_] if isReducer(n) => Seq(n.bridgeStore).flatten
      case n                  => children(n).flatMap {
        case ld: Load[_] => Some(ld.source)
        case other       => other.bridgeStore
      }
    }
  }


  /**
   * @return the nodes which might materialize output values for a given layer:
   *
   */
  lazy val mscrSinkNodes: Mscr => Seq[CompNode] = attr("mscr sink nodes") { case mscr =>
    mscr.outputChannels.toSeq.flatMap(_.sinkNodes).distinct
  }

  lazy val isInputTo: OutputChannel => CompNode => Boolean = paramAttr("is a node input to an output channel") { (out: OutputChannel) => (node: CompNode) =>
    outgoings(node).exists {
      case fl: Flatten[_] => parent(fl).map(out.contains).getOrElse(false)
      case other          => out.contains(other)
    }
  }

  lazy val isReducer: ParallelDo[_,_,_] => Boolean = attr("isReducer") { case pd =>
    (pd -> descendents).collect(isAGroupByKey).map(gbkOutputChannel).exists(_.reducer == Some(pd))
  }

  lazy val hasComputedEnv: ParallelDo[_,_,_] => Boolean = attr("hasComputedEnv") { case pd =>
    isMaterialize(pd.env) || isOp(pd.env)
  }

  /**
   * create a new tag each time the newTag method is invoked
   */
  case class Tagger() {
    var tag = 0
    def newTag = {
      val t = tag
      tag += 1
      t
    }
  }

}

/**
 * Simple layering algorithm using the Longest path method to assign nodes to layers.
 *
 * See here for a good overview: http://www.cs.brown.edu/~rt/gdhandbook/chapters/hierarchical.pdf
 *
 * In our case the layers have minimum height and possibly big width which is actually good if we run things in parallel
 */
trait Layering extends ShowNode {

  type T <: CompNode

  /** a function to select only some nodes in the graph. They must be of type T */
  def selectNode(n: CompNode): Boolean

  lazy val selected: CompNode => Boolean = attr("selected node") { case n => selectNode(n) }
  lazy val select: PartialFunction[CompNode, T] = { case n if n -> selected => n.asInstanceOf[T] }

  lazy val selectedDescendents: CompNode => Seq[T] = attr("selected descendents") { case n =>
    (n -> descendents).collect(select)
  }

  lazy val layers: CompNode => Seq[Layer[T]] = attr("layers") { case n =>
    val (leaves, nonLeaves) = selectedDescendents(n).partition { d =>
      selectedDescendents(d).isEmpty
    }
    val leaf = if (leaves.isEmpty && selectNode(n)) Seq(select(n)) else Seq()
    Layer(leaves ++ leaf) +:
      nonLeaves.groupBy(_ -> longestPathTo(leaves)).toSeq.sortBy(_._1).map { case (k, v) => Layer(v) }
  }

  lazy val longestPathTo: Seq[CompNode] => CompNode => Int = paramAttr("longestPathToNodeFromSomeNodes") { (target: Seq[CompNode]) => node: CompNode =>
    target.map(t => node -> longestPathToNode(t)).max
  }

  lazy val longestPathToNode: CompNode => CompNode => Int = paramAttr("longestPathToNodeFromOneNode") { (target: CompNode) => node: CompNode =>
    if (node.id == target.id)        0  // found
    else if (children(node).isEmpty) -1 // not found
    else                             1 + children(node).map(_ -> longestPathToNode(target)).max
  }

  /**
   * A layer contains group by keys and floating nodes defining mscrs so that none of them have dependencies relationship
   *
   * Because of this property they can be executed in parallel
   */
  case class Layer[T <: CompNode](nodes: Seq[T] = Seq[T]()) {
    val id = UniqueId.get
    lazy val gbks = nodes.collect(isAGroupByKey)
    override def toString = nodes.mkString("Layer("+id+"\n  ", ",\n  ", ")\n")
  }

}
