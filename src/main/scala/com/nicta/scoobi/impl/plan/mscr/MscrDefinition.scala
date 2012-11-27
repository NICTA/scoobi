package com.nicta.scoobi
package impl
package plan
package mscr

import org.kiama.attribution.Attribution
import scala.collection.immutable.SortedSet
import scalaz.Scalaz._
import scalaz.syntax.std.indexedSeq._
import control.Functions._
import collection.IdSet
import comp._
import core._

trait MscrsDefinition extends CompNodes with Layering {
  type T = CompNode

  def selectNode(n: CompNode) = isAGroupByKey.isDefinedAt(n) || (n -> isFloating)

  /** a floating node is a parallelDo node or a flatten node that's not connected to a gbk node */
  lazy val isFloating: CompNode => Boolean = attr {
    case pd: ParallelDo[_,_,_] => outputs(pd).collect(isAGroupByKey).isEmpty && outputs(pd).collect(isAFlatten).flatMap(_ -> outputs).collect(isAGroupByKey).isEmpty
    case fl: Flatten[_]        => outputs(fl).collect(isAGroupByKey).isEmpty
    case other                 => false
  }

  /** all the mscrs for a given layer */
  lazy val mscrs: Layer[T] => Seq[Mscr] = attr { case layer =>
    gbkMscrs(layer) ++ pdMscrs(layer) ++ flattenMscrs(layer)
  }

  /** Mscrs for parallel do nodes which are not part of a Gbk mscr */
  lazy val pdMscrs: Layer[T] => Seq[Mscr] = attr { case layer =>
    floatingParallelDos(layer).groupBy(_.in.id).values.toSeq.map { pds =>
      Mscr(MapperInputChannel(pds.toSet), pds.map(BypassOutputChannel(_)))
    }
  }
  /** Mscrs for flatten nodes which are not part of a Gbk mscr */
  lazy val flattenMscrs: Layer[T] => Seq[Mscr] = attr { case layer =>
    floatingFlattens(layer).map { fl =>
      Mscr(fl.ins.map {
        case pd: ParallelDo[_,_,_] => MapperInputChannel(pd)
        case other                 => StraightInputChannel(other)
      },
        FlattenOutputChannel(fl))
    }
  }

  /** Mscrs for mscrs built around gbk nodes */
  lazy val gbkMscrs: Layer[T] => Seq[Mscr] = attr { case layer =>
    val (in, out) = (gbkInputChannels(layer), gbkOutputChannels(layer))
    // groups of input channels having at least one tag in common
    val channelsWithCommonTags = in.toIndexedSeq.groupByM[Id]((i1, i2) => (i1.tags intersect i2.tags).nonEmpty)

    // create Mscr for each set of channels with common tags
    channelsWithCommonTags.map { taggedInputChannels =>
      val correspondingOutputTags = taggedInputChannels.flatMap(_.tags)
      Mscr(taggedInputChannels.toSet, out.filter(o => correspondingOutputTags.contains(o.tag)).toSet)
    }
  }

  /** create a gbk output channel for each gbk in the layer */
  lazy val gbkOutputChannels: Layer[T] => Set[OutputChannel] = {
    val tagger = new Tagger()
    attr { case layer =>
      layer.gbks.map(gbkOutputChannel).map(_.setTag(tagger.newTag))
    }
  }

  /** create a bypass output channel for each parallel do which is an input of a layer but having outputs outside of the layer */
  lazy val bypassOutputChannels: Layer[T] => Set[OutputChannel] = attr { case layer =>
    layerInputs(layer).collect { case pd: ParallelDo[_,_,_] if outputs(pd).filterNot(layerNodes(layer).contains).nonEmpty =>
      BypassOutputChannel(pd)
    }.toSet
  }
  lazy val gbkOutputChannel: GBK => GbkOutputChannel = {
    attr { case g  =>
      val flatten = Seq(g.in).collect(isAFlatten).headOption

      (g -> ancestors).toList match {
        case (c @ Combine1(_)) :: (p @ ParallelDo1(_)) :: rest => GbkOutputChannel(g, flatten, combiner = Some(c), reducer = Some(p))
        case (c @ Combine1(_)) :: rest                         => GbkOutputChannel(g, flatten, combiner = Some(c))
        case (p @ ParallelDo1(_)) :: rest                      => GbkOutputChannel(g, flatten, reducer = Some(p))
        case _                                                 => GbkOutputChannel(g)
      }
    }
  }

  lazy val gbkInputChannels: Layer[T] => Set[InputChannel] = attr { case layer =>
    val channels = mapperInputChannels(layer) ++ idInputChannels(layer)
    val outputs = gbkOutputChannels(layer)
    channels.map(in => in.setTags(outputs.collect { case o if in -> isInputTo(o) => o.tag }))
  }

  lazy val idInputChannels: Layer[T] => Set[IdInputChannel] = attr { case layer =>
    gbkInputs(layer).filter(!isParallelDo).map(i => IdInputChannel(i)).toSet
  }

  lazy val mapperInputChannels: Layer[T] => Set[MapperInputChannel] = attr { case layer =>
    mappers(layer).groupBy(_.in.id).values.map(pds => MapperInputChannel(pds:_*)).toSet
  }

  lazy val mappers: Layer[T] => Seq[ParallelDo[_,_,_]] = attr { case layer =>
    gbkInputs(layer).collect(isAParallelDo).filterNot(_ -> isReducer)
  }

  lazy val layerInputs: Layer[T] => Seq[CompNode] = attr { case layer =>
    layer.nodes.toSeq.flatMap(_ -> inputs).flatMap {
      case Flatten1(ins) => ins
      case other         => Seq(other)
    }
  }

  /** collect all input nodes to the gbks of this layer */
  lazy val gbkInputs: Layer[T] => Seq[CompNode] = attr { case layer =>
    layer.nodes.toSeq.flatMap(_ -> inputs)flatMap {
      case fl @ Flatten1(ins) if layer.gbks.flatMap(_ -> inputs).contains(fl)    => ins
      case other              if layer.gbks.flatMap(_ -> inputs).contains(other) => Seq(other)
      case other                                                                 => Seq()
    }
  }

  lazy val floatingParallelDos: Layer[T] => Seq[ParallelDo[_,_,_]] = floatingNodes(isAParallelDo)

  lazy val floatingFlattens: Layer[T] => Seq[Flatten[_]] = floatingNodes(isAFlatten)

  def floatingNodes[N <: CompNode](pf: PartialFunction[CompNode, N]): Layer[T] => Seq[N] = attr { case layer =>
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
  lazy val layerNodes: Layer[T] => Seq[CompNode] = attr { case layer =>
    gbkOutputChannels(layer).flatMap(_.nodes).toSeq ++ layer.gbks.flatMap {
      case GroupByKey1(flatten @ Flatten1(ins)) => flatten +: ins
      case GroupByKey1(pd: ParallelDo[_,_,_])   => Seq(pd)
      case GroupByKey1(other)                   => Seq()
    }
  }

  /**
   * @return the results for all mscrs of a layer
   */
  lazy val layerResults: Layer[T] => Seq[CompNode] = attr { case layer =>
    mscrs(layer).flatMap(mscrResults)
  }

  /**
   * @return the nodes which might materialize values for a given layer:
   *
   * - load or return nodes which are inputs to mscrs
   * - materialize or op nodes which are outputs of mscrs
   *
   */
  lazy val mscrResults: Mscr => Seq[CompNode] = attr { case mscr =>
    mscr.inputChannels.toSeq.flatMap(_.results) ++
    mscr.outputChannels.toSeq.flatMap(_.results)
  }

  lazy val isInputTo: OutputChannel => InputChannel => Boolean = paramAttr { (out: OutputChannel) => (in: InputChannel) =>
    in.outgoings.exists(out.contains)
  }

  lazy val isReducer: ParallelDo[_,_,_] => Boolean = attr { case pd =>
    (pd -> descendents).collect(isAGroupByKey).map(gbkOutputChannel).exists(_.reducer == Some(pd))
  }

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
trait Layering extends CompNodes with Attribution with ShowNode {

  type T <: CompNode

  /** a function to select only some nodes in the graph. They must be of type T */
  def selectNode(n: CompNode): Boolean

  lazy val selected: CompNode => Boolean = attr { case n => selectNode(n) }
  lazy val select: PartialFunction[CompNode, T] = { case n if n -> selected => n.asInstanceOf[T] }
  lazy val selectedDescendents: CompNode => Seq[T] = attr { case n => (n -> descendents).toSeq.collect(select) }

  lazy val layers: CompNode => Seq[Layer[T]] = attr { case n =>
    val (leaves, nonLeaves) = selectedDescendents(n).partition(d => selectedDescendents(d).isEmpty)
    Layer.create(leaves) +:
      nonLeaves.groupBy(_ => longestPathTo(leaves)).values.map(Layer.create).toSeq.reverse
  }

  lazy val longestPathTo: Seq[CompNode] => CompNode => Int = paramAttr { (target: Seq[CompNode]) => node: CompNode =>
    target.map(t => node -> longestPathToNode(t)).max
  }

  lazy val longestPathToNode: CompNode => CompNode => Int = paramAttr { (target: CompNode) => node: CompNode =>
    if (node.id == target.id)                0  // found
    else if (node.children.asNodes.isEmpty) -1 // not found
    else                                     1 + (node.children.asNodes).map(_ -> longestPathToNode(target)).max
  }

  case class Layer[T <: CompNode](nodes: SortedSet[T] = IdSet.empty) {
    lazy val gbks = nodes.collect(isAGroupByKey)
    override def toString = nodes.mkString("Layer(\n", ",\n", ")\n")
  }

  object Layer {
    def create[T <: CompNode](ts: Seq[T]) = Layer(collection.IdSet(ts:_*))
  }
}
