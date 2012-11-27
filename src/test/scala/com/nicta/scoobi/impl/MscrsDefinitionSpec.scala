package com.nicta.scoobi
package impl

import org.kiama.attribution.Attribution
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop._
import org.specs2._
import plan.mscr._
import plan.mscr.GbkOutputChannel
import plan.mscr.IdInputChannel
import specification.Groups
import matcher.ThrownExpectations
import plan.comp._
import core._
import testing.UnitSpecification
import collection._
import collection.IdSet._
import scala.collection.immutable.SortedSet
import control.Functions._

class MscrsDefinitionSpec extends UnitSpecification with Groups with ThrownExpectations with CompNodeData { def is =

  "The gbks of a graph can be sorted in layers according to their dependencies"                                         ^
    "all the nodes in a layer cannot be parent of each other"                                                           ! g1().e1^
    "2 different layers have at least 2 nodes parent of each other"                                                     ! g1().e2^
                                                                                                                        endp^
  "For each layer in the topological sort, we can create Mscrs"                                                         ^
    "Output channels"                                                                                                   ^ section("outputs")^
      "each gbk belongs to a GbkOutputChannel"                                                                          ! g2().e1^
      "aggregating the flatten node if there is one before the gbk"                                                     ! g2().e2^
      "aggregating the combine node if there is one after the gbk"                                                      ! g2().e3^
      "aggregating the pd node if there is one after the gbk"                                                           ! g2().e4^
      "aggregating the combine and pd nodes if they are after the gbk"                                                  ! g2().e5^
      "there is a bypass output channel for each mapper having outputs other than a gbk"                                ! g2().e6^
                                                                                                                        endp^ section("outputs")^
    "Input channels"                                                                                                    ^
      "GbkOutputChannels have inputs, some of them are Mappers"                                                         ^
      "all mappers sharing the same input go to the same MapperInputChannel"                                            ! g3().e1^
      "other mappers go to an individual MapperInputChannel"                                                            ! g3().e2^
      "other Gbk inputs go to an IdInputChannel"                                                                        ! g3().e3^
                                                                                                                        endp^
    "Mscr creation"                                                                                                     ^ section("creation")^
      "output channels must have a unique tag"                                                                          ! g4().e1^
      "the set of tags of an input channel must be all the tags of its output channels"                                 ! g4().e2^
      "there must be one mscr per set of related tags"                                                                  ! g4().e3^
      "the mscrs are all the gbk mscrs on the layer + the mscrs for parallel do nodes which are not in a gbk mscr"+
      "+ the mscrs for flatten nodes which are not in a gbk mscr"                                                       ! g4().e4^
                                                                                                                        end


  "layering of Gbk layers" - new g1 with definition { import scalaz.Scalaz._

    e1 := prop { layer: Layer[CompNode] =>
      val nodes = layer.nodes

      nodes must not(beEmpty)
      nodes.forall(n => !nodes.exists(_ -> isStrictParentOf(n))) ==== true
    }.set(minTestsOk -> 100)

    e2 := forAll(genLayerPair) { (pair: (Layer[CompNode], Layer[CompNode])) => val (layer1, layer2) = pair
      val pairs = ^(layer1.gbks.toStream, layer2.gbks.toStream)((_,_))
      val parentChild = pairs.find { case (n1, n2) => !(n1 -> isStrictParentOf(n2)) }
      lazy val showParentChild = parentChild.collect { case (n1, n2) => (showGraph(n1)+"\n"+showGraph(n2)) }.getOrElse("")

      parentChild aka showParentChild must beNone

    }.set(minTestsOk -> 100, maxSize -> 6, maxDiscarded -> 150)

  }

  "Output channels" - new g2 with definition {

    e1 := {
      val gbk1 = gbk(load)
      (gbk1 -> gbkOutputChannel) === GbkOutputChannel(gbk1)
    }
    e2 := {
      val fl1 = flatten(load)
      val gbk1 = gbk(fl1)
      (gbk1 -> gbkOutputChannel) === GbkOutputChannel(gbk1, flatten = Some(fl1))
    }
    e3 := {
      val gbk1 = gbk(load)
      val cb1 = cb(gbk1)
      (gbk1 -> gbkOutputChannel) === GbkOutputChannel(gbk1, combiner = Some(cb1))
    }
    e4 := {
      val gbk1 = gbk(load)
      val pd1 = pd(gbk1)
      (gbk1 -> gbkOutputChannel) === GbkOutputChannel(gbk1, reducer = Some(pd1))
    }
    e5 := {
      val gbk1 = gbk(load)
      val cb1 = cb(gbk1)
      val pd1 = pd(cb1)
      (gbk1 -> gbkOutputChannel) === GbkOutputChannel(gbk1, combiner = Some(cb1), reducer = Some(pd1))
    }
    e6 := {
      val pd1 = pd(load)
      val gbk1 = gbk(pd1)
      val cb1 = cb(pd1)
      val graph = flatten(cb1, gbk1)
      bypassOutputChannels(Layer.create(Seq(gbk1))) === Set(BypassOutputChannel(pd1))
    }
  }

  "Input channels" - new g3 with definition with simpleGraph {

    e1 := {
      val graph = flatten(gbk1, gbk2)
      val ls    = layers(graph)
      val inputChannels: Seq[MapperInputChannel] = mapperInputChannels(ls.head).toSeq

      inputChannels must have size(1)
      inputChannels.head.parDos must have size(2)
    }

    e2 := {
      val graph = flatten(gbk1, gbk2, gbk3)
      val ls    = layers(graph)
      val inputChannels: Seq[MapperInputChannel] = mapperInputChannels(ls.head).toSeq.sortBy(_.parDos.size).reverse

      inputChannels must have size(2)
      inputChannels.head.parDos must have size(2)
      inputChannels.last.parDos must have size(1)
    }

    e3 := {
      val graph = flatten(gbk1, gbk2, gbk3, gbk4)
      val ls    = layers(graph)
      val channels: Seq[InputChannel] = gbkInputChannels(ls.head).toSeq

      channels must have size(3)
    }
  }

  "Mscr creation" - new g4 with definition with simpleGraph {
    lazy val graph  = flatten(gbk1, gbk2, gbk3, gbk4)
    lazy val layer1 = layers(graph).head

    e1 := {
      val tags = gbkOutputChannels(layer1).map(_.tag)
      "there are as many tags as output channels on a layer" ==> {
        tags.toSet must have size(layer1.gbks.size)
      }
    }
    e2 := {
      gbkInputChannels(layer1).toSeq.map(_.tags) === Seq(Set(0, 1, 2), Set(0, 1, 2), Set(3))
    }
    e3 := {
      mscrs(layers(graph).head) must have size(2)
    }
    e4 := {
      val graph2 = flatten(gbk1, gbk2, gbk3, gbk4, pd(load), flatten(load))
      val layer1 = layers(graph2).head

      layers(graph2) must have size(1)
      "there are 4 mscrs in total" ==> { mscrs(layer1) must have size(4) }
      "there is one flatten mscr"  ==> { flattenMscrs(layer1) must have size(1) }
      "there is one pd mscr"       ==> { pdMscrs(layer1) must have size(1) }
      "there are 2 gbk mscrs"      ==> { gbkMscrs(layer1) must have size(2) }
    }
  }

  trait simpleGraph extends nodesFactory {
    val ld1 = load
    val (pd1, pd2, cb1) = (pd(ld1), pd(ld1), cb(load))
    val (gbk1, gbk2, gbk3, gbk4) = (gbk(pd1), gbk(pd2), gbk(pd(load)), gbk(cb1))
  }

  trait definition extends nodesFactory with MscrsDefinition {
    implicit val arbitraryLayer: Arbitrary[Layer[CompNode]] = Arbitrary(genLayer)

    // make sure there is at least one layer
    // by construction, there is no cycle
    val genLayers = arbitraryCompNode.arbitrary.map { n =>
      resetMemo()             // reset the memos otherwise too much data accumulates during testing!
      layers(gbk(pd(gbk(n)))) // generate at least 2 layers
    }
    val genLayer     = genLayers.flatMap(ls => Gen.pick(1, ls)).map(_.head)
    val genLayerPair = genLayers.flatMap(ls => Gen.pick(2, ls)).map { ls => (ls(0), ls(1)) }

    def graph(layer: Layer[CompNode]) =
      if (layer.nodes.isEmpty) "empty layer - shouldn't happen"
      else                     layer.nodes.map(showGraph).mkString("\nshowing graphs for layer\n", "\n", "\n")
  }

  import scalaz.Scalaz._
  import scalaz.syntax.std.indexedSeq._

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
    nonLeaves.groupBy(_ => longestPathTo(leaves)).values.map(Layer.create).toSeq
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