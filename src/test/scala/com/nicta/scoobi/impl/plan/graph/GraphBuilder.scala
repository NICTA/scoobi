package com.nicta.scoobi
package impl
package plan
package graph

import org.kiama.attribution.Attribution._
import com.github.mdr.ascii.{Box, Diagram, ConnectMode}
import comp.{CompNode, CompNodeFactory}
import org.kiama.attribution.{Attributable, Attribution}
import ConnectMode._

/**
 * This trait builds a CompNode graph from a textual representation
 */
trait GraphBuilder extends CompNodeFactory {
  /** @return the CompNode that is the root of this diagram */
  def diagramRoot(diagram: String): Option[CompNode] = try {
    val all = Diagram(diagram).allBoxes.map(toABox)
    val start = all.find(_.box.connections(In).isEmpty).get
    Option(start -> toCompNode)
  } catch {
    case e => e.printStackTrace(); None
  }

  /** transform a Box object to a Box with it's outgoing connexions which are the inputs of the CompNode */
  lazy val toABox: Box => ABox = { attr { case box => ABox(box, box.connections(Out).map(_._2)) } }

  /** create a CompNode from a Box and its inputs */
  lazy val toCompNode: ABox => CompNode = {
    val nodesMap = new scala.collection.mutable.HashMap[Int, CompNode]()
    attr {
      case ABox(box, ins) => createNode(box, ins.map(i => toABox(i) -> toCompNode), nodesMap)
    }
  }

  /** @return a CompNode from a Box and its inputs. Reuses already created nodes */
  def createNode(box: Box, ins: Seq[CompNode], nodesMap: scala.collection.mutable.HashMap[Int, CompNode]): CompNode = {
    box match {
      case ABoxId("ld",  id) => nodesMap.get(id).getOrElse(load)
      case ABoxId("rt",  id) => nodesMap.get(id).getOrElse(rt)
      case ABoxId("pd",  id) => nodesMap.get(id).getOrElse(pd(ins.head))
      case ABoxId("gbk", id) => nodesMap.get(id).getOrElse(gbk(ins.head))
      case ABoxId("fl",  id) => nodesMap.get(id).getOrElse(flatten(ins:_*))
      case ABoxId("op",  id) => nodesMap.get(id).getOrElse(op(ins(0), ins(1)))
      case ABoxId("mt",  id) => nodesMap.get(id).getOrElse(mt(ins.head))
      case other             => sys.error("no match for "+box)
    }
  }

  trait BoxNode extends Attributable
  case class ABox(box: Box, inputs: Seq[Box]) extends BoxNode
  object ABoxId {
    def unapply(box: Box): Option[(String, Int)] = {
      // regex found with http://www.txt2re.com
      """((?:[a-z][a-z]+))(\d+)""".r.unapplySeq(box.text.trim).map {
        case name :: id :: Nil => (name, id.toInt)
        case other             => sys.error("malformed box name "+box.text+". It should be name+Id")
      }
    }
  }
}
