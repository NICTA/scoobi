/**
 * Copyright 2011,2012 National ICT Australia Limited
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
package acceptance

import Scoobi._
import testing.mutable.NictaSimpleJobs
import ShortestPath._
import core.Reduction

class ShortestPathSpec extends NictaSimpleJobs {

  "The shortest path in a graph can be computed using Hadoop" >> { implicit sc: SC =>
    val nodes =
      fromInput("A B", "A C", "C D", "C E", "D E", "F G", "E F", "G E")

    val paths: DList[(String, Int)] = {
      val edges = nodes.map { n => val a :: b :: _ = n.split(" ").toList; (Node(a), Node(b)) }
      val adjacencies = edges.mapFlatten { case (first, second) => List((first, second), (second, first)) }
      val grouped = adjacencies.groupByKey[Node, Node]
      val startingPoint = Node("A")

      val formatted = grouped.map {
        case (node, es) if (node == startingPoint) => (node, NodeInfo(es, Frontier(0)))
        case (node, es)                            => (node, NodeInfo(es, Unprocessed()))
      }

      val iterations = 5
      val breadthResult = breadthFirst(formatted, iterations)

      breadthResult map { case (n, ni) => for { v <- pathSize(ni.state) } yield (n.data, v) } mapFlatten { x => x }
    }

    paths.run.sortBy(_._1) must_== Seq(("A", 0), ("B", 1), ("C", 1), ("D", 2), ("E", 2), ("F", 3), ("G", 3))
  }
}


object ShortestPath {

  implicit val NodeOrderImp = NodeComparator

  implicit val unprocessedFormat           : WireFormat[Unprocessed] = mkCaseWireFormat(Unprocessed, Unprocessed.unapply _)
  implicit val frontierFormat              : WireFormat[Frontier]    = mkCaseWireFormat(Frontier, Frontier.unapply _)
  implicit val doneFormat                  : WireFormat[Done]        = mkCaseWireFormat(Done, Done.unapply _)
  implicit val progressFormat              : WireFormat[Progress]    = mkAbstractWireFormat[Progress, Unprocessed, Frontier, Done]
  implicit val nodeFormat                  : WireFormat[Node]        = mkCaseWireFormat(Node, Node.unapply _)
  implicit val nodeInfoFormat              : WireFormat[NodeInfo]    = mkCaseWireFormat(NodeInfo, NodeInfo.unapply _)

  implicit val nodeGrouping: Grouping[Node] = OrderingGrouping[Node]

  case class Node(data: String)

  sealed trait Progress
  case class Unprocessed()       extends Progress
  case class Frontier(best: Int) extends Progress
  case class Done(best: Int)     extends Progress

  case class NodeInfo (edges: Iterable[Node], state: Progress)

  def breadthFirst(dlist: DList[(Node, NodeInfo)], depth: Int): DList[(Node, NodeInfo)] = {
    val firstMap = dlist.mapFlatten { case (n: Node, ni: NodeInfo) =>
      ni.state match {
        case Frontier(distance) =>
          List((n, NodeInfo(ni.edges, Done(distance)))) ++ ni.edges.map { edge => (edge, NodeInfo(List[Node](), Frontier(distance+1))) }
        case other              => List((n, NodeInfo(ni.edges, other)))
      }
    }

    val firstCombiner = firstMap.groupByKey.combine(Reduction( (n1: NodeInfo, n2: NodeInfo) =>
      NodeInfo(if (n1.edges.isEmpty) n2.edges else n1.edges, furthest(n1.state, n2.state))
    ))
    if (depth > 1) breadthFirst(firstCombiner, depth-1)
    else           firstCombiner
  }

  def furthest(p1: Progress, p2: Progress) = p1 match {
    case Unprocessed() => p2
    case Done(v1)      => Done(v1)
    case Frontier(v1)  => p2 match {
      case Unprocessed() => Frontier(v1)
      case Frontier(v2)  => Frontier(math.min(v1, v2))
      case Done(v2)      => Done(v2)
    }
  }

  def pathSize(p: Progress): Option[Int] = p match {
    case Frontier(v)   => Some(v)
    case Done(v)       => Some(v)
    case Unprocessed() => None
  }

  implicit object NodeComparator extends Ordering[Node] {
    def compare(a: Node, b: Node) = a.data compareTo b.data
  }


}


