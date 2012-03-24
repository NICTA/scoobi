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
package com.nicta.scoobi.examples

import com.nicta.scoobi.Scoobi._
import java.io._

object ShortestPath extends ScoobiApp
{
  implicit val NodeOrderImp = NodeComparator

  //implicits for efficient serialization

  implicit val unprocessedFormat = mkCaseWireFormat(Unprocessed, Unprocessed.unapply _) //mkObjectWireFormat(Unprocessed)
  implicit val frontierFormat = mkCaseWireFormat(Frontier, Frontier.unapply _)
  implicit val doneFormat = mkCaseWireFormat(Done, Done.unapply _)

  implicit val progressFormat = mkAbstractWireFormat[Progress, Unprocessed, Frontier, Done]

  implicit val nodeFormat = mkCaseWireFormat(Node, Node.unapply _)
  implicit val nodeInfoFormat = mkCaseWireFormat(NodeInfo, NodeInfo.unapply _)


  // To do a parallel distributed shortest-path search we want to represent the
  // graph with a list of:
  // Node as the Key, NodeInfo as the value

  // A Node is pretty simple, we're just using a String

  case class Node(data: String) // and later we will define an implicit node
                                // NodeComparator, to provide ordering and comparison

  // NodeInfo contains a list of all the nodes that connect to this node, as well
  // as the current state.
  // The possible states are:
  //   Unprocessed. This means we haven't yet got to the node, so we don't know
  //                the shortest way.
  //   Frontier. This means that the next iteration of the breadth first search
  //             should work from here. At this point, we know the shortest path
  //   Done. We have fully discovered this node, and know the shortest path to it

  // This can easily be represented in Scala

  sealed abstract class Progress()

  case class Unprocessed() extends Progress
  case class Frontier(best: Int) extends Progress
  case class Done(best: Int) extends Progress

  case class NodeInfo (edges: Iterable[Node], state: Progress)

  if (!new File("output-dir").mkdir) {
    sys.error("Could not make output-dir for results. Perhaps it already exists (and you should delete/rename the old one)")
  }

  generateGraph("output-dir/graph.txt")

  // The generated graph is in the form of a list of edges e.g (A, B), (C, D)

  val edges: DList[(Node, Node)] = fromDelimitedTextFile("output-dir/graph.txt", " ") {
    case a :: b :: _ => (Node(a), Node(b))
  }

  // This is quite easy to convert to the format we wanted. Given A connecst
  // to B, we know B connects to A, so let's go through and add the reverse:

  val adjacencies: DList[(Node, Node)] = edges.flatMap {
    case (first, second) => List((first, second), (second, first))
  }

  // And now we can group by the first node, and have a list of all its edges
  val grouped: DList[(Node, Iterable[Node])] = adjacencies.groupByKey

  // Since we are calcuating the shortest path, we also need a starting point:
  val startingPoint = Node("A")

  // And now finally, we want it the form "NodeInfo" not just a list of edges:
  val formatted: DList[(Node, NodeInfo)] = grouped.map{
    case (node, edges) if (node == startingPoint) =>
      // it takes 0 steps to get to our starting point
      (node, NodeInfo(edges, Frontier(0)))
    case (node, edges) =>
      // We don't yet know how to get to this node
      (node, NodeInfo(edges, Unprocessed()))
  }

  // Our data is now in a form ready for doing a shortest path search. We will
  // use a breadth first search, with 5 iterations

  val iterations = 5

  // We pass our DList into a function which adds 'iterations' of a breadth first search
  val breadthResult: DList[(Node, NodeInfo)] = breadthFirst(formatted, iterations)

  // And finally, instead of just having the raw information
  // we can convert it to a nice pretty string

  def getV(p: Progress): Option[Int] = p match {
    case Frontier(v) => Some(v)
    case Done(v) => Some(v)
    case Unprocessed() => None
  }

  val prettyResult: DList[String] = breadthResult map {
    case (n: Node, ni: NodeInfo) => getV(ni.state) match {
      case None => "Couldn't get to " + n + " in " + iterations + " steps"
      case Some(v) => "Shortest path from " + startingPoint + " to " + n + " is " + v + " steps"
    }
  }

  // and finally write to an output file

  DList.persist (
     toTextFile(prettyResult, "output-dir/result")
  )


// Here is where all the fun happens.
private def breadthFirst(dlist: DList[(Node, NodeInfo)], depth: Int): DList[(Node, NodeInfo)] = {
  require (depth >= 1)

  // What we want to do, is look at the NodeInfo, and if the state is "Frontier"
  // we know that we are up to this stage of the breadth first search. And we
  // also know the shortest path to it.

  // If it took n steps to get to this node, we know we can get to all its
  // connecting nodes in n+1 steps

  // However, all we really know is that these connecting nodes might be the
  // next  "Frontier" (but maybe not, perhaps we already know the best path to it)
  // and we know a possible cost in getting there (n+1).

  // What we do, is spit out all the information we do know -- and use the
  // combine function to pick the best. We also do not know this edge's edges
  // so we'll leave it as an empty list -- and again, let the combine function
  // clean up after us.

  val firstMap: DList[(Node, NodeInfo)] = dlist.flatMap {
    case (n: Node, ni: NodeInfo) => ni.state match {
      case Frontier(distance) =>
        // If it was a frontier, it will not be in the Done stage for the next iteration
        List((n, NodeInfo(ni.edges, Done(distance)))) ++
          // along with all the information we know about its connecting nodes
          ni.edges.map { edge => (edge, NodeInfo(List[Node](), Frontier(distance+1))) }

       // This isn't a frontier, so return it intact
      case o => List((n, NodeInfo(ni.edges, o)))
    }
  }

  // The previous flatMap made a big mess! Now we're going to have to clean it up
  // We group by the Node again, to get a list of all the spitted out NodeInfos

  val firstGrouped: DList[(Node, Iterable[NodeInfo])] = firstMap.groupByKey

  // And here's the clean up stage. Some of the NodeInfo's aren't going to have
  // the edge-list, so we insert that back in. Along with always picking the best
  // progress (the minimum steps, and the furthest along state)

  // A helper function, to tell us which of the progress is furthest
  def furthest(p1: Progress, p2: Progress) = p1 match {
    case Unprocessed() => p2
    case Frontier(v1) => p2 match {
      case Unprocessed() => Frontier(v1)
      case Frontier(v2) => Frontier(math.min(v1, v2))
      case Done(v2) => Done(v2)
    }
    case Done(v1) => Done(v1)
  }

  val firstCombiner: DList[(Node, NodeInfo)] = firstGrouped.combine {
    (n1: NodeInfo, n2: NodeInfo) =>
      NodeInfo (
       (if (n1.edges.isEmpty)
         n2.edges
       else
         n1.edges),
       furthest(n1.state, n2.state)
     )
 }

 // And this iteration is done.

 if (depth > 1)
    breadthFirst(firstCombiner, depth-1)
  else
    firstCombiner
  }

  object NodeComparator extends Ordering[Node] {
    def compare(a: Node, b: Node) = a.data compareTo b.data
  }

  private def generateGraph(filename: String) {
    // TODO: generate something more interesting..
    val fstream = new FileWriter(filename)
    fstream write ( "A B\nA C\nC D\nC E\nD E\nF G\nE F\nG E" )
    fstream.close()
  }

}

