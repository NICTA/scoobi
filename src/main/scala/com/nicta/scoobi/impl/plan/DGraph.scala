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
package impl
package plan

object DGraph {

  /**
   * Takes the outputs of the Execution Plan and returns a DGraph.
   * The DGraph data structure makes it easy to navigate the
   * abstract syntax tree. This is invaluable in determining
   * the boundaries of intermediate MSCRs (see Intermediate.scala)
   */
  def apply(outputs: Iterable[Smart.DComp[_, _ <: Shape]]): DGraph = {
    /*
     * Add a node as an @input@ for the graph if it has no predecessors
     */
    def addInput(g: DGraph, d: Smart.DComp[_, _ <: Shape]): DGraph = {
      g.preds.get(d) match {
        case Some(_)   => g
        case None      => new DGraph(g.inputs + d, g.outputs, g.nodes, g.succs, g.preds)
      }
    }

    /*
     * Adds all the nodes that are inputs for the execution plan to the @inputs@ field of the graph.
     *
     * This method is idempotent. i.e. g.addInputs.addInputs == g.addInputs
     */
    def addInputs(g: DGraph): DGraph = {
      g.nodes.foldLeft(g)(addInput)
    }

    /*
     * Add the edge (src,tgt) to the graph.
     */
    def addEdge(src: Smart.DComp[_, _ <: Shape], tgt: Smart.DComp[_, _ <: Shape], g: DGraph): DGraph = {
      new DGraph(g.inputs, g.outputs, g.nodes.+(src, tgt), addToAssocMap(src, tgt, g.succs),
                 addToAssocMap(tgt, src, g.preds))
    }

    /*
     * Adds all the edges in the graph that have a path ending in node @tgt@
     */
    def addEdgesEndingAt(g: DGraph, tgt: Smart.DComp[_, _ <: Shape]): DGraph = {
      val parents: Iterable[Smart.DComp[_, _ <: Shape]] = Smart.parentsOf(tgt)
      val thisLevel = parents.foldLeft(g){ case (g,src) => addEdge(src,tgt,g) }
      addEdges(thisLevel, parents)
    }

    /*
     * Adds all the edges in the graph that have paths ending at nodes in @nodes@.
     */
    def addEdges(g: DGraph, nodes: Iterable[Smart.DComp[_, _ <: Shape]]): DGraph = {
      nodes.foldLeft(g)(addEdgesEndingAt)
    }

    /*
     * Adds an association between @a@ and @b@ in association map. An association map
     * is a map from sources to sets of targets.
     *
     * Examples:
     * 1.
     *   m               == Map(A -> Set(B,C,D))
     *   addToAssocMap(A,E,m) == Map(A -> Set(B,C,D,E))
     * 2.
     *   m               == Map(A -> Set(B,C))
     *   addToAssocMap(D,E,m) == Map(A -> Set(B,C), D -> Set(E))
     */
    def addToAssocMap[A,B](a: A, b: B, m: Map[A, Set[B]]): Map[A, Set[B]] = {
      val s:Set[B] = m.get(a) match {
        case Some(s) => s
        case None => Set()
      }
      m + ( (a, s + b) )
    }

    val graphWithNoInputs = addEdges(new DGraph(outputs.toSet),outputs)
    addInputs(graphWithNoInputs)
  }
}

class DGraph(val inputs:  Set[Smart.DComp[_, _ <: Shape]],
             val outputs: Set[Smart.DComp[_, _ <: Shape]],
             val nodes:   Set[Smart.DComp[_, _ <: Shape]],
             val succs:   Map[Smart.DComp[_, _ <: Shape], Set[Smart.DComp[_, _ <: Shape]]],
             val preds:   Map[Smart.DComp[_, _ <: Shape], Set[Smart.DComp[_, _ <: Shape]]]) {

  def this(outputs: Set[Smart.DComp[_, _ <: Shape]]) = this(Set.empty, outputs, Set.empty, Map.empty, Map.empty)

  override def toString() = {
    /* Generate a map from nodes to unique integers on the fly */
    val idSeq = nodes.toIndexedSeq
    val idMap: Map[Smart.DComp[_, _ <: Shape], Int] =
      idSeq.foldLeft(Map():Map[Smart.DComp[_, _ <: Shape], Int])((m,e) => m + ((e, idSeq.indexOf(e))))

    def toStr(e: Smart.DComp[_, _ <: Shape]) = {
      val i = idMap.get(e) match { case Some(i) => i; case None => 0 }
      e + " " + i.toString()
    }

    def setToStr(s: Set[Smart.DComp[_, _ <: Shape]]): String = {
      val xs: List[String] = List()
      val elems = s.foldLeft(xs){ case (xs,d) => xs :+ toStr(d) }
      elems.mkString("{", ", ", "}")
    }

    def mapToStr(m: Map[Smart.DComp[_, _ <: Shape], Set[Smart.DComp[_, _ <: Shape]]]): String = {
      val xs: List[String] = List()
      val elems = m.foldLeft(xs){case (xs,(d,s)) => xs :+ (toStr(d) + " -> " + setToStr(s)) }
      elems.mkString("[", ", ", "]")
    }

    val l = List("inputs  = "          + setToStr(inputs),
                 "         outputs = " + setToStr(outputs),
                 "         nodes   = " + setToStr(nodes),
                 "         succs   = " + mapToStr(succs),
                 "         preds   = " + mapToStr(preds))
    l.mkString("DGraph { ", ",\n", "}")

  }
}
