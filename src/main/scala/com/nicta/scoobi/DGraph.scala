/**
  * Copyright: [2011] Sean Seefried
  */

package com.nicta.scoobi

object DGraph {

  def apply(outputs: Iterable[Smart.DList[_]]): DGraph = {
    /*
     * Add a node as an @input@ for the graph if it has no predecessors
     */
    def addInput(g: DGraph, d: Smart.DList[_]): DGraph = {
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
    def addEdge(src: Smart.DList[_], tgt: Smart.DList[_], g: DGraph): DGraph = {
      new DGraph(g.inputs, g.outputs, g.nodes.+(src, tgt), addToAssocMap(src, tgt, g.succs),
                 addToAssocMap(tgt, src, g.preds))
    }

    /*
     * Adds all the edges in the graph that have a path ending in node @tgt@
     */
    def addEdgesEndingAt(g: DGraph, tgt: Smart.DList[_]): DGraph = {
      val parents: Iterable[Smart.DList[_]] = Smart.parentsOf(tgt)
      val thisLevel = parents.foldLeft(g){ case (g,src) => addEdge(src,tgt,g) }
      addEdges(thisLevel, parents)
    }

    /*
     * Adds all the edges in the graph that have paths ending at nodes in @nodes@.
     */
    def addEdges(g: DGraph, nodes: Iterable[Smart.DList[_]]): DGraph = {
      nodes.foldLeft(g)(addEdgesEndingAt)
    }

    /*
     * Adds an association between @a@ and @b@ in assocation map. An association map
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

class DGraph(val inputs:  Set[Smart.DList[_]],
             val outputs: Set[Smart.DList[_]],
             val nodes:   Set[Smart.DList[_]],
             val succs:   Map[Smart.DList[_], Set[Smart.DList[_]]],
             val preds:   Map[Smart.DList[_], Set[Smart.DList[_]]]) {

  def this(outputs: Set[Smart.DList[_]]) = this(Set(), outputs, Set(), Map(), Map())

  override def toString() = {
    /* Generate a map from nodes to unique integers on the fly */
    val idSeq = nodes.toIndexedSeq
    val idMap: Map[Smart.DList[_],Int] =
      idSeq.foldLeft(Map():Map[Smart.DList[_], Int])((m,e) => m + ((e, idSeq.indexOf(e))))

    def toStr(e: Smart.DList[_]) = {
      val i = idMap.get(e) match { case Some(i) => i; case None => 0 }
      e.name + " " + i.toString()
    }

    def setToStr(s: Set[Smart.DList[_]]): String = {
      val xs: List[String] = List()
      val elems = s.foldLeft(xs){ case (xs,d) => xs :+ toStr(d) }
      elems.mkString("{", ", ", "}")
    }

    def mapToStr(m: Map[Smart.DList[_], Set[Smart.DList[_]]]): String = {
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
