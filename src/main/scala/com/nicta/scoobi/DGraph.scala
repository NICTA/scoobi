/**
  * Copyright: [2011] Sean Seefried
  */

package com.nicta.scoobi

class DGraph(val inputs:  Set[Smart.DList[_]],
             val outputs: Set[Smart.DList[_]],
             val nodes:   Set[Smart.DList[_]],
             val succs:   Map[Smart.DList[_], Set[Smart.DList[_]]],
             val preds:   Map[Smart.DList[_], Set[Smart.DList[_]]]) {

  def this(outputs: Set[Smart.DList[_]]) = this(Set(), outputs, Set(), Map(), Map())

  def makeDGraph(outputs: List[Smart.DList[_]]): DGraph = {
    val graphWithNoInputs = go(new DGraph(outputs.toSet), outputs)
    addInputs(graphWithNoInputs)
  }

  def addInput(g: DGraph, d: Smart.DList[_]): DGraph = {
    g.preds.get(d) match {
      case Some(_)   => g
      case None      => new DGraph(g.inputs + d, g.outputs, g.nodes, g.succs, g.preds)
    }
  }

  def addInputs(g: DGraph): DGraph = {
    g.nodes.foldLeft(g)(addInput)
  }

  def addEdge(tgt: Smart.DList[_], g: DGraph, src: Smart.DList[_]): DGraph = {
    new DGraph(g.inputs, g.outputs, g.nodes.+(src, tgt), addToMap(src, tgt, g.succs),
               addToMap(tgt, src, g.preds))
  }

  def addEdges(g: DGraph, tgt: Smart.DList[_]): DGraph = {
    val parents: List[Smart.DList[_]] = Smart.parentsOf(tgt)
    val thisLevel = parents.foldLeft(g)(addEdge(tgt,_,_))
    go(thisLevel, parents)
  }

  def go(g: DGraph, nodes: List[Smart.DList[_]]): DGraph = {
    nodes.foldLeft(g)(addEdges)
  }

  private def addToMap[A,B](src: A, tgt: B, m: Map[A, Set[B]]): Map[A, Set[B]] = {
    val s:Set[B] = m.get(src) match {
      case Some(s) => s
      case None => Set()
    }
    m + ( (src, s + tgt) )
  }

}