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
import testing._
import mutable.NictaSimpleJobs

import lib.Relational._
import core.WireFormat
import annotation.tailrec
import com.github.mdr.ascii.Edge

class PageRankSpec extends PageRank {

  "Web pages can be ranked according to their number of incoming and outcoming links" >> { implicit sc: SC =>
    val graph =
      fromInput("1: 2 3 4",
                "2: 3 4",
                "3: 4").collect { case Node(n, rest) => (n.toInt, rest.split(" ").map(_.toInt).toSeq) }

    val urls =
      fromDelimitedInput("1, www.google.com",
                         "2, www.specs2.org",
                         "3, www.notfound.org").collect { case AnInt(id) :: url :: _ => (id, url.trim) }

    getPageRanks(urls, graph).run === Vector(
      ("www.google.com"   -> 0.5f),
      ("www.specs2.org"   -> 7/12f),
      ("www.notfound.org" -> 0.75f))
  }
}

trait PageRank extends NictaSimpleJobs {
  val Node = """^(\d+): (.*)$""".r

  /** a Vertice is described as a page id and a list of incoming links */
  type Vertice[K] = (K, Seq[K])
  /** a Graph is described as a list of vertices */
  type Graph[K] = DList[Vertice[K]]
  /** a Score is: the current page rank, the previous page rank and the list of incoming links */
  type Score[K] =  (Float, Float, Seq[K])
  /** a Ranking is the association of: a vertice and a score */
  type Ranking[K] = (K, Score[K])
  /** list of Rankings */
  type Rankings[K] = DList[Ranking[K]]

  /** initialise the vertices of the graph with default scores */
  def initialise[K : WireFormat](inputs: Graph[K]): Rankings[K] = {
    inputs.map { case (url, links) => (url, (1f, 0f, links)) }
  }

  /** @return the page rank for each url */
  def getPageRanks(urls: DList[(Int, String)], graph: Graph[Int])(implicit configuration: ScoobiConfiguration) = {
    val (_, rankings) = calculateRankings(10.0f, initialise[Int](graph))
    val pageRanks = rankings.map { case (id, (pr,_,_)) => (id, pr) }
    (urls join pageRanks).values
  }

  /** @return new rankings */
  def updateRankings[K](previous: DList[Ranking[K]], d: Float = 0.5f)(implicit configuration: ScoobiConfiguration, wf: WireFormat[K], grouping: Grouping[K]) = {
    val outbound: DList[(K, Float)] = previous flatMap { case t @ (url, (pageRank, _, links)) =>
      links.map { link => (link, pageRank / links.size) }
    }
   (previous coGroup outbound) map { case (url, (prevData, outboundMass)) =>
      val newPageRank = (1 - d) + d * outboundMass.sum
      (url, prevData.headOption.map { case (oldPageRank, _, links) =>
        (newPageRank, oldPageRank, links) }.getOrElse((newPageRank, 0f, Nil)))
    }
  }

  /**
   * @param delta maximum observed value between a new score and an old score
   * @param previous previous set of rankings
   * @return a new delta and new set of rankings
   */
  @tailrec
  private def calculateRankings(delta: Float, previous: Rankings[Int])
                       (implicit configuration: ScoobiConfiguration): (Float, Rankings[Int]) = {
    if (delta <= 1.0f) (delta, previous)
    else {
      val next = updateRankings(previous)
      val maxDelta = next.map { case (_, (n, o, _)) => math.abs(n - o) }.max
      calculateRankings(maxDelta.run, next)
    }
  }
}