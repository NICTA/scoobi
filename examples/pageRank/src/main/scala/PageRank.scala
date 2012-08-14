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

object PageRank extends ScoobiApp {

  /* Load raw graph from a file. */
  private def loadGraph(path: String): DList[(Int, List[Int])] = {
    val Node = """^(\d+): (.*)$""".r
    val lines = fromTextFile(path)
    lines collect { case Node(n, rest) => (n.toInt, rest.split(" ").map(_.toInt).toList) }
  }


  /* Get graph data into correct form. */
  private def initialise[K : Manifest : WireFormat](input: DList[(K, List[K])]) =
    input map { case (url, links) => (url, (1f, 0f, links)) }


  /* Perform a single iteration of page-rank. */
  private def update[K : Manifest : WireFormat : Grouping](prev: DList[(K, (Float, Float, List[K]))], d: Float) = {

    val outbound = prev flatMap { case (url, (pr, _, links)) => links.map((_, pr / links.size)) }

    (prev coGroup outbound) map { case (url, (prev_data, outbound_mass)) =>
      val new_pr = (1 - d) + d * outbound_mass.sum
      prev_data.toList match {
        case (old_pr, _, links) :: _ => (url, (new_pr, old_pr, links))
        case _                       => (url, (new_pr, 0f,     Nil))
      }
    }
  }


  def run() {
    val names = args(0)
    val graph = args(1)
    val output = args(2) + "/pr/"

    def latestRankings(i: Int): DList[(Int, (Float, Float, List[Int]))] = fromAvroFile(output + i)

    /* Perform a single iteration of PageRank. */
    def iterateOnce(i : Int): Float = {
      val curr = if (i == 0) initialise(loadGraph(graph)) else latestRankings(i)
      val next = update(curr, 0.5f)
      val maxDelta = next.map { case (_, (n, o, _)) => math.abs(n - o) } .max
      val (_, md) = persist(toAvroFile(next, output + (i + 1)), maxDelta)
      println("Current delta = " + md)
      md
    }

    /* Iterate until convergence. */
    var i = 0
    var delta = 10.0f
    while (delta > 1.0f) { delta = iterateOnce(i); i += 1 }


    /* Write out final results to text file */
    val pageranks = latestRankings(i).map { case (id, (pr, _, _)) => (id, pr) }
    val urls = fromDelimitedTextFile(names) { case AnInt(id) :: url :: _ => (id, url) }
    persist(toDelimitedTextFile((urls join pageranks).values, output + "result"))
  }
}
