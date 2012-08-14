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

import java.io.File
import Scoobi._
import testing._
import application.ScoobiConfiguration

import PageRank._
import lib.Relational._

class PageRankSpec extends NictaSimpleJobs {

  skipAll

  "Web pages can be ranked according to their number of incoming and outcoming links" >> { implicit sc: SC =>
    val graph =
      fromInput("1: 2 3 4",
                "2: 3 4",
                "3: 4").collect { case Node(n, rest) => (n.toInt, rest.split(" ").map(_.toInt).toSeq) }

    val urls =
      fromDelimitedInput("1, www.google.com",
                         "2, www.specs2.org",
                         "3, www.notfound.org").collect { case AnInt(id) :: url :: _ => (id, url.trim) }

    getPageRanks(urls, graph).run === Seq(
      "(www.google.com,0.5)",
      "(www.specs2.org,0.5833333)",
      "(www.notfound.org,0.75)")
  }
}

object PageRank {
  val Node = """^(\d+): (.*)$""".r

  def initialise[K : Manifest : WireFormat](input: DList[(K, Seq[K])]) =
    input map { case (url, links) => (url, (1f, 0f, links)) }

  def update[K : Manifest : WireFormat : Grouping](prev: DList[(K, (Float, Float, Seq[K]))], d: Float) = {
    val outbound = prev flatMap { case (url, (pr, _, links)) => links.map((_, pr / links.size)) }

    (prev coGroup outbound) map { case (url, (prev_data, outbound_mass)) =>
      val new_pr = (1 - d) + d * outbound_mass.sum
      prev_data.toList match {
        case (old_pr, _, links) :: _ => (url, (new_pr, old_pr, links))
        case _                       => (url, (new_pr, 0f,     Nil))
      }
    }
  }

  def latestRankings(outputDir: String, i: Int)
                    (implicit configuration: ScoobiConfiguration): DList[(Int, (Float, Float, Seq[Int]))] =
    fromAvroFile(TestFiles.path(outputDir+"/"+i))

  def iterateOnce(i : Int)(outputDir: String, graph: DList[(Int, Seq[Int])])
                 (implicit configuration: ScoobiConfiguration): Float = {
    val curr = if (i == 0) initialise(graph) else latestRankings(outputDir, i)
    val next = update(curr, 0.5f)
    val maxDelta = next.map { case (_, (n, o, _)) => math.abs(n - o) }.max
    val (_, d) = persist(configuration)(toAvroFile(next, outputFile(outputDir, i + 1)), maxDelta)
    d
  }

  /**
   * create an output file name and register it for deletion at the end of the program
   */
  def outputFile(dir: String, i: Int)(implicit configuration: ScoobiConfiguration) = {
    val path = TestFiles.path(dir+"/"+i)
    TestFiles.registerFile(new File(path))
    path
  }

  def getPageRanks(urls: DList[(Int, String)], graph: DList[(Int, Seq[Int])])(implicit configuration: ScoobiConfiguration) = {
    var i = 0
    var delta = 10.0f
    val outputDir = TestFiles.createTempDir("test.iterated").getPath

    while (delta > 1.0f) {
      delta = iterateOnce(i)(outputDir, graph)(configuration)
      i += 1
    }
    val pageRanks = latestRankings(outputDir, i).map { case (id, (pr, _, _)) => (id, pr) }

    join(urls, pageRanks).values
  }
}

