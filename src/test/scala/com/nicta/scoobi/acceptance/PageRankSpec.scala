package com.nicta.scoobi.acceptance

import PageRank._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.NictaSimpleJobs
import com.nicta.scoobi.ScoobiConfiguration
import com.nicta.scoobi.testing.TestFiles

class PageRankSpec extends NictaSimpleJobs {

  "Web pages can be ranked according to their number of incoming and outcoming links" >> { implicit sc: SC =>
    val graph =
      fromInput("1: 2 3 4",
                "2: 3 4",
                "3: 4").collect { case Node(n, rest) => (n.toInt, rest.split(" ").map(_.toInt).toSeq) }.lines

    val urls =
      fromDelimitedInput("1, www.google.com",
                         "2, www.specs2.org",
                         "3, www.notfound.org").collect { case AnInt(id) :: url :: _ => (id, url.trim) }.lines

    run(getPageRanks(urls, graph)) === Seq(
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

    coGroup(prev, outbound) map { case (url, (prev_data, outbound_mass)) =>
      val new_pr = (1 - d) + d * outbound_mass.sum
      prev_data.toList match {
        case (old_pr, _, links) :: _ => (url, (new_pr, old_pr, links))
        case _                       => (url, (new_pr, 0f,     Nil))
      }
    }
  }

  def latestRankings(outputDir: String, i: Int): DList[(Int, (Float, Float, Seq[Int]))] =
    fromDelimitedTextFile[(Int, (Float, Float, Seq[Int]))](outputDir+"/"+i, ",")({case l =>
      (l(0).toInt, (l(1).toFloat, l(2).toFloat,
       l.drop(3).toSeq.map(_.replace("Vector(", "").replace(")", "").replace(",", "").trim).filterNot(_.isEmpty).map(_.toInt)))})

  def iterateOnce(i : Int)(outputDir: String, graph: DList[(Int, Seq[Int])])
                 (implicit configuration: ScoobiConfiguration): Float = {
    val curr = if (i == 0) initialise(graph) else latestRankings(outputDir, i)
    val next = update(curr, 0.5f)
    val maxDelta = next.map { case (_, (n, o, _)) => math.abs(n - o) }.max.materialize
    persist(configuration)(toDelimitedTextFile(next, outputDir+"/"+(i + 1), ","), maxDelta.use)
    maxDelta.get.head
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

