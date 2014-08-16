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

import testing.mutable.NictaSimpleJobs
import Scoobi._
import Reduction._
import math.sqrt
import Correlations._
import Movies._

class ScaldingSpec extends NictaSimpleJobs {
  "Translation of http://blog.echen.me/2012/02/09/movie-recommendations-and-more-via-mapreduce-and-scalding to Scoobi" >> { implicit sc: SC =>

    implicit val IntOrdering = scalaz.Order.fromScalaOrdering[Int]

    // very small list of ratings
    val ratings = DList(
      Rating(1, 1, 1),
      Rating(1, 2, 2),
      Rating(1, 3, 3),
      Rating(2, 1, 5),
      Rating(2, 2, 10),
      Rating(2, 3, 20),
      Rating(3, 1, 10),
      Rating(3, 2, 20),
      Rating(3, 3, 30))

    val numRaters: DList[(Movie, Int)] = ratings.groupBy(_.movie).map { case (m, rs) => (m, rs.size) }
    val ratingsWithSize: DList[SizedRating] = (ratings.map(r => (r.movie, r)) join numRaters).map(_._2)
    val byUser: DList[(User, SizedRating)] = ratingsWithSize.map(r => (r._1.user, r))

    val ratingPairs: DList[(SizedRating, SizedRating)] =
      (byUser join byUser).filter { case (user, ((r1, size1), (r2, size2))) => r1.movie < r2.movie }.groupByKey.map(_._2).mapFlatten(identity)

    // Compute (x*y, x^2, y^2,...), which we need for dot products and norms.
    val vectorCalcs = ratingPairs.map { case ((r1 @ Rating(_,_,rating1), numRating1), (r2 @ Rating(_,_,rating2), numRating2)) =>
      ((r1.movie, r2.movie), (1, rating1 * rating2, rating1, rating2, square(rating1), square(rating2), numRating1, numRating2))
    }.groupByKey
    .combine {
      Sum.int.zip8(   // size
      Sum.int,        // dot product
      Sum.int,        // sum of rating1
      Sum.int,        // sum of rating2
      Sum.double,     // sum of squares for rating1
      Sum.double,     // sum of squares for rating2
      maximum[Int],   // number of ratings for movie1
      maximum[Int])   // number of ratings for movie2
    }

    val correlations = vectorCalcs.map { case ((m1, m2), (size, dotProduct, rating1Sum, rating2Sum, rating1NormSq, rating2NormSq, numRating1, numRating2)) =>
      (correlation(size, dotProduct, rating1Sum, rating2Sum, rating1NormSq, rating2NormSq),
       cosineSimilarity(dotProduct, rating1NormSq, rating2NormSq),
       regularizedCorrelation(size, dotProduct, rating1Sum, rating2Sum, rating1NormSq, rating2NormSq, 10.0, 0.0),
       jaccardSimilarity(size, numRating1, numRating2))
    }

    // show the correlations in the console
    // correlations.run.mkString("\n").pp
    ok
  }

}

object Correlations {
  def correlation(size: Int, dotProduct: Int, rating1Sum: Int, rating2Sum: Int, rating1Norm: Double, rating2Norm: Double) = {
    val numerator = size * dotProduct - rating1Sum * rating2Sum
    val denominator = sqrt(size * rating1Norm - rating1Sum * rating1Sum) * sqrt(size * rating2Norm - rating2Sum * rating2Sum)

    numerator / denominator
  }

  def cosineSimilarity(dotProduct: Double, rating1Norm: Double, rating2Norm: Double) = {
    dotProduct / (rating1Norm * rating2Norm)
  }

  def regularizedCorrelation(size: Int, dotProduct: Int, rating1Sum: Int,
                             rating2Sum: Int, rating1Norm: Double, rating2Norm: Double,
                             virtualCount: Double, priorCorrelation: Double) = {

    val unregularizedCorrelation = correlation(size, dotProduct, rating1Sum, rating2Sum, rating1Norm, rating2Norm)
    val w = size / (size + virtualCount)

    w * unregularizedCorrelation + (1 - w) * priorCorrelation
  }

  def jaccardSimilarity(usersInCommon: Double, totalUsers1: Double, totalUsers2: Double) = {
    val union = totalUsers1 + totalUsers2 - usersInCommon
    usersInCommon / union
  }

  def square(d: Double) = math.pow(d, 2)

}
object Movies {
  type User        = Int
  type Movie       = Int
  type MovieRating = Int

  // a Rating for the movie and the number of raters having rated it
  type SizedRating = (Rating, Int)

  implicit def ratingWireFormat: WireFormat[Rating] = mkCaseWireFormat(Rating.apply _, Rating.unapply _)

  case class Rating(user: User, movie: Movie, rating: MovieRating)
}
