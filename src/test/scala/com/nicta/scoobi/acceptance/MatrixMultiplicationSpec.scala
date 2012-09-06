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

import org.specs2.ScalaCheck
import testing.NictaSimpleJobs
import application.ScoobiConfiguration
import lib.DMatrix
import org.scalacheck.Gen
import org.scalacheck.Prop
import Scoobi._
import org.apache.commons.math.linear.FieldMatrixPreservingVisitor
import org.apache.commons.math.linear.FieldMatrix
import org.apache.commons.math.linear.SparseFieldMatrix
import org.apache.commons.math.util.BigReal
import org.apache.commons.math.util.BigRealField
import org.specs2.mutable.Tags

class MatrixMultiplicationSpec extends NictaSimpleJobs with ScalaCheck with Tags {
  
  tag("MatrixMultiplication")
  "Matrix multiplication should work" >> { implicit sc: ScoobiConfiguration =>
    Prop.forAll(genMatrixData, genMatrixData)(runTest).set('minTestsOk -> 1)
  }

  def runTest(matrix1: Iterable[MatrixEntry], matrix2: Iterable[MatrixEntry])(implicit sc: ScoobiConfiguration): Boolean = {

    val res = toDMatrix(matrix1) byMatrix (toDMatrix(matrix2), mult, add) // normal multiplication
    val ref = toApacheMatrix(matrix1).multiply(toApacheMatrix(matrix2)) // sanity check

    toEntrySet(res) == toEntrySet(ref)
  }

  def mult(x: Int, y: Int) = x * y
  def add(x: Int, y: Int) = x + y

  def toDMatrix(m: Iterable[MatrixEntry])(implicit sc: ScoobiConfiguration): DMatrix[Int, Int] =
    fromDelimitedInput(m.map(entry => entry.row + "," + entry.col + "," + entry.value).toSeq: _*).collect { case AnInt(r) :: AnInt(c) :: AnInt(v) :: _ => ((r, c), v) }

  def toEntrySet(m: DMatrix[Int, Int])(implicit conf: ScoobiConfiguration) =
    Set[MatrixEntry]() ++ Scoobi.persist(m.materialize).collect { case ((r, c), v) if v != 0 => MatrixEntry(r, c, v) }

  def toEntrySet(m: FieldMatrix[BigReal]): Set[MatrixEntry] = {

    class SetAdder(var ms: Set[MatrixEntry] = Set[MatrixEntry]()) extends FieldMatrixPreservingVisitor[BigReal] {
      def start(rows: Int, columns: Int, startRows: Int, endRow: Int, startColumn: Int, endColumn: Int) {}
      def end(): BigReal = null
      def visit(row: Int, col: Int, value: BigReal) = {

        val v = value.bigDecimalValue.intValueExact

        if (v != 0)
          ms = ms + MatrixEntry(row, col, v)
      }
    }

    val sa = new SetAdder()

    m.walkInOptimizedOrder(sa)

    sa.ms
  }

  case class MatrixEntry(row: Int, col: Int, value: Int)

  val matrixMaxSize = 200 // Needs to be pretty small, for apache's maths lib
  val dimensionsInt = Gen.choose(0, matrixMaxSize - 1) // value given to a row or column

  val valueInt = Gen.choose(0, 50) // I want this to be pretty small, so as to not have to worry about overflow differences

  private def genMatrixDataMap: Gen[Map[(Int, Int), Int]] = Gen.listOf(for {
    row <- dimensionsInt
    col <- dimensionsInt
    value <- valueInt
  } yield ((row, col), value)).map(_.toMap)

  def genMatrixData: Gen[Iterable[MatrixEntry]] = genMatrixDataMap.map(m => for { entry <- m } yield MatrixEntry(entry._1._1, entry._1._2, entry._2))

  def toApacheMatrix(d: Iterable[MatrixEntry]): SparseFieldMatrix[BigReal] = {
    val sfm = new SparseFieldMatrix[BigReal](BigRealField.getInstance, matrixMaxSize, matrixMaxSize)

    d.foreach { q =>
      sfm.setEntry(q.row, q.col, new BigReal(q.value))
    }

    sfm
  }

}
