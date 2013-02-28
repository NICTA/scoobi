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

import org.apache.commons.math.linear.FieldMatrixPreservingVisitor
import org.apache.commons.math.linear.FieldMatrix
import org.apache.commons.math.linear.SparseFieldMatrix
import org.apache.commons.math.linear.RealMatrix
import org.apache.commons.math.linear.Array2DRowRealMatrix
import org.apache.commons.math.linear.LUDecompositionImpl
import org.apache.commons.math.util.BigReal
import org.apache.commons.math.util.BigRealField
import org.specs2.ScalaCheck
import org.scalacheck.Gen
import org.scalacheck.Prop

import testing.mutable.NictaSimpleJobs
import lib.{DMatrix, LinearAlgebra}
import Scoobi._
import core.Reduction, Reduction._

import java.util.Random

class MatrixMultiplicationSpec extends NictaSimpleJobs with ScalaCheck {
  /*
  skipAll
  "Sparse Int Matrix multiplication should work" >> { implicit sc: ScoobiConfiguration =>
    Prop.forAll(genIntSparseMatrixData, genIntSparseMatrixData)(runMultTest).set(minTestsOk -> 1)
  }

  Seq(5, 50, 200) foreach { size =>
    "Dense Double Matrix of size " + size + "x" + size + " by Gaussian Random matrix" >> { implicit sc: ScoobiConfiguration =>
      Prop.forAll(genDoubleDenseMatrixData)(runRandomMultTest).set(minTestsOk -> 1, minSize -> size, maxSize -> size)
    }
  }

  /** To validate that the matrix by random works, we need to find the random matrix and perform the
   *  same multiplication using the apache lib, then compare the results */
  def runRandomMultTest(matrix: Iterable[MatrixEntry[Double]])(implicit sc: ScoobiConfiguration): Boolean = {
    val apacheMatrix = toApacheRealMatrix(matrix)
    val randomMatrixWidth = (math sqrt apacheMatrix.getColumnDimension).toInt

    val resultMatrix = LinearAlgebra.matrixByDenseFunc(
        toDoubleDMatrix(matrix),
        randRowValGenerator(randomMatrixWidth)_,
        mult[Double],
        add[Double])

    val apacheResultMatrix = toApacheRealMatrix(toEntrySet(resultMatrix))
    val randomMatrix = getRandomMatrix(apacheMatrix, apacheResultMatrix)

    //precision is lost when finding the random matrix, so the two will not be identical
    fuzzyEqual(apacheResultMatrix, apacheMatrix.multiply(randomMatrix))
  }

  def runMultTest(matrix1: Iterable[MatrixEntry[Int]], matrix2: Iterable[MatrixEntry[Int]])(implicit sc: ScoobiConfiguration): Boolean = {
    val res: DMatrix[Int, Int] = toIntDMatrix(matrix1) byMatrix (toIntDMatrix(matrix2), mult[Int], add[Int]) // normal multiplication
    val ref = toApacheMatrix(matrix1).multiply(toApacheMatrix(matrix2)) // sanity check

    toEntrySet(res) == toIntEntrySet(ref)
  }

  def mult[T: Numeric](x: T, y: T) = implicitly[Numeric[T]].times(x,y)
  def add[T: Numeric](x: T, y: T) = implicitly[Numeric[T]].plus(x,y)

  def randRowValGenerator(width: Int)(): Seq[Double] = {
    val rand = new Random
    for (i <- 0 until width) yield rand.nextGaussian()
  }

  def toIntDMatrix(m: Iterable[MatrixEntry[Int]])(implicit sc: ScoobiConfiguration): DMatrix[Int, Int] =
    fromDelimitedInput(m.map(entry => entry.row + "," + entry.col + "," + entry.value).toSeq: _*).collect { case AnInt(r) :: AnInt(c) :: AnInt(v) :: _ => ((r, c), v) }

  def toDoubleDMatrix(m: Iterable[MatrixEntry[Double]])(implicit sc: ScoobiConfiguration): DMatrix[Int, Double] =
    fromDelimitedInput(m.map(entry => entry.row + "," + entry.col + "," + entry.value).toSeq: _*).collect { case AnInt(r) :: AnInt(c) :: ADouble(v) :: _ => ((r, c), v) }

  def toEntrySet[V](m: DMatrix[Int, V])(implicit conf: ScoobiConfiguration, valV: Manifest[V], wfmtV: WireFormat[V]) =
    Set[MatrixEntry[V]]() ++ m.materialise.run.collect { case ((r, c), v) if v != 0 => MatrixEntry[V](r, c, v) }

  def toIntEntrySet(m: FieldMatrix[BigReal]): Set[MatrixEntry[Int]] = {

    class SetAdder(var ms: Set[MatrixEntry[Int]] = Set[MatrixEntry[Int]]()) extends FieldMatrixPreservingVisitor[BigReal] {
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

  case class MatrixEntry[V](row: Int, col: Int, value: V)

  val matrixMaxSize = 200 // Needs to be pretty small, for apache's maths lib
  val dimensionsInt = Gen.choose(0, matrixMaxSize - 1) // value given to a row or column

  val valueInt = Gen.choose(0, 50) // I want this to be pretty small, so as to not have to worry about overflow differences
  val valueDouble = Gen.choose(0.0, 50.0)

  private def genIntSparseMatrixDataMap: Gen[Map[(Int, Int), Int]] = Gen.listOf(for {
    row <- dimensionsInt
    col <- dimensionsInt
    value <- valueInt
  } yield ((row, col), value)).map(_.toMap)

  def genIntSparseMatrixData: Gen[Iterable[MatrixEntry[Int]]] = genIntSparseMatrixDataMap.map(m => for { entry <- m } yield MatrixEntry(entry._1._1, entry._1._2, entry._2))

  def indices(size: Int): Seq[(Int, Int)] = for {
    row <- 0 until size
    col <- 0 until size
  } yield (row, col)

  def values(size: Int): Gen[List[Double]] = Gen.listOfN(size*size, valueDouble)

  private def genDoubleDenseMatrixDataMap: Gen[Map[(Int, Int), Double]] = Gen.sized(n => values(n).map(vs => indices(n) zip vs).map(_.toMap))

  def genDoubleDenseMatrixData: Gen[Iterable[MatrixEntry[Double]]] = genDoubleDenseMatrixDataMap.map(m => for { entry <- m } yield MatrixEntry(entry._1._1, entry._1._2, entry._2))

  /** Create a SparseFieldMatrix */
  def toApacheMatrix[V: Numeric](d: Iterable[MatrixEntry[V]]): SparseFieldMatrix[BigReal] = {
    val sfm = new SparseFieldMatrix[BigReal](BigRealField.getInstance, matrixMaxSize, matrixMaxSize)
    d.foreach { q =>
      sfm.setEntry(q.row, q.col, new BigReal(q.value.toString))
    }
    sfm
  }

  /** Create a RealMatrix from a set of MatrixEntry's */
  def toApacheRealMatrix(d: Iterable[MatrixEntry[Double]]): RealMatrix = {
    val dims = d.foldLeft((0,0))((max, entry) => (max._1.max(entry.row+1), max._2.max(entry.col+1)))
    val sfm = new Array2DRowRealMatrix(dims._1, dims._2)
    d.foreach { q => sfm.setEntry(q.row, q.col, q.value) }
    sfm
  }

  /** Get the random matrix out of the original and result */
  def getRandomMatrix(originalMatrix: RealMatrix, resultMatrix: RealMatrix): RealMatrix = {
    val invertedOriginalMatrix = new LUDecompositionImpl(originalMatrix).getSolver.getInverse
    invertedOriginalMatrix.multiply(resultMatrix)
  }

  /** A fuzzy equality check. This is needed because some precision is lost when creating the
   *  inverse of a matrix */
  def fuzzyEqual(matrix1: RealMatrix, matrix2: RealMatrix): Boolean = {
    if (matrix1 == matrix2)
      return true

    val m2Rows = matrix2.getRowDimension
    val m2Cols = matrix2.getColumnDimension

    if (matrix1.getColumnDimension != m2Cols || matrix1.getRowDimension != m2Rows)
      return false

    for (row <- 0 until m2Rows; col <- 0 until m2Cols)
      if ((math.round(((matrix1.getEntry(row, col) - matrix2.getEntry(row, col)).abs)*100000)*0.00001) != 0.0d)
        return false
    true
  }

  def prettyPrintMatrix(matrix: RealMatrix): Unit = {
    for (row <- 0 until matrix.getRowDimension; col <- 0 until matrix.getColumnDimension) {
      if(col == 0) print("\n\n\n| ")
      print("(" + row + "," + col + ")=" + matrix.getEntry(row, col) + " | ")
    }
  }
  */
}
