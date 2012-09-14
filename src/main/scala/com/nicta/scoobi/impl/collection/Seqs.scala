package com.nicta.scoobi
package impl
package collection

private[scoobi]
trait Seqs {

  /**
   * Split a sequence into smaller input splits according to a splitSize parameter.
   *
   * if the list is empty or with 1 element, don't try to group by splitSize because in this case splitSize is 0
   * and grouped will fail at runtime (see issue #60, issue #75)
   */
  def split[A, B, T <% BoundedLinearSeq[A]](seq: T, splitSize: Int, makeSplit: (Int, Int, T) => B): Seq[B] = {
    if      (seq.size == 0) Seq[B]()
    else if (seq.size == 1) Seq(makeSplit(0, seq.size, seq))
    else                    (0 to (seq.size - 1)).toSeq.grouped(splitSize).
                            map { r => (r.head, r.size) }.
                            map { case (s, l) => makeSplit(s, l, seq) }.toSeq
  }

  implicit def sequenceToSeqBoundedLinearSeq[A](seq: Seq[A]): BoundedLinearSeq[A] = SeqBoundedLinearSeq(seq)
}

trait BoundedLinearSeq[+A] {
  def apply(i: Int): A
  def size: Int
}

case class SeqBoundedLinearSeq[A](seq: Seq[A]) extends BoundedLinearSeq[A] {
  def apply(i: Int): A = seq(i)
  def size: Int        = seq.size
}

case class FunctionBoundedLinearSeq[A](f: Int => A, fsize: Int) extends BoundedLinearSeq[A] {
  def apply(i: Int): A = f(i)
  def size: Int        = fsize
}

private[scoobi]
object Seqs extends Seqs
