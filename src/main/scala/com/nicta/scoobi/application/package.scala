package com.nicta.scoobi

import org.apache.hadoop.io.{DoubleWritable, BytesWritable, Text}

package object application extends Orderings {

  // see the use of tagged types: http://etorreborre.blogspot.com.au/2011/11/practical-uses-for-unboxed-tagged-types.html
  // this will be replaced with similar functionalities in scalaz 7 when released
  type Tagged[U] = { type Tag = U }
  type @@[T, U] = T with Tagged[U]

  trait AsLevel
  type Level = String @@ AsLevel
  def level(l: String): Level = l.asInstanceOf[Level]

}

trait Orderings {
  implicit def TextOrdering = new Ordering[Text] {
    def compare(x: Text, y: Text): Int = x.compareTo(y)
  }

  implicit def BytesOrdering = new Ordering[BytesWritable] {
    def compare(x: BytesWritable, y: BytesWritable): Int = x.compareTo(y)
  }

  implicit def DoubleOrdering = new Ordering[DoubleWritable] {
    def compare(x: DoubleWritable, y: DoubleWritable): Int = x.compareTo(y)
  }
}