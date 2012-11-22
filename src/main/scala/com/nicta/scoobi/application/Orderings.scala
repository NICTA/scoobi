package com.nicta.scoobi
package application

import org.apache.hadoop.io.{DoubleWritable, BytesWritable, Text}


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
object Orderings extends Orderings