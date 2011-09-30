/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.TextOutputFormat


/** Smart functions for persisting distributed lists by storing them as text files. */
object TextOutput {

  /** Specify a distibuted list to be persisitent by storing it to disk as a
    * text file. */
  def toTextFile[A : HadoopWritable](dl: DList[A], path: String): DList.DListPersister[A] = {
    new DList.DListPersister(dl, new TextPersister(path))
  }


  /** A Persister that will store the output to a specified path using Hadoop's TextOutputFormat. */
  class TextPersister[A](path: String) extends Smart.Persister[A] {
    def mkOutputStore(node: AST.Node[A]) = new OutputStore(node) {
      def outputTypeName = typeName
      val outputPath = new Path(path)
      val outputFormat = classOf[TextOutputFormat[_,_]]
    }
  }
}
