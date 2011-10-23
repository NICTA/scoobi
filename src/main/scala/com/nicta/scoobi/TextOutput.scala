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
