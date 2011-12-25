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
package com.nicta.scoobi.io.sequence

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat

import com.nicta.scoobi.DList
import com.nicta.scoobi.DListPersister
import com.nicta.scoobi.WireFormat
import com.nicta.scoobi.io.DataStore
import com.nicta.scoobi.io.OutputStore
import com.nicta.scoobi.io.Persister 
import com.nicta.scoobi.impl.plan.AST

/** Smart functions for persisting distributed lists by storing them as sequence files. */
object SequenceFileOutput {

  /**
   * Specify a distributed list to be persistent by storing it to disk as a
   * sequence file.
   */
  def toSequenceFile[A: WireFormat](dl: DList[A], path: String): DListPersister[A] = {
    new DListPersister(dl, new SequenceFilePersister(path))
  }

  /** A Persister that will store the output to a specified path using Hadoop's SequenceFileOutputFormat. */
  class SequenceFilePersister[A](path: String) extends Persister[A] {
    def mkOutputStore(node: AST.Node[A]) = new OutputStore(node) {
      def outputTypeName = typeName
      val outputPath = new Path(path)
      val outputFormat = classOf[SequenceFileOutputFormat[_,_]]
    }
  }
}


