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
package com.nicta.scoobi.io.text

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job

import com.nicta.scoobi.DList
import com.nicta.scoobi.DListPersister
import com.nicta.scoobi.io.DataStore
import com.nicta.scoobi.io.OutputStore
import com.nicta.scoobi.io.OutputConverter
import com.nicta.scoobi.io.Persister
import com.nicta.scoobi.io.Helper
import com.nicta.scoobi.impl.plan.AST


/** Smart functions for persisting distributed lists by storing them as text files. */
object TextOutput {
  lazy val logger = LogFactory.getLog("scoobi.TextOutput")

  /** Persist a distributed list as a text file. */
  def toTextFile[A : Manifest](dl: DList[A], path: String): DListPersister[A] = {
    val persister = new Persister[A] {
      def mkOutputStore(node: AST.Node[A]) = new OutputStore[NullWritable, A, A](node) {
        private val outputPath = new Path(path)

        val outputFormat = classOf[TextOutputFormat[NullWritable, A]]
        val outputKeyClass = classOf[NullWritable]
        val outputValueClass = implicitly[Manifest[A]] match {
          case Manifest.Boolean => classOf[java.lang.Boolean].asInstanceOf[Class[A]]
          case Manifest.Char    => classOf[java.lang.Character].asInstanceOf[Class[A]]
          case Manifest.Short   => classOf[java.lang.Short].asInstanceOf[Class[A]]
          case Manifest.Int     => classOf[java.lang.Integer].asInstanceOf[Class[A]]
          case Manifest.Long    => classOf[java.lang.Long].asInstanceOf[Class[A]]
          case Manifest.Float   => classOf[java.lang.Float].asInstanceOf[Class[A]]
          case Manifest.Double  => classOf[java.lang.Double].asInstanceOf[Class[A]]
          case Manifest.Byte    => classOf[java.lang.Byte].asInstanceOf[Class[A]]
          case mf               => mf.erasure.asInstanceOf[Class[A]]
        }

        def outputCheck() =
          if (Helper.pathExists(outputPath))
            throw new FileAlreadyExistsException("Output path already exists: " + outputPath)
          else
            logger.info("Output path: " + outputPath.toUri.toASCIIString)

        def outputConfigure(job: Job) = FileOutputFormat.setOutputPath(job, outputPath)

        val outputConverter = new OutputConverter[NullWritable, A, A] {
          def toKeyValue(x: A) = (NullWritable.get, x)
        }
      }
    }
    new DListPersister(dl, persister)
  }

  /** Persist a distributed lists of 'Products' (e.g. Tuples) as a deliminated text file. */
  def toDelimitedTextFile[A <: Product : Manifest](dl: DList[A], path: String, sep: String = "\t"): DListPersister[String] = {
    def anyToString(any: Any, sep: String): String = any match {
      case prod: Product => prod.productIterator.map(anyToString(_, sep)).mkString(sep)
      case _             => any.toString
    }
    toTextFile(dl map { anyToString(_, sep) }, path)
  }
}
