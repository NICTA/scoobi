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
package com.nicta.scoobi.io

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import Option.{apply => ?}

import com.nicta.scoobi.Scoobi


/** A set of helper functions for implementing DataSources and DataSinks. */
object Helper {

  /* Determine whether a path exists or not. */
  def pathExists(p: Path): Boolean = ?(FileSystem.get(Scoobi.conf).globStatus(p)) match {
    case None                     => false
    case Some(s) if s.length == 0 => false
    case Some(_)                  => true
  }

  /** Determine the byte size of data specified by a path. */
  def pathSize(p: Path): Long = {
    val fs = FileSystem.get(Scoobi.conf)
    fs.globStatus(p).map(stat => fs.getContentSummary(stat.getPath).getLength).sum
  }

  /** Provide a nicely formatted string for a byte size. */
  def sizeString(bytes: Long): String = {
    val gigabytes = (bytes / (1000 * 1000 * 1000).toDouble).toString
    gigabytes + "GB"
  }
}
