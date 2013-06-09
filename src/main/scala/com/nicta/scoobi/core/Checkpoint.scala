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
package core

import java.util.UUID
import org.apache.hadoop.fs.Path

/** store the output path of a Sink as a checkpoint */
case class Checkpoint(path: Path) {
  /** @return the name for the file used as checkpoint */
  lazy val name = path.getName

  /**
   * @return the path for the file used as checkpoint as a string
   *
   * Important! This needs to be computed only once because it is used with an identity map in the filledSink attribute
   * to determine if a sink has been filled or not  
   */
  lazy val pathAsString = path.toString
}

object Checkpoint {
  def create(path: Option[String], doIt: Boolean)(implicit sc: ScoobiConfiguration) =
    if (doIt) Some(Checkpoint(new Path(path.map(_.toString).getOrElse(UUID.randomUUID().toString)))) else None
}