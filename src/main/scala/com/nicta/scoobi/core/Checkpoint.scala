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

/** store the output path of a Sink as a checkpoint */
case class Checkpoint(name: String)

object Checkpoint {
  def create(path: Option[String], doIt: Boolean)(implicit sc: ScoobiConfiguration) =
    if (doIt) Some(Checkpoint(path.map(_.toString).getOrElse(UUID.randomUUID().toString))) else None
}