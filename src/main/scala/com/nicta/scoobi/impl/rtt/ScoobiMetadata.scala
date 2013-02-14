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
package impl
package rtt

import scalaz.Memo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import util.DistCache
import core.ScoobiConfiguration

/**
 * This object stores and retrieves metadata from the Distributed cache.
 *
 * When storing the metadata, it returns a path identifying the stored information in order to retrieve it later
 */
object ScoobiMetadata {

  def saveMetadata(metadataTag: String, metadata: Any)(implicit sc: ScoobiConfiguration): Path = DistCache.pushObject(sc.configuration, metadata, metadataTag)

  /** we retrieve metadata from the distributed cache and memoise each retrieved piece of metadata */
  lazy val metadata = (path: String) => Memo.mutableHashMapMemo[String, Any]{ path: String =>
    DistCache.deserialise(new Configuration).apply(new Path(path)).get: Any
  }.apply(path)
}

/**
 * Set of metadata, which can be a different tuple for each tag (and all tuples don't have to have the same size)
 */
trait TaggedMetadata {

  def metadataPath: String
  lazy val metaDatas = ScoobiMetadata.metadata(metadataPath).asInstanceOf[Map[Int, Product]]
  lazy val tags = metaDatas.keySet.toSeq

}

