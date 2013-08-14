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
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import util.DistCache
import core.ScoobiConfiguration
import org.apache.commons.logging.LogFactory
import monitor.Loggable._
/**
 * This object stores and retrieves metadata from the Distributed cache.
 *
 * When storing the metadata, it returns a path identifying the stored information in order to retrieve it later
 */
object ScoobiMetadata {

  private implicit val logger = LogFactory.getLog("scoobi.metadata")

  def saveMetadata(metadataTag: String, metadata: Any, configuration: Configuration): String = {
    DistCache.pushObject(configuration, metadata, metadataTag).debug("saving metadata for tag ")
    metadataTag
  }

  /** we retrieve metadata from the distributed cache and memoise each retrieved piece of metadata */
  def metadata(implicit configuration: Configuration) = (metadataTag: String) => Memo.mutableHashMapMemo[String, Any]{ metadataTag: String =>
    logger.debug("retrieving metadata for tag "+metadataTag)
    DistCache.pullObject(configuration, metadataTag).get: Any
  }.apply(metadataTag)
}

/**
 * Set of metadata, which can be a different tuple for each tag (and all tuples don't have to have the same size)
 */
trait TaggedMetadata extends Configured {
  def metadataTag: String
  lazy val metaDatas = ScoobiMetadata.metadata(configuration)(metadataTag).asInstanceOf[Map[Int, Product]]
  lazy val tags = metaDatas.keySet.toSeq
}

trait Configured extends Configurable {
  protected[scoobi] implicit var configuration = new Configuration
  def setConf(conf: Configuration) { configuration = conf }
  def getConf = configuration
}