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
    def metadata(path: String): Any = DistCache.deserialise(new Configuration).apply(new Path(path)).get: Any
}

/**
 * Set of metadata, which can be a different tuple for each tag (and all tuples don't have to have the same size)
 */
trait TaggedMetadata {

  def metadataPath: String
  lazy val metaDatas = ScoobiMetadata.metadata(metadataPath).asInstanceOf[Map[Int, Product]]
  lazy val tags = metaDatas.keySet.toSeq

}

