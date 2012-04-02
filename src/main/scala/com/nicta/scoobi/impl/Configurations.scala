package com.nicta.scoobi.impl

import org.apache.hadoop.conf.Configuration

/**
 * This trait adds functionalities to Hadoop's Configuration object
 */
trait Configurations {
  import scala.collection.JavaConversions._
  import com.nicta.scoobi.impl.collection.Maps._

  /**
   * This conversion transforms a Configuration, seen as an Iterable[Map.Entry[String, String]]
   * to a Map[String, String] or a mutable.Map[String, String]
   *
   * It also provides additional convenience methods to modify the map
   */
  implicit def extendConfiguration(conf: Configuration) = new ExtendedConfiguration(conf)
  class ExtendedConfiguration(conf: Configuration) {
    def toList = conf.toSeq.map(me => (me.getKey, me.getValue))
    def toMap: Map[String, String] = toList.toMap
    def toMutableMap: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map(toList: _*)

    /**
     * add a list of values to a Configuration, for a given key
     */
    def addValues(key: String, values: String*): Configuration = {
      conf.set(key, (toMap.get(key).toSeq ++ values).mkString(","))
      conf
    }

    /**
     * add all the keys found in the other Configuration to this configuration, possibly mapping to different keys
     * or value if necessary
     * @return the modified configuration object
     */
    def updateWith(other: Configuration)(update: PartialFunction[(String, String), (String, String)]): Configuration = {
      conf.overrideWith(conf.toMutableMap.updateWith(other.toMap)(update))
    }
    /**
     * add all the keys found in the other map to this configuration
     * @return the modified configuration object
     */
    def overrideWith(map: scala.collection.mutable.Map[String, String]): Configuration = {
      map foreach { case (k, v) => conf.set(k, v) }
      conf
    }
  }

  /**
   * @return a Configuration object from a sequence of key/value
   */
  def configuration(pairs: (String, String)*): Configuration = {
    val configuration = new Configuration
    pairs.foreach { case (k, v) => configuration.set(k, v) }
    configuration
  }
}
object Configurations extends Configurations
