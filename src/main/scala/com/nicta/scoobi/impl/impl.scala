package com.nicta.scoobi
package object impl {

  import org.apache.hadoop.conf.Configuration
  import scala.collection.JavaConversions._

  /**
   * This conversion transforms a Configuration, seen as an Iterable[Map.Entry[String, String]]
   * to a Map[String, String]
   */
  implicit def configurationToMap(conf: Configuration) = new ConfigurationToMap(conf)
  class ConfigurationToMap(conf: Configuration) {
    def toMap: Map[String, String] = conf.toList.map(me => (me.getKey, me.getValue)).toMap
  }

}