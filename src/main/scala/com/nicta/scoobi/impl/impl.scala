package com.nicta.scoobi
package object impl {

  import org.apache.hadoop.conf.Configuration
  import scala.collection.JavaConversions._

  /**
   * This conversion transforms a Configuration, seen as an Iterable[Map.Entry[String, String]]
   * to a Map[String, String]
   *
   * It also provides additional convenience methods to modify the map
   */
  implicit def extendConfiguration(conf: Configuration) = new ExtendedConfiguration(conf)
  class ExtendedConfiguration(conf: Configuration) {
    def toMap: Map[String, String] = conf.toList.map(me => (me.getKey, me.getValue)).toMap

    def addValues(key: String, values: String*) = {
      conf.set(key, (toMap.get(key).toSeq ++ values).mkString(","))
      conf
    }
  }

}