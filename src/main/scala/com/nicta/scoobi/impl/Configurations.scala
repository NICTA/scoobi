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

import control.Exceptions
import org.apache.hadoop.conf.Configuration
import Exceptions._
import scala.collection.JavaConversions._
import collection.Maps._

/**
 * This trait adds functionalities to Hadoop's Configuration object
 */
trait Configurations {

  /**
   * List of Scoobi properties which are stored in the Hadoop configuration object
   */
  val JOB_ID                             = "scoobi.jobid"
  val PROGRESS_TIME                      = "scoobi.progress.time"
  val JOB_NAME                           = "scoobi.jobname"
  val SCOOBI_MODE                        = "scoobi.mode"
  val UPLOADED_LIBJARS                   = "scoobi.uploadedlibjars"
  val MAPREDUCE_REDUCERS_MIN             = "scoobi.mapreduce.reducers.min"
  val MAPREDUCE_REDUCERS_MAX             = "scoobi.mapreduce.reducers.max"
  val MAPREDUCE_REDUCERS_BYTESPERREDUCER = "scoobi.mapreduce.reducers.bytesperreducer"

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
     * add a list of values to a Configuration, for a given key, using a specific separator
     * remove redundant values if there are any
     */
    def addValues(key: String, values: Seq[String], separator: String): Configuration = {
      conf.set(key, (toMap.get(key).toSeq.flatMap(_.split(separator)) ++ values).distinct.mkString(separator))
      conf
    }

    /**
     * add a list of values to a Configuration, for a given key, with a comma separator
     */
    def addValues(key: String, values: String*): Configuration = addValues(key, values, ",")

    /**
     * add all the keys found in the other Configuration to this configuration, possibly mapping to different keys
     * or value if necessary
     * @return the modified configuration object
     */
    def updateWith(other: Configuration)(update: PartialFunction[(String, String), (String, String)]): Configuration = {
      conf.overrideWith(conf.toMutableMap.updateWith(other.toMap)(update))
    }
    /**
     * set all the keys defined by the partial function
     * @return the modified configuration object
     */
    def updateWith(update: PartialFunction[(String, String), (String, String)]): Configuration = {
      conf.overrideWith(conf.toMap.collect(update))
    }
    /**
     * add all the keys found in the other map to this configuration
     * @return the modified configuration object
     */
    def overrideWith(map: scala.collection.Map[String, String]): Configuration = {
      map foreach { case (k, v) => conf.set(k, v) }
      conf
    }

    /**
     * update the value of a given key, using a default value if missing
     */
    def update(key: String, default: =>String): Configuration = update(key, default, identity)
    /**
     * update the value of a given key, using a default value if missing
     */
    def update(key: String, default: =>String, f: String => String): Configuration = {
      conf.set(key, f(Option(conf.get(key)).getOrElse(f(default))))
      conf
    }

    /**
     * increment an Int property by 1.
     *
     * Add a new key/value pair with value == 1 if the key didn't exist before
     */
    def increment(key: String): Int = synchronized {
      val value = conf.getInt(key, 0) + 1
      conf.setInt(key, value)
      value
    }

    /**
     * increment an Int property by 1.
     *
     * The value to be incremented is specified as a list of values for keys corresponding to a regex.
     * In that case the maximum value for those keys is taken and incremented
     *
     * @see FunctionInput
     */
    def incrementRegex(key: String, keyRegex: String): Int = synchronized {
      val value = conf.getValByRegex(keyRegex).values().toList.map(s => tryo(s.toInt)).flatten.sorted.lastOption.getOrElse(0) + 1
      conf.setInt(key, value)
      value
    }

    /**
     * @return the value of the configuration for a given key or set it with a default value
     */
    def getOrSet(key: String, defaultValue: String): String = {
      if (!conf.defines(key)) conf.set(key, defaultValue)
      conf.get(key)
    }

    /**
     * @return true if a key is defined
     */
    def defines(key: String) = Option(conf.get(key)).isDefined

    /** @return a string with all the key/values, one per line */
    def show = conf.getValByRegex(".*").entrySet().mkString("\n")

  }

  /**
   * @return a Configuration object from a sequence of key/value (and only those, for testing)
   */
  private[scoobi]
  def configuration(pairs: (String, String)*): Configuration = {
    val configuration = new Configuration(false)
    pairs.foreach { case (k, v) => configuration.set(k, v) }
    configuration
  }
}
object Configurations extends Configurations
