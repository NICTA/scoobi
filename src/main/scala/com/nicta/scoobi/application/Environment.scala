/**
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
package application


trait Environment {
 /* Hadoop environment */

 /**
   * Increment a counter by a given amount.  This only works inside of
   * code run from a task tracker.  If you want to have your counters logged
   * on the job tracker, use `enableCounterLogging` in the job tracker
   * (where `persist` is called).
   */
  def incrCounter(group: String, name: String, incr: Long = 1) {
    ScoobiEnvironment.incrCounter(group, name, incr)
  }

  /**
   * Return the value of a counter.  NOTE: May not reflect latest updates
   * to the counter, especially when done in other tasks.
   */
  def getCounter(group: String, name: String): Long =
    ScoobiEnvironment.getCounter(group, name)

  /**
   * Enable or disable logging of set counters after each job.
   *
   * @param enable Whether to log counters at the end of each mapreduce job
   * @param includeSystem Whether to include system-internal counters in
   *   the log (rather than just user-set counters)
   */
  def enableCounterLogging(enable: Boolean = true,
      includeSystem: Boolean = false) {
    ScoobiEnvironment.enableCounterLogging(enable, includeSystem)
  }

  val ScoobiEnvironment = com.nicta.scoobi.application.ScoobiEnvironment
}
object Environment extends Environment


