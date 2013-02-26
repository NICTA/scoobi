package com.nicta.scoobi
package application

import org.apache.hadoop.mapreduce._
import collection.JavaConversions._

/**
 * Interface for interacting more directly with the Hadoop environment,
 * e.g. accessing and setting counters.
 */
object ScoobiEnvironment {

  ///////////// Keep track of job and task contexts
  //
  // FIXME: We don't yet actually set the job context, although we should.
  // We should also consider propagating counters from one map-reduce job
  // to the next, since a typical Scoobi job contains multiple map-reduce
  // jobs, and the user probably doesn't care about this.

  // On the job tracker, we need the Job object
  private var job: Job = _
  // On the task tracker, we need the context from the mapper or reducer
  private var taskContext: TaskInputOutputContext[_,_,_,_] = _

  /**
   * Set the Job object, if we're running the job-running code on the
   * job tracker.
   */
  private[scoobi] def setJob(job: Job) {
    this.job = job
  }

  /**
   * Return the Job object associated with the current job, if we are
   * running on the job tracker.  If we're running on a task tracker,
   * this will be null.
   */
  private[scoobi] def getJob = job

  /**
   * Set the task context, if we're running in the map or reduce task
   * code on a task tracker. (Both Mapper.Context and Reducer.Context are
   * subclasses of TaskInputOutputContext.)
   */
  private[scoobi] def setTaskContext(context: TaskInputOutputContext[_,_,_,_]) {
    this.taskContext = context
  }

  /**
   * Return the TaskInputOutputContext associated with the current task,
   * if we are running on a task tracker. If we're running on the job tracker,
   * this will be null.
   */
  def getTaskContext = taskContext

  /**
   * Return the JobContext object.  This is always available.
   */
  private def getJobContext: JobContext = {
    if (taskContext != null) taskContext
    else if (job != null) job
    else needToSetContext()
  }

  ///////////// Get the raw configuration.
  //
  // FIXME: Should probably integrate this with ScoobiConfiguration, which
  // only functions on the job tracker (not task tracker) and may not (?)
  // contain all the system properties.

  /**
   * Return the raw configuration associated with the job or task.
   */
  def getContextConfiguration = getJobContext.getConfiguration

  // def getTaskID = get_configuration.getInt("mapred.task.partition", -1)

  ///////////// Counters

  /**
   * Find the Counter object for the given counter.  The way to do this
   * depends on whether we're running on the job tracker, or in a map or
   * reduce task on a task tracker.
   */
  def findCounter(group: String, counter: String): Counter = {
    if (taskContext != null)
      taskContext.getCounter(group, counter)
    else if (job != null)
      job.getCounters.findCounter(group, counter)
    else
      needToSetContext()
  }

  private def needToSetContext() =
    throw new IllegalStateException("Either task context or job needs to be set before any counter operations")

  private[scoobi] var logCounters = false
  private[scoobi] var logSystemCounters = false

  /**
   * Increment a counter by a given amount.
   */
  def incrCounter(group: String, name: String, incr: Long = 1) {
    val counter = findCounter(group, name)
    counter.increment(incr)
  }

  /**
   * Return the value of a counter.  NOTE: May not reflect latest updates
   * to the counter, especially when done in other tasks.
   */
  def getCounter(group: String, name: String): Long = {
    val counter = findCounter(group, name)
    counter.getValue()
  }

  /**
   * Enable or disable logging of set counters after each job.
   *
   * @param enable Whether to log counters at the end of each mapreduce job
   * @param includeSystem Whether to include system-internal counters in
   *   the log (rather than just user-set counters)
   */
  def enableCounterLogging(enable: Boolean = true,
      includeSystem: Boolean = false) {
    logCounters = enable
    logSystemCounters = includeSystem
  }

  ///////////// Signal progress being made

  /**
   * Signal to Hadoop that we're making progress, in case we're doing a
   * long operation that isn't interacting with Hadoop.
   */
  def heartbeat() {
    if (taskContext != null)
      taskContext.progress
  }
}

