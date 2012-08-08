package com.nicta.scoobi
package application

import org.apache.hadoop.mapreduce._

object ScoobiEnvironmentPrivate {
  // NOTE: These objects cannot be serialized properly; hence they must be
  // placed in a separate object to which there are no pointers. (There's
  // a pointer to ScoobiEnvironment in the main `object Scoobi`.)

  // On the job tracker, we need the Job object
  private[scoobi] var job: Job = _
  // On the task tracker, we need the context from the mapper or reducer
  private[scoobi] var taskContext: TaskInputOutputContext[_,_,_,_] = _
}

/**
 * Interface for interacting more directly with the Hadoop environment,
 * e.g. accessing and setting counters.
 */
object ScoobiEnvironment {
  ///////////// Keep track of job and task contexts
  
  /*
  FIXME: The code below for tracking the job context isn't currently used.
  It should be possible to retrieve counters programmatically after a call
  to `persist` is made.  However, that requires

  (1) that we aggregate counters across different jobs executed in a single
      `persist` call;
  (2) that we provide a counter interface directly on a DList or DObject,
      to retrieve the counters associated with the job(s) that were executed
      in order to do the necessary distributed computations.
  */

  /**
   * Set the Job object, if we're running the job-running code on the
   * job tracker.
   */
  private[scoobi] def setJob(job: Job) {
    ScoobiEnvironmentPrivate.job = job
  }

  /**
   * Return the Job object associated with the current job, if we are
   * running on the job tracker.  If we're running on a task tracker,
   * this will be null.
   */
  private[scoobi] def getJob = ScoobiEnvironmentPrivate.job

  /**
   * Set the task context, if we're running in the map or reduce task
   * code on a task tracker. (Both Mapper.Context and Reducer.Context are
   * subclasses of TaskInputOutputContext.)
   */
  private[scoobi] def setTaskContext(context: TaskInputOutputContext[_,_,_,_]) {
    ScoobiEnvironmentPrivate.taskContext = context
  }

  /**
   * Return the TaskInputOutputContext associated with the current task,
   * if we are running on a task tracker. If we're running on the job tracker,
   * this will be null.
   */
  def getTaskContext = ScoobiEnvironmentPrivate.taskContext

  /**
   * Return the JobContext object.  This is available either when running on
   * a task tracker or a job tracker.
   */
  private def getJobContext: JobContext = {
    if (getTaskContext != null) getTaskContext
    else if (getJob != null) getJob
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
    if (getTaskContext != null)
      getTaskContext.getCounter(group, counter)
    else if (getJob != null)
      getJob.getCounters.findCounter(group, counter)
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
   *
   * FIXME: There should be a programmatic interface to retrieve these
   * counters. See FIXME comment above.
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
    if (getTaskContext != null)
      getTaskContext.progress
  }
}
