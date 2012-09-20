package com.nicta.scoobi
package application

import org.apache.hadoop.mapreduce._

object ScoobiEnvironmentPrivate {
  // NOTE: These objects cannot be serialized properly; hence they must be
  // placed in a separate object to which there are no pointers. (There's
  // a pointer to ScoobiEnvironment in the main `object Scoobi`.)

  // On the task tracker, we need the context from the mapper or reducer
  private[scoobi] var taskContext: TaskInputOutputContext[_,_,_,_] = _

  /* A comment about Hadoop contexts:
  
     -- On a task tracker, context is available through a
     TaskInputOutputContext object, which is passed into the setup(),
     map()/reduce(), and cleanup() methods of the Hadoop Mapper and
     Reducer classes.

     -- On the job tracker, context is available from a Job object, which
     is used to run jobs.  Note that in Scoobi, we don't create any Job
     objects until a call to persist() is made.

     -- Both TaskInputOutputContext and Job are subclasses of JobContext,
     which provides certain basic context information that is shared by
     both task and job trackers -- for example, the configuration, stored
     in a Configuration object and retrievable from the getConfiguration()
     method on a JobContext.
     
     -- Note that Scoobi creates its own Configuration object, which contains
     Configuration settings from the command line and is separate from
     the Configuration object provided by Hadoop from getConfiguration().
     However, it's not clear that these settings are available in a task
     tracker. In addition, there are other settings that Hadoop adds to the
     Configuration object internally during the running of jobs or tasks,
     e.g. the current task ID, which is available inside of a task tracker
     from the `mapred.task.partition` setting.  Settings like this can't
     be retrieved from the Scoobi-provided Configuration, but can be
     retrieved by client code by calling `getTaskContext.getConfiguration`
     to fetch the Hadoop-provided Configuration.
   */
}

/**
 * Interface for interacting more directly with the Hadoop environment,
 * e.g. accessing and setting counters.
 */
object ScoobiEnvironment {
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
   * Set the task context, if we're running in the map or reduce task
   * code on a task tracker. (Both Mapper.Context and Reducer.Context are
   * subclasses of TaskInputOutputContext.)
   */
  private[scoobi] def setTaskContext(context: TaskInputOutputContext[_,_,_,_]) {
    ScoobiEnvironmentPrivate.taskContext = context
  }

  /**
   * Return the TaskInputOutputContext associated with the current task,
   * if we are running on a task tracker; otherwise null.
   */
  def getTaskContext = ScoobiEnvironmentPrivate.taskContext

  ///////////// Counters

  /**
   * Find the Counter object for the given counter.
   */
  def findCounter(group: String, counter: String): Counter = {
    if (getTaskContext != null)
      getTaskContext.getCounter(group, counter)
    // This isn't the right interface to retrieving counters from a job
    // context, since there may be more than one context active (or at least
    // available) in the client program -- at least one per DList or DObject.
    // else if (getJob != null)
    //   getJob.getCounters.findCounter(group, counter)
    else
      noContextAvailable()
  }

  private def noContextAvailable() =
    throw new IllegalStateException("Not running on a task tracker")

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
