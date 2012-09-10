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
package testing

import org.specs2.execute._
import org.specs2.specification._
import org.specs2.Specification
import application._
import impl.time.SimpleTimer

/**
 * This trait provides an Around context to be used in a Specification
 *
 * Subclasses need to define the context method:
 *
 *  - def context = local            // execute the code locally
 *  - def context = cluster          // execute the code on the cluster
 *  - def context = localThenCluster // execute the code locally then on the cluster in case of a success
 *
 * They also need to implement the Cluster trait to specify the location of the remote nodes
 *
 */
trait HadoopExamples extends Hadoop with CommandLineScoobiUserArgs with Cluster { outer =>

  /** make the context available implicitly as an Outside[ScoobiConfiguration] so that examples taking that context as a parameter can be declared */
  implicit protected def aroundContext: HadoopContext = context

  /** define the context to use: local, cluster or localThenCluster */
  def context: HadoopContext = localThenCluster

  /**
   * the execution time will not be displayed with this function, but by adding more information to the execution Result
   */
  override def displayTime(prefix: String) = (timer: SimpleTimer) => ()

  /** context for local execution */
  def local: HadoopContext = new LocalHadoopContext
  /** context for cluster execution */
  def cluster: HadoopContext = new ClusterHadoopContext
  /** context for local then cluster execution */
  def localThenCluster: HadoopContext = new LocalThenClusterHadoopContext

  /** execute an example body on the cluster */
  def remotely[R <% Result](r: =>R) = showResultTime("Cluster execution time", runOnCluster(r))

  /** execute an example body locally */
  def locally[R <% Result](r: =>R) = showResultTime("Local execution time", runOnLocal(r))

  /**
   * Context for running examples on the cluster
   */
  class ClusterHadoopContext extends HadoopContext {
    def outside = configureForCluster(new ScoobiConfiguration)

    override def apply[R <% Result](a: ScoobiConfiguration => R) = {
      if (!outer.isLocalOnly) remotely(cleanup(a).apply(outside))
      else                    Skipped("excluded", "No cluster execution time")
    }
  }

  /** @return a composed function cleaning up after the job execution */
  def cleanup[R <% Result](a: ScoobiConfiguration => R): ScoobiConfiguration => R = {
    if (!keepFiles) (c: ScoobiConfiguration) => try { a(c) } finally { cleanup(c) }
    else            a
  }

  /** cleanup temporary files after job execution */
  def cleanup(c: ScoobiConfiguration) {
    // the 2 actions are isolated. In case the first one fails, the second one has a chance to succeed.
    try { c.deleteWorkingDirectory }
    finally { TestFiles.deleteFiles(c) }
  }

  /**
   * Context for running examples locally
   */
  class LocalHadoopContext extends HadoopContext {
    def outside = configureForLocal(new ScoobiConfiguration)

    override def apply[R <% Result](a: ScoobiConfiguration => R) = {
      if (!outer.isClusterOnly) locally(cleanup(a).apply(outside))
      else                      Skipped("excluded", "No local execution time")
    }
    override def isRemote = false
  }

  /**
   * Context for running examples locally, then on the cluster if it succeeds locally
   */
  case class LocalThenClusterHadoopContext() extends ClusterHadoopContext {
    /**
     * delegate the apply method to the LocalContext, then the Cluster context in case of a Success
     */
    override def apply[R <% Result](a: ScoobiConfiguration => R) = {
      local(a) match {
        case f @ Failure(_,_,_,_) => f
        case e @ Error(_,_)       => e
        case s @ Skipped(_,_)     => cluster(a).mapExpected((e: String) => s.expected+"\n"+e)
        case other                => changeSeparator(other and cluster(a))
      }
    }
  }

  /** change the separator of a Result */
  private def changeSeparator(r: Result) = r.mapExpected((_:String).replace("; ", "\n"))
  /**
   * trait for creating contexts having ScoobiConfigurations
   *
   * the isLocalOnly method provides a hint to speed-up the execution (because there's no need to upload jars if a run
   * is local)
   */
  trait HadoopContext extends Outside[ScoobiConfiguration] {
    def isRemote = true
  }

  /**
   * @return an executed Result updated with its execution time
   */
  private def showResultTime[T <% Result](prefix: String, t: =>T): Result = {
    if (showTimes) {
      lazy val (result, timer) = withTimer(ResultExecution.execute(t)(implicitly[T => Result]))
      result.updateExpected(prefix+": "+timer.time)
    } else t
  }

}

/**
 * You can use this abstract class to create your own specification class, specifying:
 *
 *  - the type of Specification: mutable or not
 *  - the cluster
 *  - additional variables
 *
 *      class MyHadoopSpec(args: Arguments) extends HadoopSpecificationStructure(args) with
 *        MyCluster with
 *        mutable.Specification
 */
trait HadoopSpecificationStructure extends
  Cluster with
  HadoopExamples with
  UploadedLibJars with
  HadoopLogFactorySetup with
  CommandLineHadoopLogFactory {
}

trait HadoopLogFactorySetup extends LocalHadoop with SpecificationStructure {
  override def map(fs: =>Fragments) = super.map(fs).insert(Step(setLogFactory()))
}

trait CommandLineHadoopLogFactory extends HadoopLogFactorySetup with CommandLineScoobiUserArgs {
  /** for testing, the output must be quiet by default, unless verbose is specified */
  override def quiet = !isVerbose
}

/**
 * Hadoop specification with an acceptance specification
 */
trait HadoopSpecification extends Specification with HadoopSpecificationStructure with ScoobiAppConfiguration