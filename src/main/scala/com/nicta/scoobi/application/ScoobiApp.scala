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
package application

import io.FileSystems

/**
 * This trait can be extended to create an application running Scoobi code.
 *
 * Command-line arguments are available in the args attribute (minus the hadoop specific ones) and a default
 * implicit ScoobiConfiguration is also accessible to create DLists.
 *
 * A ScoobiApp will be used in 2 different contexts:
 *
 *  1. with the hadoop script
 *
 *  In that case you will use hadoop default configuration files or you will need to tell this script where to find the configuration
 *  files.
 *
 *  2. within sbt
 *
 *  In that case the cluster location can be either defined by:
 *    - overriding the 'fs' and 'jobTracker' methods
 *    - using the 'useconfdir' command line argument to add the configuration files found in $HADOOP_HOME/conf
 *
 * Then, if it can be determined that the execution will not be local but on the cluster (@see locally), the ScoobiApp
 * trait will attempt to load the dependent jars to the libjars directory on the cluster
 * (if not already there, @see LibJars for the details). This behavior can be switched off by overriding the `upload`
 * method: `override def upload = false` or by passing the 'nolibjars' argument on the command line
 */
trait ScoobiApp extends ScoobiCommandLineArgs with ScoobiAppConfiguration with Hadoop {

  /**
   * this provides the arguments which are parsed to change the behavior of the Scoobi app: logging, local/cluster,...
   * @see ScoobiUserArgs
   */
  def scoobiArgs = scoobiArguments
  /** shortcut name for command-line arguments, after extraction of the hadoop and scoobi ones */
  def args = userArguments

  /** this method needs to be overridden and define the code to be executed */
  def run()

  /**
   * parse the command-line argument and:
   *
   *  - upload the dependent jars on the cluster
   *
   *  - execute the user code
   */
  def main(arguments: Array[String]) {
    parseHadoopArguments(arguments)
    onHadoop {
      // uploading the jars must only be done when the configuration is fully setup with "onHadoop"
      if (!locally) uploadLibJarsFiles
      try { run }
      finally { if (!keepFiles) { configuration.deleteWorkingDirectory } }
    }
  }

  private def parseHadoopArguments(arguments: Array[String]) {
    // arguments need to be stored before the configuration is even created
    // so that we know if configuration files must be read or not
    set(arguments)
    HadoopLogFactory.setLogFactory(classOf[HadoopLogFactory].getName, quiet, showTimes, level, categories)
    configuration.withHadoopArgs(arguments) { remainingArgs =>
      setRemainingArgs(remainingArgs)
    }
  }

  /** upload the jars unless 'nolibjars' has been set on the command-line' */
  override def upload = !noLibJars

  /**
   * the execution is local if the file system is local, as determined by the configuration files loaded by the hadoop script
   * or if "local" is passed on the command line.
   *
   * if locally returns true then we might attempt to upload the dependent jars to the cluster and to add them to the classpath
   */
  override def locally = FileSystems.isLocal || super.locally
}



