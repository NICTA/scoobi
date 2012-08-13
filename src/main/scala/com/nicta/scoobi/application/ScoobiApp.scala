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

/**
 * This trait can be extended to create an application running Scoobi code.
 *
 * Command-line arguments are available in the args attribute (minus the hadoop specific ones) and a default
 * implicit ScoobiConfiguration is also accessible to create DLists.
 *
 * Before the code is executed the dependent jars loaded in this class classloader will be uploaded to the cluster
 * (if not already there, @see LibJars for the details).
 *
 * This behavior can be switched off by overriding the `upload` method: `override def upload = false`
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
      if (!locally) uploadLibJars
      try { run }
      finally { if (!keepFiles) { configuration.deleteWorkingDirectory } }
    }
  }

  private def parseHadoopArguments(arguments: Array[String]) = {
    // arguments need to be stored before the configuration is even created
    // so that we know if configuration files must be read or not
    set(arguments)
    configuration.withHadoopArgs(arguments) { remainingArgs =>
      setRemainingArgs(remainingArgs)
    }
  }
}



