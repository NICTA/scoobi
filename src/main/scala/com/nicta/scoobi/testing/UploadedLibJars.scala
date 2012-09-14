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

import org.specs2.specification._
import application.ScoobiUserArgs

/**
 * This trait can be mixed in a Specification to automatically add a setup step uploading the library jars to the cluster
 */
trait UploadedLibJars extends SpecificationStructure with HadoopExamples with ScoobiUserArgs {

  /**
   * add a first step to upload the library jars before doing anything else
   */
  override def map(fs: =>Fragments) = fs.insert(uploadStep)

  /** create a Step to upload the jars on the cluster */
  def uploadStep = Step(if (!isLocalOnly) uploadLibJarsFiles(context.outside))

}