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
package guide

import org.specs2.Specification
import org.specs2.specification._
import org.specs2.specification.Snippets

/**
 * base class for creating Scoobi user guide pages.
 *
 * If the text contains "${VERSION}", each occurrence will be replaced by the current Scoobi version as defined in the build.sbt file
 * If the text contains "${BRANCH}", each occurrence will be replaced by either the official tag or master if the version is a SNAPSHOT one
 */
trait ScoobiPage extends Specification with ScoobiVariables with Snippets {
  override def map(fs: =>Fragments) =
    noindent ^ Fragments.create(fs.fragments.map {
      case start @ SpecStart(_,_,_) if isIndex(start) => start.urlIs("index.html")
      case start @ SpecStart(_,_,_)                   => start.baseDirIs(s"./$GUIDE_DIR")
      case other                                      => other
    }:_*)

  private def isIndex(start: SpecStart) = start.specName.javaClassName endsWith "Index"
}
