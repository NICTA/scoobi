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

import scala.io.Source
import impl.control.Exceptions._

trait ScoobiVariables {

  lazy val VERSION = versionLine.flatMap(extractVersion).getOrElse("version not found")
  lazy val PREVIOUS_VERSION_IF_SNAPSHOT = "SCOOBI-" + (
    if (IS_SNAPSHOT) {
      val major :: minor :: patch :: _ = VERSION.replace("-SNAPSHOT", "").split("\\.").toList
      Seq(major, minor.toInt-1, patch).mkString(".")
    } else VERSION
  )

  lazy val IS_SNAPSHOT = VERSION endsWith "SNAPSHOT"

  lazy val BRANCH = if (IS_SNAPSHOT) "master" else PREVIOUS_VERSION_IF_SNAPSHOT

  lazy val LANDING_PAGE = "http://nicta.github.io/scoobi/"

  lazy val API_DIR            = LANDING_PAGE+"api/"
  lazy val API_OFFICIAL_PAGE  = API_DIR+PREVIOUS_VERSION_IF_SNAPSHOT+"/index.html"
  lazy val API_SNAPSHOT_PAGE  = API_DIR+"master/index.html"
  lazy val API_PAGE           = (if (IS_SNAPSHOT) API_SNAPSHOT_PAGE else API_OFFICIAL_PAGE)

  lazy val GUIDE_OFFICIAL_DIR = "guide/"
  lazy val GUIDE_SNAPSHOT_DIR = "guide-SNAPSHOT/guide/"
  lazy val GUIDE_DIR          = (if (IS_SNAPSHOT) GUIDE_SNAPSHOT_DIR else GUIDE_OFFICIAL_DIR)

  lazy val GUIDE_OFFICIAL_PAGE = LANDING_PAGE + GUIDE_OFFICIAL_DIR
  lazy val GUIDE_SNAPSHOT_PAGE = LANDING_PAGE + GUIDE_SNAPSHOT_DIR
  lazy val GUIDE_PAGE          = LANDING_PAGE + GUIDE_DIR

  lazy val OFFICIAL_TAG        = PREVIOUS_VERSION_IF_SNAPSHOT

  private lazy val versionLine = buildSbt.flatMap(_.getLines.find(line => line contains "version"))
  private def extractVersion(line: String) = "\\s*version.*\\:\\=\\s*\"(.*)\"".r.findFirstMatchIn(line).map(_.group(1))
  private lazy val buildSbt = tryOr(Option(Source.fromFile("version.sbt")))((e:Exception) => { println("can't find the version.sbt file "+e.getMessage); None })

}

object ScoobiVariables extends ScoobiVariables
