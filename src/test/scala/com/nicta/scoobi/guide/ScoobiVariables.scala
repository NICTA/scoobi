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

  lazy val version = versionLine.flatMap(extractVersion).getOrElse("version not found")
  lazy val previousVersionIfSnapshot = "SCOOBI-" + (
    if (isSnapshot) {
      val major :: minor :: patch :: _ = version.replace("-SNAPSHOT", "").split("\\.").toList
      Seq(major, minor.toInt-1, patch).mkString(".")
    } else version
  )

  lazy val isSnapshot = version endsWith "SNAPSHOT"

  lazy val branch = if (isSnapshot) "master" else previousVersionIfSnapshot

  lazy val landingPage = "http://nicta.github.com/scoobi/"

  lazy val apiDir           = landingPage+"api/"
  lazy val apiOfficialPage  = apiDir+previousVersionIfSnapshot+"/index.html"
  lazy val apiSnapshotPage  = apiDir+"master/scala/index.html"
  lazy val apiPage          = (if (isSnapshot) apiSnapshotPage else apiOfficialPage)

  lazy val guideOfficialDir = "guide/"
  lazy val guideSnapshotDir = "guide-SNAPSHOT/guide/"
  lazy val guideDir         = (if (isSnapshot) guideSnapshotDir else guideOfficialDir)

  lazy val guideOfficialPage = landingPage + guideOfficialDir
  lazy val guideSnapshotPage = landingPage + guideSnapshotDir
  lazy val guidePage         = landingPage + guideDir

  private lazy val versionLine = buildSbt.flatMap(_.getLines.find(line => line contains "version"))
  private def extractVersion(line: String) = "\\s*version.*\\:\\=\\s*\"(.*)\"".r.findFirstMatchIn(line).map(_.group(1))
  private lazy val buildSbt = tryOr(Option(Source.fromFile("version.sbt")))((e:Exception) => { println("can't find the version.sbt file "+e.getMessage); None })

  implicit def toVersionedText(t: String): VersionedText = VersionedText(t)
  case class VersionedText(t: String) {
    /**
     * set the version and branch tags in the pages
     */
    def replaceVariables = {
      Seq("VERSION"             -> version,
          "LANDING_PAGE"        -> landingPage,
          "API_PAGE"            -> apiPage,
          "API_OFFICIAL_PAGE"   -> apiOfficialPage,
          "API_SNAPSHOT_PAGE"   -> apiSnapshotPage,
          "BRANCH"              -> branch,
          "OFFICIAL_TAG"        -> previousVersionIfSnapshot,
          "GUIDE"               -> guideDir,
          "GUIDE_OFFICIAL_DIR"  -> guideOfficialDir,
          "GUIDE_SNAPSHOT_DIR"  -> guideSnapshotDir,
          "GUIDE_PAGE"          -> guidePage,
          "GUIDE_OFFICIAL_PAGE" -> guideOfficialPage,
          "GUIDE_SNAPSHOT_PAGE" -> guideSnapshotPage).foldLeft(t) { case (res, (k, v)) => res.replaceAll("\\$\\{SCOOBI_"+k+"\\}", v) }
    }
  }

}

object ScoobiVariables extends ScoobiVariables
