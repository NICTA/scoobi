package com.nicta.scoobi.guide

import io.Source
import com.nicta.scoobi.impl.control.Exceptions._

object ScoobiVariables {

  lazy val version = versionLine.flatMap(extractVersion).getOrElse("version not found")
  lazy val isSnapshot = version endsWith "SNAPSHOT"

  lazy val branch = if (isSnapshot) "master" else version

  lazy val guideDir         = "guide"
  lazy val guideSnapshotDir = guideDir + "-SNAPSHOT/guide"

  private lazy val versionLine = buildSbt.flatMap(_.getLines.find(line => line contains "version"))
  private def extractVersion(line: String) = "version\\s*\\:\\=\\s*\"(.*)\"".r.findFirstMatchIn(line).map(_.group(1))
  private lazy val buildSbt = tryo(Source.fromFile("build.sbt"))((e:Exception) => println("can't find the build.sbt file "+e.getMessage))

  implicit def toVersionedText(t: String): VersionedText = VersionedText(t)
  case class VersionedText(t: String) {
    /**
     * set the version and branch tags in the pages
     */
    def replaceVariables = {
      Seq("VERSION"        -> version,
          "BRANCH"         -> branch,
          "GUIDE"          -> (if (isSnapshot) guideSnapshotDir else guideDir),
          "GUIDE-SNAPSHOT" -> guideSnapshotDir).foldLeft(t) { case (res, (k, v)) => res.replaceAll("\\$\\{SCOOBI_"+k+"\\}", v) }
    }
  }

}
