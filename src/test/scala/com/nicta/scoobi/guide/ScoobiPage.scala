package com.nicta.scoobi.guide

import org.specs2.Specification
import ScoobiVersion._
import org.specs2.specification.{Text, Fragments}

/**
 * base class for creating Scoobi user guide pages.
 *
 * If the text contains "SCOOBI_VERSION", each occurrence will be replaced by the current Scoobi version as defined in the build.sbt file
 * If the text contains "SCOOBI_BRANCH", each occurrence will be replaced by either the official tag or master if the version is a SNAPSHOT one
 */
abstract class ScoobiPage extends Specification {
  override def map(fs: =>Fragments) =
    noindent ^ fs.map {
      case Text(t) => Text(t.setVersionAndBranch)
      case other => other
    }
}
