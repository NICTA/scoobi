package com.nicta.scoobi.guide

import org.specs2.Specification
import ScoobiVersion._
import org.specs2.specification.{Text, Fragments}

/**
 * base class for creating Scoobi user guide pages. If the text contains "VERSION", each occurrence will be replaced by the current Scoobi version as defined in the build.sbt file
 */
abstract class ScoobiPage extends Specification {
  override def map(fs: =>Fragments) = noindent ^ fs.map { case Text(t) => Text(t.setVersion); case other => other }
}
