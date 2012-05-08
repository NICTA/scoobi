package com.nicta.scoobi.guide

import org.specs2.Specification
import ScoobiVariables._
import org.specs2.specification.{SpecStart, Text, Fragments}

/**
 * base class for creating Scoobi user guide pages.
 *
 * If the text contains "${SCOOBI_VERSION}", each occurrence will be replaced by the current Scoobi version as defined in the build.sbt file
 * If the text contains "${SCOOBI_BRANCH}", each occurrence will be replaced by either the official tag or master if the version is a SNAPSHOT one
 */
trait ScoobiPage extends Specification {
  override def map(fs: =>Fragments) =
    noindent ^ fs.map {
      case start @ SpecStart(_,_,_) if isIndex(start) => start.urlIs("index.html")
      case start @ SpecStart(_,_,_)                   => start.baseDirIs("./${SCOOBI_GUIDE}".replaceVariables)
      case Text(t)                                    => Text(t.replaceVariables)
      case other                                      => other
    }

  private def isIndex(start: SpecStart) = start.specName.javaClassName endsWith "Index"
}
