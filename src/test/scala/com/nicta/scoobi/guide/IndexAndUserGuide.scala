package com.nicta.scoobi.guide

import org.specs2.runner.FilesRunner
import org.specs2.main.Arguments

class IndexAndUserGuide extends FilesRunner {

  override protected def specifications(implicit args: Arguments) =
    Seq(new Index, new UserGuide)

}
