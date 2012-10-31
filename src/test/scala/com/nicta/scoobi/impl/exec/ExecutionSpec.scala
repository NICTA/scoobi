package com.nicta.scoobi
package impl
package exec

import org.specs2.specification.Grouped
import plan.mscr.{BypassOutputChannel, MapperInputChannel, Mscr}
import application.ScoobiConfiguration
import testing.UnitSpecification
import plan.comp.factory
import HadoopMode._

class ExecutionSpec extends UnitSpecification with Grouped { def is =

  "The Execution class takes a list of mscrs to compute and executes them one by one"  ^
                                                                                       p^
  "It is possible to collect all the input Mscrs of a given Mscr"                      ! g1.e1^
  "Mscrs must be computed according to their dependencies"                             ! g1.e2^
  end

  "compute" - new g1 with factory {
    val (pd1, pd2) = (pd(load), pd(load))
    implicit val configuration = ScoobiConfiguration()

  }

}
