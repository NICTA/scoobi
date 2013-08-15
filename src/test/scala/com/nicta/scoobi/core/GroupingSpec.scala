package com.nicta.scoobi
package core

import testing._

class GroupingSpec extends UnitSpecification { def is = s2"""
  Calling the partition method on Groupings must not trigger a DivByZero exception $e1
"""

  def e1 = Grouping.groupingId[Int].partition(1, 0) must not(throwAn[Exception])
}
