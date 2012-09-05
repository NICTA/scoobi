package com.nicta.scoobi
package plan

import org.specs2.mutable.Specification
import impl.plan.Smart.Load
import io.ConstantStringDataSource

class DCompSpec extends Specification {

  "A DComp node has an equal method based on its case class" >> {
    val (source1, source2) = (ConstantStringDataSource("s1"), ConstantStringDataSource("s2"))
    val (l1, l2) = (Load(source1), Load(source2))
    l1 must not be_==(l2)
    // copying a DComp node with the same information should not change Set containment
    Set(l1, l2).contains(l1.copy(source = source1))
  }

}
