package com.nicta.scoobi.testing.mutable

import com.nicta.scoobi.testing.HadoopSpecificationStructure
import org.specs2.main.Arguments
import org.specs2.mutable.Specification

/**
 * Hadoop specification with an acceptance specification
 */
class HadoopSpecification(args: Arguments) extends HadoopSpecificationStructure(args) with Specification
