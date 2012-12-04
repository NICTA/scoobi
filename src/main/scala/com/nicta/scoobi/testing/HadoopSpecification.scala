package com.nicta.scoobi
package testing

import org.specs2.Specification
import application.ScoobiAppConfiguration

/**
 * Hadoop specification with an acceptance specification
 */
trait HadoopSpecification extends Specification with HadoopSpecificationStructure with ScoobiAppConfiguration

