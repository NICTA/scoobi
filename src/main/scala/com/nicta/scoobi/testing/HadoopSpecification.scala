package com.nicta.scoobi
package testing

import org.specs2.Specification
import application.ScoobiAppConfiguration

/**
 * Hadoop specification with an acceptance specification
 */
abstract class HadoopSpecification extends Specification with HadoopSpecificationStructure with ScoobiAppConfiguration

