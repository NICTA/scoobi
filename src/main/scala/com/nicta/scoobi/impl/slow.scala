package com.nicta.scoobi
package impl

import annotation.target._

/** An annotation that designates that a definition is slow and has a faster alternative implementation
 *  Access to the member then generates a 'slow' warning.
 *
 *  @param  message the message to print during compilation if the definition is accessed
 *  @param  since   a string identifying the first version in which the definition was marked as slow
 *  @since  2.3
 */
@getter @setter @beanGetter @beanSetter
class slow(message: String = "", since: String = "") extends annotation.StaticAnnotation

