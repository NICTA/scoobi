/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

