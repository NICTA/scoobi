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
package core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.MapContext

/**
 * Convert an InputFormat's key-value types to the type produced by a source
 */
trait InputConverter[K, V, A] extends FromKeyValueConverter {
  type InputContext = MapContext[K, V, _, _]
  def asValue(context: InputOutputContext, key: Any, value: Any): Any = fromKeyValue(context.context.asInstanceOf[InputContext], key.asInstanceOf[K], value.asInstanceOf[V])
  def fromKeyValue(context: InputContext, key: K, value: V): A
}

/** Convert the type consumed by a DataSink into an OutputFormat's key-value types. */
trait OutputConverter[K, V, B] extends ToKeyValueConverter {
  protected[scoobi]
  def asKeyValue(x: Any)(implicit configuration: Configuration) = toKeyValue(x.asInstanceOf[B]).asInstanceOf[(Any, Any)]
  def toKeyValue(x: B)(implicit configuration: Configuration): (K, V)
}

/**
 * Internal untyped output converter from value to (key,value)
 */
private[scoobi]
trait ToKeyValueConverter {
  protected[scoobi]
  def asKeyValue(x: Any)(implicit configuration: Configuration): (Any, Any)
}

/** fusion of both trait when bi-directional conversion is possible */
trait InputOutputConverter[K, V, B] extends OutputConverter[K, V, B] with InputConverter[K, V, B]