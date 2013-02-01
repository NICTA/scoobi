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

/** Convert the type consumed by a DataSink into an OutputFormat's key-value types. */
trait OutputConverter[K, V, B] extends ToKeyValueConverter {
  protected[scoobi]
  def asKeyValue(x: Any) = toKeyValue(x.asInstanceOf[B]).asInstanceOf[(Any, Any)]
  def toKeyValue(x: B): (K, V)
}

/**
 * Internal untyped output converter from value to (key,value)
 */
private[scoobi]
trait ToKeyValueConverter {
  protected[scoobi]
  def asKeyValue(x: Any): (Any, Any)
}


