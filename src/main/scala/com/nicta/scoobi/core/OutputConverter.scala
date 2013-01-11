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


