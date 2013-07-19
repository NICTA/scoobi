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

import org.kiama.attribution.Attributable
import scalaz.Equal

/**
 * Base trait for "computation nodes" with no generic type information for easier rewriting
 *
 * Each computation node has a unique id and equality of 2 nodes is based on this id.
 *
 * CompNodes are Attributable so that they can be used in attribute grammars
 */
trait CompNode extends Attributable {
  type C <: CompNode
  /** unique identifier for this computation node */
  def id: Int

  /** object defining how to serialise / deserialise  data for that node */
  def wf: WireReaderWriter

  override def equals(a: Any) = a match {
    case n: CompNode => n.id == id
    case other       => false
  }
  override def hashCode = id.hashCode

  def sinks: Seq[Sink]
  def hasCheckpoint = sinks.exists { case s: SinkSource => s.isCheckpoint; case _ => false }

  def updateSinks(f: Seq[Sink] => Seq[Sink]): C
  def addSink(sink: Sink) = updateSinks(sinks => sinks :+ sink)
}

/**
 * Definition of the Equal instance for CompNodes
 */
object CompNode {
  implicit def compNodeEqual[T <: CompNode] = new Equal[T] {
    def equal(a1: T, a2: T) = a1.id == a2.id
  }
}

trait ProcessNode extends CompNode {
  type C = ProcessNode
  def bridgeStore: Bridge
  def bridgeStoreId: String
  def nodeSinks : Seq[Sink]
}
trait ValueNode extends CompNode {
  type C = ValueNode
  def environment(sc: ScoobiConfiguration): Environment
  def pushEnv(result: Any)(implicit sc: ScoobiConfiguration)
}


