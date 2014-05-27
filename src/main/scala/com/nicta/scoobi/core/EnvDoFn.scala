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

import collection.immutable.VectorBuilder
import impl.plan.comp.ParallelDo
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.apache.hadoop.conf.Configuration

/**
 * Interface for specifying parallel operation over DLists. The semantics
 * of DoFn lifecycle are as follows:
 *
 * For a given chunk of DList elements:
 * 1. 'setup' will be called;
 * 2. 'process' will be called for each element in the chunk;
 * 3. 'cleanup' will be called.
 *
 * These 3 steps encapsulate the entire life-cycle of a DoFn. A DoFn object
 * will not be referenced after these steps
 */
trait EnvDoFn[A, B, E] extends DoFunction { outer =>
  private def typedEmitter(emitter: EmitterWriter) = new Emitter[B] with DelegatedScoobiJobContext {
    def emit(x: B) { emitter.write(x) }
    def delegate = emitter
  }
  private[scoobi] def setupFunction(env: Any) { setup(env.asInstanceOf[E]) }
  private[scoobi] def processFunction(env: Any, input: Any, emitter: EmitterWriter) { process(env.asInstanceOf[E], input.asInstanceOf[A], typedEmitter(emitter)) }
  private[scoobi] def cleanupFunction(env: Any, emitter: EmitterWriter) { cleanup(env.asInstanceOf[E], typedEmitter(emitter)) }

  def setup(env: E)
  def process(env: E, input: A, emitter: Emitter[B])
  def cleanup(env: E, emitter: Emitter[B])
}

/**
 * Interface for specifying parallel operation over DLists in the absence of an
 * environment
 */
trait DoFn[A, B] extends EnvDoFn[A, B, Unit] {
  def setup()
  def process(input: A, emitter: Emitter[B])
  def cleanup(emitter: Emitter[B])

  final def setup(env: Unit) { setup() }
  final def process(env: Unit, input: A, emitter: Emitter[B]) { process(input, emitter) }
  final def cleanup(env: Unit, emitter: Emitter[B]) { cleanup(emitter) }
}

object DoFn {
  def apply[A, B](proc: (A, Emitter[B]) => Unit) = new DoFn[A, B] {
    def setup() {}
    def process(input: A, emitter: Emitter[B]) { proc(input, emitter) }
    def cleanup(emitter: Emitter[B]) {}
  }

  def fromFunctionWithCounters[A, B](fn: (A, Counters) => B) = new DoFn[A, B] {
    def setup() {}
    def process(input: A, emitter: Emitter[B]) { emitter.write(fn(input, emitter)) }
    def cleanup(emitter: Emitter[B]) {}
  }

  def fromFunctionWithHeartbeat[A, B](fn: (A, Heartbeat) => B) = new DoFn[A, B] {
    def setup() {}
    def process(input: A, emitter: Emitter[B]) { emitter.write(fn(input, emitter)) }
    def cleanup(emitter: Emitter[B]) {}
  }

  def fromFunctionWithScoobiJobContext[A, B](fn: (A, ScoobiJobContext) => B) = new DoFn[A, B] {
    def setup() {}
    def process(input: A, emitter: Emitter[B]) { emitter.write(fn(input, emitter)) }
    def cleanup(emitter: Emitter[B]) {}
  }

  def fromFunctionWithConfiguration[A, B](fn: (A, Configuration) => B) = new DoFn[A, B] {
    def setup() {}
    def process(input: A, emitter: Emitter[B]) { emitter.write(fn(input, emitter.configuration)) }
    def cleanup(emitter: Emitter[B]) {}
  }

}

/** Interface for writing outputs from a DoFn */
trait Emitter[A] extends EmitterWriter {
  private[scoobi]
  def write(value: Any) { emit(value.asInstanceOf[A]) }
  def emit(value: A)
}

/**
 * Internal version of a EnvDoFn functions without type information
 */
private[scoobi]
trait DoFunction {
  private[scoobi] def setupFunction(env: Any)
  private[scoobi] def processFunction(env: Any, input: Any, emitter: EmitterWriter)
  private[scoobi] def cleanupFunction(env: Any, emitter: EmitterWriter)
}

private[scoobi]
case class BasicDoFunction(f: (Any, Any, EmitterWriter) => Any) extends DoFunction {
  private[scoobi] def setupFunction(en: Any) {}
  private[scoobi] def processFunction(env: Any, input: Any, emitter: EmitterWriter) { f(env, input, emitter) }
  private[scoobi] def cleanupFunction(env: Any, emitter: EmitterWriter) {}
}

private[scoobi]
case class MapFunction(f: Any => Any) extends DoFunction {
  private[scoobi] def setupFunction(en: Any) {}
  private[scoobi] def processFunction(env: Any, input: Any, emitter: EmitterWriter) { emitter.write(f(input)) }
  private[scoobi] def cleanupFunction(env: Any, emitter: EmitterWriter) {}
}

/**
 * simple forwarder to an Emitter
 */
private[scoobi]
object EmitterDoFunction extends DoFunction {
  private[scoobi] def setupFunction(en: Any) {}
  private[scoobi] def processFunction(env: Any, input: Any, emitter: EmitterWriter) { emitter.write(input) }
  private[scoobi] def cleanupFunction(env: Any, emitter: EmitterWriter) {}
}


/**
 * Untyped emitter
 */
private[scoobi]
trait EmitterWriter extends ScoobiJobContext {
  private[scoobi]
  def write(value: Any)
}

trait ScoobiJobContext extends Counters with Heartbeat {
  def configuration: Configuration
}

trait Counters {
  def incrementCounter(groupName: String, name: String, increment: Long = 1)
  def getCounter(groupName: String, name: String): Long
}

trait Heartbeat {
  def tick
}
trait NoScoobiJobContext extends NoCounters with NoHeartbeat
trait NoCounters extends Counters {
  def incrementCounter(groupName: String, name: String, increment: Long = 1) {}
  def getCounter(groupName: String, name: String) = -1
}
trait NoHeartbeat extends Heartbeat {
  def tick {}
}

trait DelegatedScoobiJobContext extends ScoobiJobContext { outer =>
  def incrementCounter(groupName: String, name: String, increment: Long = 1) { delegate.incrementCounter(groupName, name, increment) }
  def getCounter(groupName: String, name: String) = delegate.getCounter(groupName, name)
  def tick { delegate.tick }
  def delegate: ScoobiJobContext
  def configuration = delegate.configuration
}

trait InputOutputContextScoobiJobContext extends ScoobiJobContext {
  def incrementCounter(groupName: String, name: String, increment: Long = 1) {
    context.incrementCounter(groupName, name, increment)
  }
  def getCounter(groupName: String, name: String) = {
    context.getCounter(groupName, name)
  }
  def tick { context.tick }

  def context: InputOutputContext
}

