package com.nicta.scoobi
package impl
package mapreducer

import core._
import ManifestWireFormat._
import WireFormat._
import org.apache.hadoop.conf.Configuration

sealed trait MapReducer[A] {

  def mwf: ManifestWireFormat[A]
  def mf: Manifest[A] = mwf.mf
  def wf: WireFormat[A] = mwf.wf

  def makeTaggedIdentityMapper(tags: Set[Int]) =
    new TaggedIdentityMapper(tags, manifestWireFormat[Int], grouping[Int], mwf) {
      override def map(env: Any, input: Any, emitter: Emitter[Any]) { emitter.emit((RollingInt.get, input)) }
    }

  def makeTaggedIdentityReducer(tag: Int) = new TaggedIdentityReducer(tag, mwf)
}

case class SimpleMapReducer[A](mwf: ManifestWireFormat[A]) extends MapReducer[A] {
  override def toString =
    if (mwf.toString == "Unit") ""
    else                        "["+mwf+"]"
}

case class DoMapReducer[A, B, E](mwfa: ManifestWireFormat[A],
                                 mwfb: ManifestWireFormat[B],
                                 mwfe: ManifestWireFormat[E]) extends MapReducer[B] { outer =>
  def mwf = mwfb

  def setup(env: Any) { dofn.setup(env.asInstanceOf[E]) }
  def reduce(env: Any, key: Any, values: Iterable[Any], emitter: Emitter[Any]) {
    dofn.process(env.asInstanceOf[E], (key, values).asInstanceOf[A], emitter.asInstanceOf[Emitter[B]])
  }
  def cleanup(env: Any, emitter: Emitter[Any]) { dofn.cleanup(env.asInstanceOf[E], emitter.asInstanceOf[Emitter[B]]) }

  def makeTaggedReducer(tag: Int, dofn: EnvDoFn[A, B, E], mwf: ManifestWireFormat[_]) =
    new TaggedReducer(tag, mwf) {
      def setup(env: Any) { dofn.setup(env.asInstanceOf[E]) }
      def reduce(env: Any, key: Any, values: Iterable[Any], emitter: Emitter[Any]) {
        dofn.setup(env.asInstanceOf[E])
        dofn.process(env.asInstanceOf[E], (key, values).asInstanceOf[A], emitter.asInstanceOf[Emitter[B]])
      }
      def cleanup(env: Any, emitter: Emitter[Any]) { dofn.cleanup(env.asInstanceOf[E], emitter.asInstanceOf[Emitter[B]]) }
    }

  def makeTaggedMapper(tags: Set[Int], env: Env[E], dofn: EnvDoFn[A, B, E], mwf: ManifestWireFormat[_]) = new TaggedMapper(tags, manifestWireFormat[Int], grouping[Int], mwf) {
    def setup(implicit configuration: Configuration) { dofn.setup(env.pull) }
    def map(input: Any, emitter: Emitter[Any])(implicit configuration: Configuration) {
      val e = new Emitter[B] { def emit(b: B) { emitter.emit((RollingInt.get, b)) } }
      dofn.process(env.pull, input.asInstanceOf[A], e.asInstanceOf[Emitter[B]])
    }
    def cleanup(emitter: Emitter[Any])(implicit configuration: Configuration) {
      val e = new Emitter[B] { def emit(b: B) { emitter.emit((RollingInt.get, b)) } }
      dofn.cleanup(env.pull, e.asInstanceOf[Emitter[B]])
    }
  }
  def makeTaggedMapper(tags: Set[Int], env: Env[E], dofn: EnvDoFn[A, B, E], mwfk: ManifestWireFormat[_], gpk: Grouping[_], mwfv: ManifestWireFormat[_]) = new TaggedMapper(tags, mwfk, gpk, mwfv) {
    def setup(implicit configuration: Configuration) { dofn.setup(env.asInstanceOf[E]) }
    def map(input: Any, emitter: Emitter[Any])(implicit configuration: Configuration) {
      dofn.process(env.pull, input.asInstanceOf[A], emitter.asInstanceOf[Emitter[B]])
    }
    def cleanup(emitter: Emitter[Any])(implicit configuration: Configuration) {
      dofn.cleanup(env.pull, emitter.asInstanceOf[Emitter[B]])
    }
  }

  override def toString =
    if (mwfe.isUnit) Seq(mwfa, mwfb).mkString("[", ",", "]")
    else             Seq(mwfa, mwfb, mwfe).mkString("[", ",", "]")

}

case class KeyValueMapReducer[K, V](mwfk: ManifestWireFormat[K], gpk: Grouping[K], mwfv: ManifestWireFormat[V]) extends MapReducer[(K, V)] { outer =>

  def mwf = pairManifestWireFormat[K, V](mwfk, mwfv)

  def toSimpleSinksKey   = SimpleMapReducer(mwfk)
  def toSimpleSinksValue = SimpleMapReducer(mwfv)

  def makeTaggedCombiner(tag: Int, f: (V, V) => V) = new TaggedCombiner[V](tag, toSimpleSinksValue) {
    def combine(x: V, y: V): V = f(x, y)
  }

  def makeTaggedReducer(tag: Int, f: (V, V) => V) = new TaggedReducer(tag, mwf) {
    def setup(env: Any) {}
    def reduce(env: Any, key: Any, values: Iterable[Any], emitter: Emitter[Any]) {
      emitter.emit((key, values.reduce((v1, v2) => f(v1.asInstanceOf[V], v2.asInstanceOf[V]))))
    }
    def cleanup(env: Any, emitter: Emitter[Any]) {}
  }

  def makeTaggedReducer(tag: Int, f: (V, V) => V, dofn: EnvDoFn[_,_,_]) = new TaggedReducer(tag, mwf) {
    def setup(env: Any) { dofn.unsafeSetup(env) }
    def reduce(env: Any, key: Any, values: Iterable[Any], emitter: Emitter[Any]) {
      dofn.unsafeSetup(env)
      if (!values.isEmpty)
        dofn.unsafeProcess(env, (key, values.asInstanceOf[Iterable[V]].reduce(f)), emitter)
    }
    def cleanup(env: Any, emitter: Emitter[Any]) { dofn.unsafeCleanup(env, emitter) }

  }

  override def toString = Seq(mwfk, mwfv).mkString("[", ",", "]")

}

case class KeyValuesMapReducer[K, V](mwfk: ManifestWireFormat[K],
                                     gpk: Grouping[K],
                                     mwfv: ManifestWireFormat[V]) extends MapReducer[(K, Iterable[V])] { outer =>

  implicit val ((mfk, wfk), (mfv, wfv)) = (decompose(mwfk), decompose(mwfv))

  lazy val mwf = ManifestWireFormat(manifest[(K, Iterable[V])], wireFormat[(K, Iterable[V])])

  def makeTaggedReducer(tag: Int) = new TaggedReducer(tag, mwf) {
    def setup(env: Any) {}
    def reduce(env: Any, key: Any, values: Iterable[Any], emitter: Emitter[Any]) {
      emitter.emit((key, values))
    }
    def cleanup(env: Any, emitter: Emitter[Any]) {}
  }
  override def makeTaggedIdentityMapper(tags: Set[Int]) = new TaggedIdentityMapper(tags, mwfk, gpk, mwfv)

  override def toString = Seq(mwfk, mwfv).mkString("[", ",", "]")
}



