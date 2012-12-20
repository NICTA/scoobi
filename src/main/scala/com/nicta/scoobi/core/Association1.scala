package com.nicta.scoobi
package core

import impl.collection.{Iterator1, Iterable1}
import scalaz._, Scalaz._

/**
 * An association of a key to 1 or many values.
 */
sealed trait Association1[+K, +V] {
  /**
   * The key.
   */
  val key: K

  /**
   * One or many values.
   */
  val values: Iterable1[V]

  /**
   * Map a function on the association. Synonym for `:->`.
   */
  def map[W](f: V => W): Association1[K, W] =
    Association1(key, values map f)

  /**
   * Map a function on the association. Synonym for `map`.
   */
  def :->[W](f: V => W): Association1[K, W] =
    map(f)

  /**
   * Run an effect on each value.
   */
  def foreach[W](f: V => Unit) =
    values foreach f

  /**
   * Run a function on the key to a new key. Synonym for `<-:`.
   */
  def mapKey[L](f: K => L): Association1[L, V] =
    Association1(f(key), values)

  /**
   * Run a function on the key to a new key. Synonym for `mapKey`.
   */
  def <-:[L](f: K => L): Association1[L, V] =
    mapKey(f)

  /**
   * Run a function on the values to new values.
   */
  def mapValues[W](f: Iterable1[V] => Iterable1[W]): Association1[K, W] =
    Association1(key, f(values))

  /**
   * Map two functions on the key and values (binary map).
   */
  def bimap[W, L](f: K => L, g: V => W): Association1[L, W] =
    Association1(f(key), values map g)

  /**
   * Zip the values of this association with the given association to produce an iterable of pairs. Alias for `***`.
   */
  def product[KK >: K, W](w: Association1[KK, W])(implicit S: Semigroup[KK]): Association1[KK, (V, W)] =
    Association1(S.append(key, w.key), values zip w.values)

  /**
   * Zip the values of this association with the given association to produce an iterable of pairs. Alias for `product`.
   */
  def ***[KK >: K, W](w: Association1[KK, W])(implicit S: Semigroup[KK]): Association1[KK, (V, W)] =
    product(w)

  /**
   * Zip the key of this association with the given association to produce an iterable of pairs.
   */
  def productKey[L, VV >: V](w: Association1[L, VV]): Association1[(K, L), VV] =
    Association1((key, w.key), values ++ w.values)

  /**
   * Zip the given association with this association, appending on keys and values. Alias for `<*>:`.
   */
  def ap[KK >: K, W](f: Association1[KK, V => W])(implicit S: Semigroup[KK]): Association1[KK, W] =
    Association1(S.append(f.key, key), values ap f.values)

  /**
   * Zip the given association with this association, appending on keys and values. Alias for `ap``.
   */
  def <*>:[KK >: K, W](f: Association1[KK, V => W])(implicit S: Semigroup[KK]): Association1[KK, W] =
    ap(f)

  /**
   * Return the first value in the associated values.
   */
  def firstValue: V =
    values.head

  /**
   * Traverse this association with the given function on an arbitrary functor.
   */
  def traverseKey[F[_]: Functor, L, VV >: V](f: K => F[L]): F[Association1[L, VV]] =
    implicitly[Functor[F]].map(f(key))(l => Association1(l, values))

  /**
   * True if all values of the association satisfy the given predicate.
   */
  def forall(p: V => Boolean): Boolean =
    values forall p

  /**
   * True if any values of the association satisfy the given predicate.
   */
  def exists(p: V => Boolean): Boolean =
    values exists p

  /**
   * Return the first element in the values satisfying the given predicate.
   */
  def find(p: V => Boolean): Option[V] =
    values find p

  /**
   * Return an iterator on the values.
   */
  def iterator: Iterator1[V] =
    values.iterator

  /**
   * Return the number of values in the association.
   */
  def nvalues: Int =
    values.size

  override def toString: String =
    "Association(" + key + ", " + values.toString + ")"
}

object Association1 {
  /**
   * Construct an assocation with the given key and values.
   */
  def apply[K, V](k: K, vs: Iterable1[V]): Association1[K, V] =
    new Association1[K, V] {
      val key =
        k
      val values =
        vs
    }

  /**
   * Construct an assocation with the given key and one value.
   */
  def single[K, V](k: K, v: V): Association1[K, V] =
    apply(k, Iterable1.single(v))

  /**
   * Construct an assocation with the given key and values.
   */
  def many[K, V](k: K, v: V, vs: V*): Association1[K, V] =
    apply(k, Iterable1(v, vs: _*))

  /**
   * A lens on the key of an association.
   */
  def keyL[K, V]: Association1[K, V] @> K =
    Lens(a => Store(Association1(_, a.values), a.key))

  /**
   * A lens on the values of an association.
   */
  def valuesL[K, V]: Association1[K, V] @> Iterable1[V] =
    Lens(a => Store(Association1(a.key, _), a.values))

  /**
   * A lens on the first value of an association.
   */
  def firstValueL[K, V]: Association1[K, V] @> V =
    valuesL >=> Iterable1.headL

  implicit def Association1Functor[K]: Functor[({type λ[α] = Association1[K, α]})#λ] =
    new Functor[({type λ[α] = Association1[K, α]})#λ] {
      def map[A, B](a: Association1[K, A])(f: A => B) =
        a map f
    }

  implicit def Association1Zip[K: Semigroup]: Zip[({type λ[α] = Association1[K, α]})#λ] =
    new Zip[({type λ[α] = Association1[K, α]})#λ] {
      def zip[A, B](a: => Association1[K, A], b: => Association1[K, B]) =
        a product b
    }

  implicit def Association1ApplyZip[K: Semigroup]: Zip[({type λ[α] = Association1[K, α]})#λ] with Apply[({type λ[α] = Association1[K, α]})#λ] =
    new Zip[({type λ[α] = Association1[K, α]})#λ] with Apply[({type λ[α] = Association1[K, α]})#λ] {
      override def map[A, B](a: Association1[K, A])(f: A => B) =
        a map f
      override def zip[A, B](a: => Association1[K, A], b: => Association1[K, B]) =
        a product b
      def ap[A, B](a: => Association1[K, A])(f: => Association1[K, A => B]) =
        a ap f
    }

  implicit val Associative1Bifunctor: Bifunctor[Association1] =
    new Bifunctor[Association1] {
      def bimap[A, B, C, D](a: Association1[A, B])(f: A => C, g: B => D): Association1[C, D] =
        a bimap (f, g)
    }

  implicit def Association1WireFormat[K: WireFormat, V: WireFormat]: WireFormat[Association1[K, V]] = {
    val k = implicitly[WireFormat[(K, Iterable1[V])]]
    k xmap
      (e => Association1(e._1, e._2), (q: Association1[K, V]) => (q.key, q.values))
  }
}
