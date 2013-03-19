package com.nicta.scoobi
package core

import scalaz._, Scalaz._, BijectionT._

/*
 * A closed, binary operation on a set. Implementations may benefit by adhering to the law of associativity, however,
 * this is *not* mandatory.
 *
 * Associativity
 * =============
 * Implementations of the `Reduction` trait supply a value for the reduce operation: (A, A) => A.
 * This is typically done using the existing `Reduction` values,
 * however a user can supply their own with the `Reduction#apply` function.
 *
 * This function is described as:
 * - binary, since it accepts 2 arguments.
 * - closed, since that operation accepts and returns arguments of the same type (set).
 * - implementations may satisfy the law of associativity.
 *
 * The associativity law on the `reduce` operation is given by:
 * ∀ x y z. reduce(reduce(x, y), z) = reduce(x, reduce(y, z))
 *
 * Equivalence is defined here by extensional equivalence.
 *
 * Implementers can check adherence to the law of associativity given by the `Reduction#associative` method.
 * For a given Reduction (`r`), `r.associative(x, y, z)` always returns `true` if it is associative.
 * (within the bounds of extensional equivalence).
 */
trait Reduction[A] {
  /**
   * The binary operation.
   */
  val reduce: (A, A) => A

  /**
   * The binary operation. Alias for `-\`.
   */
  def apply(a1: A, a2: A): A =
    reduce(a1, a2)

  /**
   * The binary operation. Alias for `apply`.
   */
  def -\(a1: A, a2: A): A =
    apply(a1, a2)

  /**
   * The binary operation in curried form.
   */
  def reduceC: A => A => A =
    reduce.curried

  /**
   * Swaps the arguments to the binary operation. Alias for `unary_~`.
   */
  def dual: Reduction[A] =
    Reduction((a1, a2) => reduce(a2, a1))

  /**
   * Swaps the arguments to the binary operation. Alias for `dual`.
   */
  def unary_~ : Reduction[A] =
    dual

  /**
   * Return a unary operation that applies to both sides of the binary operation.
   */
  def pair: A => A =
    a => reduce(a, a)

  /**
   * Return a unary operation that applies to both sides of the binary operation as an endomorphism.
   */
  def pairE: Endo[A] =
    Endo(pair)

  /**
   * Takes a pair of reductions to a reduction on pairs. Alias for `***`.
   */
  def zip[B](r: Reduction[B]): Reduction[(A, B)] =
    Reduction {
      case ((a1, b1), (a2, b2)) => (reduce(a1, a2), r reduce (b1, b2))
    }

  /**
   * Takes a pair of reductions to a reduction on pairs. Alias for `zip`.
   */
  def ***[B](r: Reduction[B]): Reduction[(A, B)] =
    zip(r)

  /**
   * Takes three reductions to a reduction on tuple-3.
   */
  def zip3[B, C](b: Reduction[B], c: Reduction[C]): Reduction[(A, B, C)] =
    Reduction {
      case ((a1, b1, c1), (a2, b2, c2)) => (reduce(a1, a2), b reduce (b1, b2), c reduce (c1, c2))
    }

  /**
   * Takes four reductions to a reduction on tuple-4.
   */
  def zip4[B, C, D](b: Reduction[B], c: Reduction[C], d: Reduction[D]): Reduction[(A, B, C, D)] =
    Reduction {
      case ((a1, b1, c1, d1), (a2, b2, c2, d2)) => (reduce(a1, a2), b reduce (b1, b2), c reduce (c1, c2), d reduce (d1, d2))
    }

  /**
   * Takes five reductions to a reduction on tuple-5.
   */
  def zip5[B, C, D, E](b: Reduction[B], c: Reduction[C], d: Reduction[D], e: Reduction[E]): Reduction[(A, B, C, D, E)] =
    Reduction {
      case ((a1, b1, c1, d1, e1), (a2, b2, c2, d2, e2)) => (reduce(a1, a2), b reduce (b1, b2), c reduce (c1, c2), d reduce (d1, d2), e reduce (e1, e2))
    }

  /**
   * Takes a reduction and a semigroup to a reduction on pairs.
   */
  def wzip[W](implicit S: Semigroup[W]): Reduction[(W, A)] =
    Reduction {
      case ((w1, a1), (w2, a2)) => (S.append(w1, w2), reduce(a1, a2))
    }

  /**
   * Takes a reduction and two semigroups to a reduction on tuple-3.
   */
  def wzip3[W, X](implicit SW: Semigroup[W], SX: Semigroup[X]): Reduction[(W, X, A)] =
    Reduction {
      case ((w1, x1, a1), (w2, x2, a2)) => (SW.append(w1, w2), SX.append(x1, x2), reduce(a1, a2))
    }

  /**
   * Takes a reduction and three semigroups to a reduction on tuple-4.
   */
  def wzip4[W, X, Y](implicit SW: Semigroup[W], SX: Semigroup[X], SY: Semigroup[Y]): Reduction[(W, X, Y, A)] =
    Reduction {
      case ((w1, x1, y1, a1), (w2, x2, y2, a2)) => (SW.append(w1, w2), SX.append(x1, x2), SY.append(y1, y2), reduce(a1, a2))
    }

  /**
   * Takes a reduction and four semigroups to a reduction on tuple-5.
   */
  def wzip5[W, X, Y, Z](implicit SW: Semigroup[W], SX: Semigroup[X], SY: Semigroup[Y], SZ: Semigroup[Z]): Reduction[(W, X, Y, Z, A)] =
    Reduction {
      case ((w1, x1, y1, z1, a1), (w2, x2, y2, z2, a2)) => (SW.append(w1, w2), SX.append(x1, x2), SY.append(y1, y2), SZ.append(z1, z2), reduce(a1, a2))
    }

  /**
   * Takes a pair of reductions to a reduction on a disjunction with left (A) bias.
   */
  def left[B](r: => Reduction[B]): Reduction[A \/ B] =
    Reduction((x1, x2) => x1 match {
      case -\/(a1) => x2 match {
        case -\/(a2) => -\/(reduce(a1, a2))
        case \/-(_) => -\/(a1)
      }
      case \/-(b1) => x2 match {
        case -\/(a2) => -\/(a2)
        case \/-(b2) => \/-(r reduce (b1, b2))
      }
    })

  /**
   * Takes a pair of reductions to a reduction on a disjunction with right (B) bias.
   */
  def right[B](r: => Reduction[B]): Reduction[A \/ B] =
    Reduction((x1, x2) => x1 match {
      case -\/(a1) => x2 match {
        case -\/(a2) => -\/(reduce(a1, a2))
        case \/-(b2) => \/-(b2)
      }
      case \/-(b1) => x2 match {
        case -\/(a2) => \/-(b1)
        case \/-(b2) => \/-(r reduce (b1, b2))
      }
    })

  /**
   * Takes a reduction and produces a reduction on `Option`, taking as many `Some` values as possible.
   */
  def option: Reduction[Option[A]] =
    Reduction((a1, a2) => a1 match {
      case None => a2
      case Some(aa1) => a2 match {
        case None => Some(aa1)
        case Some(aa2) => Some(reduce(aa1, aa2))
      }
    })

  /**
   * Takes a reduction to a reduction on a unary function.
   */
  def pointwise[B]: Reduction[B => A] =
    Reduction((g, h) => b => reduce(g(b), h(b)))

  /**
   * Takes a reduction to a reduction on a binary function.
   */
  def pointwise2[B, C]: Reduction[(B, C) => A] =
    Reduction((g, h) => (b, c) => reduce(g(b, c), h(b, c)))

  /**
   * Takes a reduction to a reduction on a ternary function.
   */
  def pointwise3[B, C, D]: Reduction[(B, C, D) => A] =
    Reduction((g, h) => (b, c, d) => reduce(g(b, c, d), h(b, c, d)))

  /**
   * Takes a reduction to a reduction on an arity-4 function.
   */
  def pointwise4[B, C, D, E]: Reduction[(B, C, D, E) => A] =
    Reduction((g, h) => (b, c, d, e) => reduce(g(b, c, d, e), h(b, c, d, e)))

  /**
   * Takes a reduction to a reduction on an arity-5 function.
   */
  def pointwise5[B, C, D, E, F]: Reduction[(B, C, D, E, F) => A] =
    Reduction((g, h) => (b, c, d, e, f) => reduce(g(b, c, d, e, f), h(b, c, d, e, f)))

  /**
   * Takes a reduction to a reduction on a unary function in an environment (Q).
   */
  def pointwiseK[Q[+_], B](implicit A: Apply[Q]): Reduction[Kleisli[Q, B, A]] =
    Reduction((g, h) => Kleisli(
      b => A.apply2(g(b), h(b))(reduce(_, _))
    ))

  /**
   * Takes a reduction to a reduction on a binary function with an environment (Q) in return position.
   */
  def pointwise2K[Q[+_], B, C](implicit A: Apply[Q]): Reduction[Kleisli[Q, (B, C), A]] =
    Reduction((g, h) => Kleisli {
      case (b, c) => A.apply2(g(b, c), h(b, c))(reduce(_, _))
    })

  /**
   * Takes a reduction to a reduction on a ternary function with an environment (Q) in return position.
   */
  def pointwise3K[Q[+_], B, C, D](implicit A: Apply[Q]): Reduction[Kleisli[Q, (B, C, D), A]] =
    Reduction((g, h) => Kleisli {
      case (b, c, d) => A.apply2(g(b, c, d), h(b, c, d))(reduce(_, _))
    })

  /**
   * Takes a reduction to a reduction on an arity-4 function with an environment (Q) in return position.
   */
  def pointwise4K[Q[+_], B, C, D, E](implicit A: Apply[Q]): Reduction[Kleisli[Q, (B, C, D, E), A]] =
    Reduction((g, h) => Kleisli {
      case (b, c, d, e) => A.apply2(g(b, c, d, e), h(b, c, d, e))(reduce(_, _))
    })

  /**
   * Takes a reduction to a reduction on an arity-5 function with an environment (Q) in return position.
   */
  def pointwise5K[Q[+_], B, C, D, E, F](implicit A: Apply[Q]): Reduction[Kleisli[Q, (B, C, D, E, F), A]] =
    Reduction((g, h) => Kleisli {
      case (b, c, d, e, f) => A.apply2(g(b, c, d, e, f), h(b, c, d, e, f))(reduce(_, _))
    })

  /**
   * Takes a reduction to a reduction on a unary function with an environment (Q) in argument position.
   */
  def pointwiseC[Q[+_], B]: Reduction[Cokleisli[Q, B, A]] =
    Reduction((g, h) => Cokleisli(
      b => reduce(g run b, h run b)
    ))

  /**
   * Takes a reduction to a reduction on a binary function with an environment (Q) in argument position.
   */
  def pointwise2C[Q[+_], B, C]: Reduction[Cokleisli[Q, (B, C), A]] =
    Reduction((g, h) => Cokleisli(
      b => reduce(g run b, h run b)
    ))

  /**
   * Takes a reduction to a reduction on a ternary function with an environment (Q) in argument position.
   */
  def pointwise3C[Q[+_], B, C, D]: Reduction[Cokleisli[Q, (B, C, D), A]] =
    Reduction((g, h) => Cokleisli(
      b => reduce(g run b, h run b)
    ))

  /**
   * Takes a reduction to a reduction on an arity-4 function with an environment (Q) in argument position.
   */
  def pointwise4C[Q[+_], B, C, D, E]: Reduction[Cokleisli[Q, (B, C, D, E), A]] =
    Reduction((g, h) => Cokleisli(
      b => reduce(g run b, h run b)
    ))

  /**
   * Takes a reduction to a reduction on an arity-5 function with an environment (Q) in argument position.
   */
  def pointwise5C[Q[+_], B, C, D, E, F]: Reduction[Cokleisli[Q, (B, C, D, E, F), A]] =
    Reduction((g, h) => Cokleisli(
      b => reduce(g run b, h run b)
    ))

  /**
   * Takes a reduction to a reduction on validation values with bias on success.
   */
  def validation[B](b: Reduction[B]): Reduction[Validation[A, B]] =
    Reduction((v1, v2) => v1 match {
      case Failure(a1) => v2 match {
        case Failure(a2) => Failure(reduce (a1, a2))
        case Success(b2) => Success(b2)
      }
      case Success(b1) => v2 match {
        case Failure(a2) => Success(b1)
        case Success(b2) => Success(b reduce (b1, b2))
      }
    })

  /**
   * Takes a pair of reductions to a reduction on store (comonad).
   */
  def store[B](r: Reduction[B]): Reduction[Store[A, B]] =
    Reduction((s1, s2) =>
      Store(r.pointwise[A] reduce (s1 put _, s2 put _), reduce(s1.pos, s2.pos))
    )

  /**
   * Lifts this reduction to a reduction with an environment.
   */
  def lift[F[+_]](implicit A: Apply[F]): Reduction[F[A]] =
    Reduction((a1, a2) =>
      A.apply2(a1, a2)(reduce(_, _)))

  /**
   * Takes a reduction to a reduction on state.
   */
  def state[S]: Reduction[State[S, A]] =
    lift[({type lam[+a]=State[S, a]})#lam]

  /**
   * Takes a reduction to a reduction on writer.
   */
  def writer[W: Semigroup]: Reduction[Writer[W, A]] =
    lift[({type lam[+a]=Writer[W, a]})#lam]

  /**
   * Maps a pair of functions on a reduction to produce a reduction.
   */
  def xmap[B](f: A => B, g: B => A): Reduction[B] =
    Reduction((b1, b2) => f(reduce(g(b1), g(b2))))

  /**
   * Maps a function on the inputs to a reduction to produce a reduction.
   */
  def mapIn(g: A => A): Reduction[A] =
    xmap(identity, g)

  /**
   * Maps a function on the output of a reduction to produce a reduction.
   */
  def mapOut(f: A => A): Reduction[A] =
    xmap(f, identity)

  /**
   * Maps a bijection on a reduction to produce a function.
   */
  def biject[B](b: Bijection[A, B]): Reduction[B] =
    xmap(b to _, b from _)

  /**
   * Maps a function on the inputs and output of a reduction to produce a function.
   */
  def on(f: A => A): Reduction[A] =
    xmap(f, f)

  /**
   * Takes a reduction to a scalaz semigroup.
   */
  def semigroup: Semigroup[A] =
    Semigroup.instance((a1, a2) => reduce(a1, a2))

  /**
   * Lift a reduction to an applicative environment.
   */
  def apply: Apply[({type lam[a]=A})#lam] = new Apply[({type lam[a]=A})#lam] {
    override def map[X, Y](a: A)(f: X => Y) = a
    def ap[X, Y](a: => A)(f: => A) = reduce(f, a)
  }

  /**
   * Lift a reduction to a composition of functors.
   */
  def compose: Compose[({type lam[a, b]=A})#lam] = new Compose[({type lam[a, b]=A})#lam] {
    def compose[X, Y, Z](f: A, g: A) = reduce(f, g)
  }

  /**
   * Encodes the associative law that reductions may satisfy.
   */
  class Associative {
    def apply(a1: A, a2: A, a3: A)(implicit E: Equal[A]): Boolean =
      reduce(reduce(a1, a2), a3) === reduce(a1, (reduce(a2, a3)))

    def by[B](a1: A, a2: A, a3: A)(f: A => B)(implicit E: Equal[B]): Boolean =
      f(reduce(reduce(a1, a2), a3)) === f(reduce(a1, (reduce(a2, a3))))

    def on[B](b1: B, b2: B, b3: B)(f: B => A)(implicit E: Equal[A]): Boolean =
      as(b1, b2, b3)(f)(E equal (_, _))

    def as[B](b1: B, b2: B, b3: B)(f: B => A)(g: (A, A) => Boolean): Boolean =
      g(reduce(reduce(f(b1), f(b2)), f(b3)), reduce(f(b1), (reduce(f(b2), f(b3)))))

  }
  def associative: Associative = new Associative

}

object Reduction extends Reductions
trait Reductions {
  /**
   * Construct a reduction from the given binary operation.
   */
  def apply[A](f: (A, A) => A): Reduction[A] =
    new Reduction[A] {
      val reduce = f
    }

  /**
   * Construct a reduction that ignores its argument pair.
   */
  def constant[A](a: => A): Reduction[A] =
    apply((_, _) => a)

  /**
   * Construct a reduction that applies its first argument to the given function.
   */
  def split1[A](f: A => A): Reduction[A] =
    apply((a1, _) => f(a1))

  /**
   * Construct a reduction that applies its first argument to the given endomorphism.
   */
  def split1E[A](e: Endo[A]): Reduction[A] =
    split1(e.run)

  /**
   * Construct a reduction that applies its second argument to the given function.
   */
  def split2[A](f: A => A): Reduction[A] =
    apply((_, a2) => f(a2))

  /**
   * Construct a reduction that applies its second argument to the given endomorphism.
   */
  def split2E[A](e: Endo[A]): Reduction[A] =
    split2(e.run)

  /**
   * A reduction that cancels.
   */
  def unit: Reduction[Unit] =
    constant(())

  /**
   * A reduction that returns the first argument.
   */
  def first[A]: Reduction[A] =
    Reduction((a, _) => a)

  /**
   * A reduction that returns the last argument.
   */
  def last[A]: Reduction[A] =
    Reduction((_, a) => a)

  /**
   * A reduction that tries the first option for `Some`, otherwise the last.
   */
  def firstOption[A]: Reduction[Option[A]] =
    Reduction((a1, a2) => a1 orElse a2)

  /**
   * A reduction that tries the last option for `Some`, otherwise the first.
   */
  def lastOption[A]: Reduction[Option[A]] =
    Reduction((a1, a2) => a2 orElse a1)

  /**
   * A reduction that produces the minimum value of its two arguments.
   */
  def minimum[A](implicit O: Order[A]): Reduction[A] =
    Reduction((a1, a2) => O min (a1, a2))

  /**
   * A reduction that produces the maximum value of its two arguments.
   */
  def maximum[A](implicit O: Order[A]): Reduction[A] =
    Reduction((a1, a2) => O max (a1, a2))

  /**
   * A reduction that produces the minimum value of its two arguments.
   */
  def minimumS[A](implicit O: math.Ordering[A]): Reduction[A] =
    Reduction((a1, a2) => O min (a1, a2))

  /**
   * A reduction that produces the maximum value of its two arguments.
   */
  def maximumS[A](implicit O: math.Ordering[A]): Reduction[A] =
    Reduction((a1, a2) => O max (a1, a2))

  /**
   * A reduction that composes two functions.
   */
  def endo[A]: Reduction[A => A] =
    Reduction((a1, a2) => a1 compose a2)

  /**
   * A reduction that composes two endomorphisms.
   */
  def endoE[A]: Reduction[Endo[A]] =
    Reduction((a1, a2) => a1 compose a2)

  /**
   * A reduction that takes the disjunction of its two arguments.
   */
  def or: Reduction[Boolean] =
    Reduction(_ || _)

  /**
   * A reduction that takes the conjunction of its two arguments.
   */
  def and: Reduction[Boolean] =
    Reduction(_ && _)

  /**
   * Takes a boolean reduction to a reduction on equal instances.
   */
  def equal[A](r: Reduction[Boolean]): Reduction[Equal[A]] =
    Reduction((e1, e2) => new Equal[A] {
      def equal(a1: A, a2: A) =
        r.reduce(e1 equal (a1, a2), e2 equal (a1, a2))
    })

  /**
   * Takes a boolean reduction to a reduction on unequal instances.
   */
  def unequal[A](r: Reduction[Boolean]): Reduction[Equal[A]] =
    Reduction((e1, e2) => new Equal[A] {
      def equal(a1: A, a2: A) =
        r.reduce(!(e1 equal (a1, a2)), !(e2 equal (a1, a2)))
    })

  /**
   * Takes an ordering reduction to a reduction on order instances.
   */
  def order[A](r: Reduction[Ordering]): Reduction[Order[A]] =
    Reduction((e1, e2) => new Order[A] {
      def order(a1: A, a2: A) =
        r.reduce(e1 order (a1, a2), e2 order (a1, a2))
    })

  /**
   * A reduction on ordering (3) values by first looking for `EQ`.
   */
  def ordering: Reduction[Ordering] =
    Reduction((a1, a2) => a1 match {
      case Ordering.EQ => a2
      case _ => a1
    })

  /**
   * A reduction on comparable values by first looking for `0`.
   */
  def comparable[A]: Reduction[Comparable[A]] =
    Reduction((c1, c2) => new Comparable[A] {
      def compareTo(a: A) =
        c1.compareTo(a) match {
          case 0 => c2.compareTo(a)
          case n => n
        }
    })

  /**
   * A reduction on comparator values by first looking for `0`.
   */
  def comparator[A]: Reduction[java.util.Comparator[A]] =
    Reduction((c1, c2) => new java.util.Comparator[A] {
      def compare(a1: A, a2: A) =
        c1.compare(a1, a2) match {
          case 0 => c2.compare(a1, a2)
          case n => n
        }
    })

  /**
   * A reduction on scala ordering by first looking for `0`.
   */
  def orderingS[A]: Reduction[math.Ordering[A]] =
    Reduction((o1, o2) => new math.Ordering[A] {
      def compare(a1: A, a2: A) =
        o1.compare(a1, a2) match {
          case 0 => o2.compare(a1, a2)
          case n => n
        }
    })

  /**
   * A reduction on strings by appending.
   */
  def string: Reduction[String] =
    Reduction(_ + _)

  /**
   * A reduction on lists by appending.
   */
  def list[A]: Reduction[List[A]] =
    Reduction(_ ::: _)

  /**
   * A reduction on streams by appending.
   */
  def stream[A]: Reduction[Stream[A]] =
    Reduction(_ #::: _)

  /**
   * A reduction on ephemeral streams by appending.
   */
  def ephemeralStream[A]: Reduction[EphemeralStream[A]] =
    Reduction(_ ++ _)

  /**
   * A reduction on vectors by appending.
   */
  def vector[A]: Reduction[Vector[A]] =
    Reduction(_ ++ _)

  /**
   * A reduction on arrays by appending.
   */
  def array[A: ClassManifest]: Reduction[Array[A]] =
    Reduction((c1, c2) => (c1 ++ c2).toArray)

  /**
   * A reduction on difference lists by appending.
   */
  def differenceList[A]: Reduction[DList[A]] =
    Reduction(_ ++ _)

  /**
   * A reduction on non-empty lists by appending.
   */
  def nonEmptyList[A]: Reduction[NonEmptyList[A]] =
    Reduction(_ append _)

  /**
   * A reduction on int maps by appending.
   */
  def intmap[A]: Reduction[collection.immutable.IntMap[A]] =
    Reduction(_ ++ _)

  /**
   * A reduction on long maps by appending.
   */
  def longmap[A]: Reduction[collection.immutable.LongMap[A]] =
    Reduction(_ ++ _)

  /**
   * A reduction on hash maps by appending.
   */
  def hashmap[A, B]: Reduction[collection.immutable.HashMap[A, B]] =
    Reduction(_ ++ _)

  /**
   * A reduction on hash sets by appending.
   */
  def hashset[A]: Reduction[collection.immutable.HashSet[A]] =
    Reduction(_ ++ _)

  /**
   * A reduction on tree maps by appending.
   */
  def treemap[A, B]: Reduction[collection.immutable.TreeMap[A, B]] =
    Reduction(_ ++ _)

  /**
   * A reduction on tree sets by appending.
   */
  def treeset[A]: Reduction[collection.immutable.TreeSet[A]] =
    Reduction(_ ++ _)

  /**
   * A reduction on list maps by appending.
   */
  def listmap[A, B]: Reduction[collection.immutable.ListMap[A, B]] =
    Reduction(_ ++ _)

  /**
   * A reduction on list sets by appending.
   */
  def listset[A]: Reduction[collection.immutable.ListSet[A]] =
    Reduction(_ ++ _)

  /**
   * A reduction on queues by appending.
   */
  def queue[A]: Reduction[collection.immutable.Queue[A]] =
    Reduction(_ ++ _)

  /**
   * A reduction on stacks by appending.
   */
  def stack[A]: Reduction[collection.immutable.Stack[A]] =
    Reduction(_ ++ _)

  /**
   * A reduction on xml node sequences by appending.
   */
  def nodeseq: Reduction[scala.xml.NodeSeq] =
    Reduction(_ ++ _)

  /**
   * Reductions that perform addition.
   */
  object Sum {

    /**
     * Reduction on big integers by addition.
     */
    def bigint: Reduction[BigInt] =
      Reduction(_ + _)

    /**
     * Reduction on java big integers by addition.
     */
    def biginteger: Reduction[java.math.BigInteger] =
      Reduction(_ add _)

    /**
     * Reduction on big decimals by addition.
     *
     * _Note that this is not an associative operation._
     */
    def bigdec: Reduction[BigDecimal] =
      Reduction(_ + _)

    /**
     * Reduction on java big decimals by addition.
     *
     * _Note that this is not an associative operation._
     */
    def bigdecimal: Reduction[java.math.BigDecimal] =
      Reduction(_ add _)

    /**
     * Reduction on floats by addition.
     *
     * _Note that this is not an associative operation._
     */
    def float: Reduction[Float] =
      Reduction((c1, c2) => c1 + c2)

    /**
     * Reduction on doubles by addition.
     *
     * _Note that this is not an associative operation._
     */
    def double: Reduction[Double] =
      Reduction((c1, c2) => c1 + c2)

    /**
     * Reduction on bytes by addition.
     */
    def byte: Reduction[Byte] =
      Reduction((c1, c2) => (c1 + c2).toByte)

    /**
     * Reduction on characters by addition.
     */
    def char: Reduction[Char] =
      Reduction((c1, c2) => (c1 + c2).toChar)

    /**
     * Reduction on integers by addition.
     */
    def int: Reduction[Int] =
      Reduction(_ + _)

    /**
     * Reduction on longs by addition.
     */
    def long: Reduction[Long] =
      Reduction(_ + _)

    /**
     * Reduction on shorts by addition.
     */
    def short: Reduction[Short] =
      Reduction((c1, c2) => (c1 + c2).toShort)

    /**
     * Reduction on digits by addition.
     */
    def digit: Reduction[Digit] =
      Reduction((d1, d2) => Digit.mod10Digit(d1.toInt + d2.toInt))

  }

  /**
   * Reductions that perform multiplication.
   */
  object Product {
    /**
     * Reduction on big integers by multiplication.
     */
    def bigint: Reduction[BigInt] =
      Reduction(_ * _)

    /**
     * Reduction on java big integers by multiplication.
     */
    def biginteger: Reduction[java.math.BigInteger] =
      Reduction(_ multiply _)

    /**
     * Reduction on big decimals by multiplication.
     *
     * _Note that this is not an associative operation._
     */
    def bigdec: Reduction[BigDecimal] =
      Reduction(_ * _)

    /**
     * Reduction on java big decimals by multiplication.
     *
     * _Note that this is not an associative operation._
     */
    def bigdecimal: Reduction[java.math.BigDecimal] =
      Reduction(_ multiply _)

    /**
     * Reduction on floats by multiplication.
     *
     * _Note that this is not an associative operation._
     */
    def float: Reduction[Float] =
      Reduction((c1, c2) => c1 * c2)

    /**
     * Reduction on doubles by addition.
     *
     * _Note that this is not an associative operation._
     */
    def double: Reduction[Double] =
      Reduction((c1, c2) => c1 * c2)

    /**
     * Reduction on bytes by multiplication.
     */
    def byte: Reduction[Byte] =
      Reduction((c1, c2) => (c1 * c2).toByte)

    /**
     * Reduction on chars by multiplication.
     */
    def char: Reduction[Char] =
      Reduction((c1, c2) => (c1 * c2).toChar)

    /**
     * Reduction on integers by multiplication.
     */
    def int: Reduction[Int] =
      Reduction(_ * _)

    /**
     * Reduction on longs by multiplication.
     */
    def long: Reduction[Long] =
      Reduction(_ * _)

    /**
     * Reduction on shorts by multiplication.
     */
    def short: Reduction[Short] =
      Reduction((c1, c2) => (c1 * c2).toShort)

    /**
     * Reduction on digits by multiplication.
     */
    def digit: Reduction[Digit] =
      Reduction((d1, d2) => Digit.mod10Digit(d1.toInt * d2.toInt))

  }
}
