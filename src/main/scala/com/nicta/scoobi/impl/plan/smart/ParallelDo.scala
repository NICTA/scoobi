package com.nicta.scoobi
package impl
package plan
package smart

import core.{Emitter, EnvDoFn}

/** The ParallelDo node type specifies the building of a DComp as a result of applying a function to
 * all elements of an existing DComp and concatenating the results. */
case class ParallelDo[A, B, E](in: DComp[A, Arr], env: DComp[E, Exp], dofn: EnvDoFn[A, B, E], groupBarrier: Boolean = false, fuseBarrier: Boolean = false) extends DComp[B, Arr] {

  override val toString = "ParallelDo" + id + (if (groupBarrier) "*" else "") + (if (fuseBarrier) "%" else "")
  val toVerboseString = toString + "(" + env.toVerboseString + "," + in.toVerboseString + ")"

  def fuse[Z, G](p2: ParallelDo[B, Z, G]) =
    ParallelDo.fuse(this, p2)
}

object ParallelDo {
  def fuse[X, Y, Z, F, G](pd1: ParallelDo[X, Y, F], pd2: ParallelDo[Y, Z, G]): ParallelDo[X, Z, (F, G)] = {

    val ParallelDo(in1, env1, dofn1, gb1, _)   = pd1
    val ParallelDo(in2, env2, dofn2, gb2, fb2) = pd2

    new ParallelDo(in1, fuseEnv(env1, env2), fuseDoFn(dofn1, dofn2), gb1 || gb2, fb2)
  }

  /** Create a new ParallelDo function that is the fusion of two connected ParallelDo functions. */
  def fuseDoFn[X, Y, Z, F, G](f: EnvDoFn[X, Y, F], g: EnvDoFn[Y, Z, G]): EnvDoFn[X, Z, (F, G)] = new EnvDoFn[X, Z, (F, G)] {
    def setup(env: (F, G)) {
      f.setup(env._1); g.setup(env._2)
    }

    def process(env: (F, G), input: X, emitter: Emitter[Z]) {
      f.process(env._1, input, new Emitter[Y] { def emit(value: Y) { g.process(env._2, value, emitter) } } )
    }

    def cleanup(env: (F, G), emitter: Emitter[Z]) {
      f.cleanup(env._1, new Emitter[Y] { def emit(value: Y) { g.process(env._2, value, emitter) } })
      g.cleanup(env._2, emitter)
    }
  }

  /** Create a new environment by forming a tuple from two separate evironments.*/
  def fuseEnv[F, G](fExp: DComp[F, Exp], gExp: DComp[G, Exp]): DComp[(F, G), Exp] =
    Op(fExp, gExp, (f: F, g: G) => (f, g))

}
