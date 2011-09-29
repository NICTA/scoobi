/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi


/** Test out the MSCR machinery. */
object MscrTest {

  import AST._

  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  //  Execution plan - already converted to IR.
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  val l1 = Load("in/L1")
  val l2 = Load("in/L2")
  val l3 = Load("in/L3")
  val l4 = Load("in/L4")

  val m1 = GbkMapper(l2, (s: String) => List((s, 1)))
  val m2 = GbkMapper(l3, (s: String) => List((s, 2)))
  val m3 = Mapper(l3, (s: String) => List((3L, s)))

  val f1 = Flatten(List(m1, m2))
  val g1 = GroupByKey(f1)
  val c1 = Combiner(g1, (a: Int, b: Int) => a + b)

  // ~~~~~~~~~~~~

  val m4 = GbkMapper(l1, (s: String) => List((4, s)))
  val m5 = GbkMapper(c1, (t: (String, Int)) => List(t.swap))
  val m6 = GbkMapper(l1, (s: String) => List((6.6, (6, s))))

  val g2 = GroupByKey(m6)
  val c2 = Combiner(g2, (ta: (Int, String), tb: (Int, String)) => (ta._1 + tb._1, ta._2 + tb._2))
  val f2 = Flatten(List(m4, m5))
  val g3 = GroupByKey(f2)
  val c3 = Combiner(g3, (s1: String, s2: String) => s1 ++ s2)

  // ~~~~~~~~~~~~

  val m7 = GbkMapper(c3, (t: (Int, String)) => List((t._2, 7.7)))
  val m8 = GbkMapper(l4, (s: String) => List((8L, s)))
  val m9 = GbkMapper(c3, (t: (Int, String)) => List((9L, t._2)))

  val g4 = GroupByKey(m7)
  val r1 = GbkReducer(g4, (t: (String, Iterable[Double])) => List(('a', t._2.sum)))
  val f3 = Flatten(List(m3, m8, m9))
  val g5 = GroupByKey(f3)
  val c4 = Combiner(g5, (a: String, b: String) => a + b)
  val r2 = Reducer(c4, (t: (Long, String)) => List((t._2, t._1, t._2)))


  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  //  Connectors - TODO: need to be determined programatically.
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  val din1 = InputStore(l1)
  val din2 = InputStore(l2)
  val din3 = InputStore(l3)
  val din4 = InputStore(l4)
  val dins = Set(din1, din2, din3, din4)

  // TODO - derrive these from 'plan'
  val dout1 = OutputStore(m3, "out/M3")
  val dout2 = OutputStore(r2, "out/R2")
  val dout3 = OutputStore(r1, "out/R1")
  val dout4 = OutputStore(c2, "out/C2")
  val douts = Set(dout1, dout2, dout3, dout4)

  val dint1 = BridgeStore(c1, 1)
  val dint2 = BridgeStore(c3, 1)
  val dint3 = BridgeStore(m3, 1)
  val dints = Set(dint1, dint2, dint3)


  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  //  MSCRs.
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  val ic1A = MapperInputChannel(din2, Set(m1))
  val ic2A = MapperInputChannel(din3, Set(m2, m3))

  val oc1A = GbkOutputChannel(Set(dint1), Some(f1), g1, JustCombiner(c1))
  val oc2A = BypassOutputChannel(Set(dout1, dint3), m3)

  val mscrA = MSCR(Set(ic1A, ic2A), Set(oc1A, oc2A))

  // ~~~~~~~~~~~~~~~~~~~~~~

  val ic1B = MapperInputChannel(din1, Set(m4, m6))
  val ic2B = MapperInputChannel(dint1, Set(m5))

  val oc1B = GbkOutputChannel(Set(dout4), None,     g2, JustCombiner(c2))
  val oc2B = GbkOutputChannel(Set(dint2), Some(f2), g3, JustCombiner(c3))

  val mscrB = MSCR(Set(ic1B, ic2B), Set(oc1B, oc2B))

  // ~~~~~~~~~~~~~~~~~~~~~~

  val ic1C = MapperInputChannel(dint2, Set(m7, m9))
  val ic2C = MapperInputChannel(din4, Set(m8))
  val ic3C = BypassInputChannel(dint3, m3)

  val oc1C = GbkOutputChannel(Set(dout3), None,     g4, JustReducer(r1))
  val oc2C = GbkOutputChannel(Set(dout2), Some(f3), g5, CombinerReducer(c4, r2))

  val mscrC = MSCR(Set(ic1C, ic2C, ic3C), Set(oc1C, oc2C))

  // ~~~~~~~~~~~~~~~~~~~~~~

  val mscrs = Set(mscrA, mscrB, mscrC)


  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  //  DO IT
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def main(args: Array[String]) {
    Scoobi.setJarByClass(this.getClass)
    Executor.executePlan(mscrs, dins, dints, douts)
  }
}
