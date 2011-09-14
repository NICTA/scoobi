/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi


/** A list that is distributed accross multiple machines. */
class DList[A : Manifest : HadoopWritable](private val ast: AST.DList[A]) {

  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Primitive functionality.
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /** */
  def flatMap[B]
      (f: A => Iterable[B])
      (implicit mB:  Manifest[B],
                wtB: HadoopWritable[B]): DList[B] = new DList(AST.FlatMap(ast, f))

  /** */
  def concat(in: DList[A]): DList[A] = new DList(AST.Flatten(List(ast) ::: List(in.ast)))

  /** */
  def groupByKey[K, V]
      (implicit ev:   AST.DList[A] <:< AST.DList[(K, V)],
                mK:   Manifest[K],
                wtK:  HadoopWritable[K],
                ordK: Ordering[K],
                mV:   Manifest[V],
                wtV:  HadoopWritable[V]): DList[(K, Iterable[V])] = new DList(AST.GroupByKey(ast))

  /** */
  def combine[K, V]
      (f: (V, V) => V)
      (implicit ev:   AST.DList[A] <:< AST.DList[(K,Iterable[V])],
                mK:   Manifest[K],
                wtK:  HadoopWritable[K],
                mV:   Manifest[V],
                wtV:  HadoopWritable[V]): DList[(K, V)] = new DList(AST.Combine(ast, f))


  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Derrived functionality.
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /** */
  def map[B]
      (f: A => B)
      (implicit mB:  Manifest[B],
                wtB: HadoopWritable[B]): DList[B] = flatMap(x => List(f(x)))

  /** */
  def filter(f: A => Boolean): DList[A] = flatMap(x => if (f(x)) List(x) else Nil)

  /** */
  def reduceByKey[K, V]
      (f: (V, V) => V)
      (implicit ev:   AST.DList[A] <:< AST.DList[(K, V)],
                mK:   Manifest[K],
                wtK:  HadoopWritable[K],
                ordK: Ordering[K],
                mV:   Manifest[V],
                wtV:  HadoopWritable[V]): DList[(K, V)] = groupByKey.combine(f)
}


object DList {

  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Input
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /** Create a distributed list from a file. */
  def fromTextFile(path: String): DList[String] = new DList(AST.Load(path, identity))

  /** Create a distributed list from a text file that is a number of fields deliminated
    * by some separator. Use an extractor function to pull out the required fields to
    * create the distributed list. */
  def extractFromDelimitedTextFile[A]
      (sep: String, path: String)
      (extractFn: PartialFunction[List[String], A])
      (implicit mA:  Manifest[A],
                wtA: HadoopWritable[A]): DList[A] = {

    val lines = fromTextFile(path)
    lines.flatMap { line =>
      val fields = line.split(sep).toList
      if (extractFn.isDefinedAt(fields))
        List(extractFn(fields))
      else
        Nil
    }
  }

  /** Create a distributed list from a text file that is a number of fields deliminated
    * by some separator. The type of the resultant list is determined by type inference.
    * An implicit schema must be in scope for the requried resultant type. */
  def fromDelimitedTextFile[A]
      (sep: String, path: String)
      (implicit schema: Schema[A],
                mA:     Manifest[A],
                wtA:    HadoopWritable[A]): DList[A] = {

    val lines = fromTextFile(path)
    lines.map { line =>
      val fields = line.split(sep).toList
      val (data, _) = schema.read(fields)
      data
    }
  }


  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Output
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /** A class that specifies how to make a distributed list persisitent. */
  class PersistList[A](val dl: DList[A], val persistFn: Unit => Unit)

  /** Specify a distibuted list to be persisitent by storing it to disk as a
    * text file. */
  def toTextFile[A](dl: DList[A], path: String)(implicit io: LineOutput[A]): PersistList[A] = {
    import java.io._

    def writeFile() = {
      val file = new PrintWriter(path)
      try {
        AST.eval(dl.ast).map { io.toLine } foreach { file.println }
      }
      finally { file.close() }
    }

    new PersistList[A](dl, (_: Unit) => writeFile())
  }

  /** Persist one or more distributed lists. */
  def persist(outputs: PersistList[_]*) = outputs foreach { _.persistFn() }


  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /** A type class for parsing string fields into specific types. */
  @annotation.implicitNotFound(msg = "No implicit Schema defined for ${A}.")
  trait Schema[A] {
    /* Grab fields, in sequence, required to make 'A', and return the remaining fields. */
    def read(fields: List[String]): (A, List[String])
  }

  /** Type class instances. */
  object Schema {
    implicit object SchemaForUnit extends Schema[Unit] {
      def read(fields: List[String]) = ((), fields)
    }

    implicit object SchemaForBoolean extends Schema[Boolean] {
      def read(fields: List[String]) = (fields(0).toBoolean, fields.drop(1))
    }

    implicit object SchemaForByte extends Schema[Byte] {
      def read(fields: List[String]) = (fields(0).toByte, fields.drop(1))
    }

    implicit object SchemaForChar extends Schema[Char] {
      def read(fields: List[String]) = (fields(0).toArray.apply(0), fields.drop(1))
    }

    implicit object SchemaForDouble extends Schema[Double] {
      def read(fields: List[String]) = (fields(0).toDouble, fields.drop(1))
    }

    implicit object SchemaForFloat extends Schema[Float] {
      def read(fields: List[String]) = (fields(0).toFloat, fields.drop(1))
    }

    implicit object SchemaForLong extends Schema[Long] {
      def read(fields: List[String]) = (fields(0).toLong, fields.drop(1))
    }

    implicit object SchemaForShort extends Schema[Short] {
      def read(fields: List[String]) = (fields(0).toShort, fields.drop(1))
    }

    implicit object SchemaForInt extends Schema[Int] {
      def read(fields: List[String]) = (fields(0).toInt, fields.drop(1))
    }

    implicit object SchemaForString extends Schema[String] {
      def read(fields: List[String]) = (fields(0), fields.drop(1))
    }

    implicit def SchemaForTuple2[T1, T2]
        (implicit sch1: Schema[T1],
                  sch2: Schema[T2]) = new Schema[Tuple2[T1, T2]] {

      def read(fields: List[String]) = {
        val (e1, rf1) = sch1.read(fields)
        val (e2, rf2) = sch2.read(rf1)
        ((e1, e2), rf2)
      }
    }

    implicit def SchemaForTuple3[T1, T2, T3]
        (implicit sch1: Schema[T1],
                  sch2: Schema[T2],
                  sch3: Schema[T3]) = new Schema[Tuple3[T1, T2, T3]] {

      def read(fields: List[String]) = {
        val (e1, rf1) = sch1.read(fields)
        val (e2, rf2) = sch2.read(rf1)
        val (e3, rf3) = sch3.read(rf2)
        ((e1, e2, e3), rf3)
      }
    }

    implicit def SchemaForTuple4[T1, T2, T3, T4]
        (implicit sch1: Schema[T1],
                  sch2: Schema[T2],
                  sch3: Schema[T3],
                  sch4: Schema[T4]) = new Schema[Tuple4[T1, T2, T3, T4]] {

      def read(fields: List[String]) = {
        val (e1, rf1) = sch1.read(fields)
        val (e2, rf2) = sch2.read(rf1)
        val (e3, rf3) = sch3.read(rf2)
        val (e4, rf4) = sch4.read(rf3)
        ((e1, e2, e3, e4), rf4)
      }
    }

    implicit def SchemaForTuple5[T1, T2, T3, T4, T5]
        (implicit sch1: Schema[T1],
                  sch2: Schema[T2],
                  sch3: Schema[T3],
                  sch4: Schema[T4],
                  sch5: Schema[T5]) = new Schema[Tuple5[T1, T2, T3, T4, T5]] {

      def read(fields: List[String]) = {
        val (e1, rf1) = sch1.read(fields)
        val (e2, rf2) = sch2.read(rf1)
        val (e3, rf3) = sch3.read(rf2)
        val (e4, rf4) = sch4.read(rf3)
        val (e5, rf5) = sch5.read(rf4)
        ((e1, e2, e3, e4, e5), rf5)
      }
    }
  }

  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /** Type class for writing to file.
    * TODO - should really just be using 'toString' or 'show'. */
  @annotation.implicitNotFound(msg = "No implicit LineOutput defined for ${A}.")
  trait LineOutput[A] {
    def toLine(v: A): String
  }

  /** Type class instances. */
  object LineOutput {
    implicit object LineOutputForInt extends LineOutput[Int] {
      def toLine(v: Int) = v.toString
    }

    implicit object LineOutputForDouble extends LineOutput[Double] {
      def toLine(v: Double) = v.toString
    }

    implicit object LineOutputForString extends LineOutput[String] {
      def toLine(v: String) = v
    }

    implicit def LineOutputForTuple2[A, B]
        (implicit ioA: LineOutput[A],
                  ioB: LineOutput[B]) = new LineOutput[Tuple2[A, B]] {
      def toLine(v: Tuple2[A, B]) = "(" + ioA.toLine(v._1) + ", " + ioB.toLine(v._2) + ")"
    }

    implicit def LineOutputForTuple4[A, B, C, D]
        (implicit ioA: LineOutput[A],
                  ioB: LineOutput[B],
                  ioC: LineOutput[C],
                  ioD: LineOutput[D]) = new LineOutput[Tuple4[A, B, C, D]] {
      def toLine(v: Tuple4[A, B, C, D]) = "(" + ioA.toLine(v._1) + ", " +
                                                ioB.toLine(v._2) + ", " +
                                                ioC.toLine(v._3) + ", " +
                                                ioD.toLine(v._4) + ")"
    }

    implicit def LineOutputForIterable[T](implicit io: LineOutput[T]) = new LineOutput[Iterable[T]] {

      def mkString(xs: Iterable[T]) = {
        var b = new StringBuilder
        var first = true

        for (x <- xs) {
          if (first) {
            b append io.toLine(x)
            first = false
          }
          else {
            b append ","
            b append io.toLine(x)
          }
        }
        b.toString
      }

      def toLine(xs: Iterable[T]) = "[" + mkString(xs) + "]"
    }

  }
}
