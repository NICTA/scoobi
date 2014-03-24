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
package io
package avro

import java.util.UUID
import java.util.{ Map => JMap }
import org.apache.avro.Schema
import org.apache.avro.io.parsing.Symbol
import org.apache.avro.generic.{ GenericContainer, GenericData }
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Builder
import scala.collection.JavaConversions._
import core.UniqueInt
import impl.control.Exceptions._

/** Defines the Avro schema for a given Scala type. */
trait AvroSchema[A] { outer =>
  type AvroType
  def schema: Schema
  def fromAvro(x: AvroType): A
  def toAvro(x: A): AvroType

  /**
   * Map a pair of functions on this exponential functor to produce a new Avro schema.
   */
  def xmap[B](f: A => B, g: B => A): AvroSchema[B] = new AvroSchema[B] {
    type AvroType = outer.AvroType
    def schema = outer.schema
    def fromAvro(x: AvroType): B = f(outer.fromAvro(x))
    def toAvro(x: B): AvroType = outer.toAvro(g(x))
  }

  override def toString = schema.toString
  def getType = schema.getType
}

/** Provide an implicit of this, if your type is a fixed. Then a FixedSchema can kick-in */
trait AvroFixed[T] {
  def length: Int
  def toArray(t: T): Array[Byte]
  def fromArray(t: Array[Byte]): T
}

// Deprioritize expensive implicits to improve compile times.
//
// These implicits lead to more expensive implicit searches, as they need to trigger
// nested searchs for implicit arguments. If they are defined directly in `AvroSchema`,
// this cost is incurred even for "trival" searches like `implicitly[AvroSchema[Double]]`
//
// However, if the compiler sees that they are defined in proper superclass of an an
// already-found, eligible search result, it can short circuit the search altogether:
// even if one of these was found to be eligible, it would be deemed lower priority
// by the subclass rule of static overload resolution.
//
// These changes, together with corresponding changes in `WireFormat`, leads to a 4.3x
// speedup (2600ms ~> 600ms) in compiling a benchmark: 
// https://gist.github.com/retronym/fdef5a41c8e1e31124a4
//
// The goal is the reduce the number of lines like the following under `-Xlog-implicits`
//
// Hoisted.scala:25: avro.this.AvroSchema.AvroRecordSchema is not a valid implicit value for com.nicta.scoobi.io.avro.AvroSchema[String] because:
//   typing TypeApply reported errors for the implicit tree: type arguments [String] do not conform to method AvroRecordSchema's type parameter bounds [T <: org.apache.avro.generic.GenericContainer]
//
trait LowPriorityAvroSchemaImplicits {
  self: AvroSchema.type =>

  /* Map-like types AvroSchema type class instances. */
  implicit def MapSchema[CC[String, X] <: Map[String, X], T](implicit sch: AvroSchema[T], bf: CanBuildFrom[_, (String, T), CC[String, T]]) = new AvroSchema[CC[String, T]] {
    type AvroType = JMap[String, sch.AvroType]

    val schema: Schema = Schema.createMap(sch.schema)

    val b: Builder[(String, T), CC[String, T]] = bf()

    def fromAvro(xs: AvroType): CC[String, T] = {
      b.clear()
      xs.foreach { case (s, v) => b += (s.toString -> sch.fromAvro(v)) }
      b.result()
    }

    def toAvro(xs: CC[String, T]): AvroType = xs map { case (k, v) => k -> sch.toAvro(v) }
  }

  /* Traversable type AvroSchema type class instances. */
  implicit def TraversableSchema[CC[X] <: Traversable[X], T](implicit sch: AvroSchema[T], bf: CanBuildFrom[_, T, CC[T]]) = new AvroSchema[CC[T]] {
    type AvroType = GenericData.Array[sch.AvroType]

    val schema: Schema = Schema.createArray(sch.schema)


    def fromAvro(array: GenericData.Array[sch.AvroType]): CC[T] = {
      val b: Builder[T, CC[T]] = bf()
      array.iterator.foreach { x => b += sch.fromAvro(x) }
      b.result()
    }

    def toAvro(xs: CC[T]): GenericData.Array[sch.AvroType] =
      new GenericData.Array[sch.AvroType](schema, xs.map(sch.toAvro(_)).toIterable)
  }

  object FixedCounter extends UniqueInt

  implicit def FixedSchema[T](implicit fxd: AvroFixed[T]) = new AvroSchema[T]  {
    private val id = FixedCounter.get
    type AvroType = GenericData.Fixed
    val schema: Schema = Schema.createFixed("anonfixed" + id, "", " ", fxd.length)
    def fromAvro(data: GenericData.Fixed) = {
      val bytes = data.bytes()
      require(bytes.length == fxd.length)
      fxd.fromArray(bytes)
    }
    def toAvro(data: T): GenericData.Fixed = {
      val bytes = fxd.toArray(data)
      require(bytes.length == fxd.length)
      new GenericData.Fixed(schema, bytes)
    }
  }

  /**
   *  Actual Avro Generic/SpecificRecord support
   *
   *  When T is a GenericRecord, we use the NULL schema type.
   *
   *  @see AvroInput/AvroOutput how the NULL schema type is used to create the appropriate AvroKeyRecordReader/AvroKeyRecordWriter instance
   */
  implicit def AvroRecordSchema[T <: GenericContainer](implicit r: Manifest[T]) = new AvroSchema[T] {
    val sclass = r.runtimeClass.asInstanceOf[Class[T]]
    def schema: Schema = tryOrElse(sclass.newInstance().getSchema)(Schema.create(Schema.Type.NULL))

    type AvroType = T
    def fromAvro(x: T): T = x
    def toAvro(x: T): T = x
  }
}

object AvroSchema extends LowPriorityAvroSchemaImplicits {

  /* Primitive Scala type AvroSchema type class instances. */
  implicit def BooleanSchema = new AvroSchema[Boolean] {
    type AvroType = Boolean
    val schema: Schema = Schema.create(Schema.Type.BOOLEAN)
    def fromAvro(x: Boolean): Boolean = x
    def toAvro(x: Boolean): Boolean = x
  }

  implicit def IntSchema = new AvroSchema[Int] {
    type AvroType = Int
    val schema: Schema = Schema.create(Schema.Type.INT)
    def fromAvro(x: Int): Int = x
    def toAvro(x: Int): Int = x
  }

  implicit def FloatSchema = new AvroSchema[Float] {
    type AvroType = Float
    val schema: Schema = Schema.create(Schema.Type.FLOAT)
    def fromAvro(x: Float): Float = x
    def toAvro(x: Float): Float = x
  }

  implicit def LongSchema = new AvroSchema[Long] {
    type AvroType = Long
    val schema: Schema = Schema.create(Schema.Type.LONG)
    def fromAvro(x: Long): Long = x
    def toAvro(x: Long): Long = x
  }

  implicit def DoubleSchema = new AvroSchema[Double] {
    type AvroType = Double
    val schema: Schema = Schema.create(Schema.Type.DOUBLE)
    def fromAvro(x: Double): Double = x
    def toAvro(x: Double): Double = x
  }

  implicit def StringSchema = new AvroSchema[String] {
    type AvroType = String
    val schema: Schema = Schema.create(Schema.Type.STRING)
    def fromAvro(x: String): String = x
    def toAvro(x: String): String = x
  }

  // redundant, but better for performance to have this here rather than forcing
  // this common implicit serach to go through TraversableSchema.
  implicit def SeqSchema[T](implicit sch: AvroSchema[T]): AvroSchema[Seq[T]] = TraversableSchema[Seq, T]

  /* AvroSchema type class instance for Arrays. */
  implicit def ArraySchema[T](implicit mf: Manifest[T], sch: AvroSchema[T]) = new AvroSchema[Array[T]] {
    type AvroType = GenericData.Array[sch.AvroType]
    val schema: Schema = Schema.createArray(sch.schema)

    def fromAvro(array: GenericData.Array[sch.AvroType]): Array[T] =
      array.iterator.map(v => sch.fromAvro(v)).toArray

    def toAvro(xs: Array[T]): GenericData.Array[sch.AvroType] =
      new GenericData.Array[sch.AvroType](schema, xs.map(sch.toAvro(_)).toIterable)
  }

  /* Tuple types AvroSchema type class instances. */
  implicit def Tuple2Schema[T1: AvroSchema, T2: AvroSchema] = new AvroSchema[(T1, T2)] {
    type AvroType = GenericData.Record

    val sch1 = implicitly[AvroSchema[T1]]
    val sch2 = implicitly[AvroSchema[T2]]

    val schema: Schema = mkRecordSchema(List(sch1, sch2))

    def fromAvro(record: GenericData.Record): (T1, T2) = {
      val x1 = sch1.fromAvro(record.get(0).asInstanceOf[sch1.AvroType])
      val x2 = sch2.fromAvro(record.get(1).asInstanceOf[sch2.AvroType])
      (x1, x2)
    }

    def toAvro(x: (T1, T2)): GenericData.Record = {
      val record = new GenericData.Record(schema)
      record.put(0, sch1.toAvro(x._1))
      record.put(1, sch2.toAvro(x._2))
      record
    }
  }

  implicit def Tuple3Schema[T1: AvroSchema, T2: AvroSchema, T3: AvroSchema] = new AvroSchema[(T1, T2, T3)] {
    type AvroType = GenericData.Record

    val sch1 = implicitly[AvroSchema[T1]]
    val sch2 = implicitly[AvroSchema[T2]]
    val sch3 = implicitly[AvroSchema[T3]]

    val schema: Schema = mkRecordSchema(List(sch1, sch2, sch3))

    def fromAvro(record: GenericData.Record): (T1, T2, T3) = {
      val x1 = sch1.fromAvro(record.get(0).asInstanceOf[sch1.AvroType])
      val x2 = sch2.fromAvro(record.get(1).asInstanceOf[sch2.AvroType])
      val x3 = sch3.fromAvro(record.get(2).asInstanceOf[sch3.AvroType])
      (x1, x2, x3)
    }

    def toAvro(x: (T1, T2, T3)): GenericData.Record = {
      val record = new GenericData.Record(schema)
      record.put(0, sch1.toAvro(x._1))
      record.put(1, sch2.toAvro(x._2))
      record.put(2, sch3.toAvro(x._3))
      record
    }
  }

  implicit def Tuple4Schema[T1: AvroSchema, T2: AvroSchema, T3: AvroSchema, T4: AvroSchema] = new AvroSchema[(T1, T2, T3, T4)] {
    type AvroType = GenericData.Record

    val sch1 = implicitly[AvroSchema[T1]]
    val sch2 = implicitly[AvroSchema[T2]]
    val sch3 = implicitly[AvroSchema[T3]]
    val sch4 = implicitly[AvroSchema[T4]]

    val schema: Schema = mkRecordSchema(List(sch1, sch2, sch3, sch4))

    def fromAvro(record: GenericData.Record): (T1, T2, T3, T4) = {
      val x1 = sch1.fromAvro(record.get(0).asInstanceOf[sch1.AvroType])
      val x2 = sch2.fromAvro(record.get(1).asInstanceOf[sch2.AvroType])
      val x3 = sch3.fromAvro(record.get(2).asInstanceOf[sch3.AvroType])
      val x4 = sch4.fromAvro(record.get(3).asInstanceOf[sch4.AvroType])
      (x1, x2, x3, x4)
    }

    def toAvro(x: (T1, T2, T3, T4)): GenericData.Record = {
      val record = new GenericData.Record(schema)
      record.put(0, sch1.toAvro(x._1))
      record.put(1, sch2.toAvro(x._2))
      record.put(2, sch3.toAvro(x._3))
      record.put(3, sch4.toAvro(x._4))
      record
    }
  }

  implicit def Tuple5Schema[T1: AvroSchema, T2: AvroSchema, T3: AvroSchema, T4: AvroSchema, T5: AvroSchema] = new AvroSchema[(T1, T2, T3, T4, T5)] {
    type AvroType = GenericData.Record

    val sch1 = implicitly[AvroSchema[T1]]
    val sch2 = implicitly[AvroSchema[T2]]
    val sch3 = implicitly[AvroSchema[T3]]
    val sch4 = implicitly[AvroSchema[T4]]
    val sch5 = implicitly[AvroSchema[T5]]

    val schema: Schema = mkRecordSchema(List(sch1, sch2, sch3, sch4, sch5))

    def fromAvro(record: GenericData.Record): (T1, T2, T3, T4, T5) = {
      val x1 = sch1.fromAvro(record.get(0).asInstanceOf[sch1.AvroType])
      val x2 = sch2.fromAvro(record.get(1).asInstanceOf[sch2.AvroType])
      val x3 = sch3.fromAvro(record.get(2).asInstanceOf[sch3.AvroType])
      val x4 = sch4.fromAvro(record.get(3).asInstanceOf[sch4.AvroType])
      val x5 = sch5.fromAvro(record.get(4).asInstanceOf[sch5.AvroType])
      (x1, x2, x3, x4, x5)
    }

    def toAvro(x: (T1, T2, T3, T4, T5)): GenericData.Record = {
      val record = new GenericData.Record(schema)
      record.put(0, sch1.toAvro(x._1))
      record.put(1, sch2.toAvro(x._2))
      record.put(2, sch3.toAvro(x._3))
      record.put(3, sch4.toAvro(x._4))
      record.put(4, sch5.toAvro(x._5))
      record
    }
  }

  implicit def Tuple6Schema[T1: AvroSchema, T2: AvroSchema, T3: AvroSchema, T4: AvroSchema, T5: AvroSchema, T6: AvroSchema] = new AvroSchema[(T1, T2, T3, T4, T5, T6)] {
    type AvroType = GenericData.Record

    val sch1 = implicitly[AvroSchema[T1]]
    val sch2 = implicitly[AvroSchema[T2]]
    val sch3 = implicitly[AvroSchema[T3]]
    val sch4 = implicitly[AvroSchema[T4]]
    val sch5 = implicitly[AvroSchema[T5]]
    val sch6 = implicitly[AvroSchema[T6]]

    val schema: Schema = mkRecordSchema(List(sch1, sch2, sch3, sch4, sch5, sch6))

    def fromAvro(record: GenericData.Record): (T1, T2, T3, T4, T5, T6) = {
      val x1 = sch1.fromAvro(record.get(0).asInstanceOf[sch1.AvroType])
      val x2 = sch2.fromAvro(record.get(1).asInstanceOf[sch2.AvroType])
      val x3 = sch3.fromAvro(record.get(2).asInstanceOf[sch3.AvroType])
      val x4 = sch4.fromAvro(record.get(3).asInstanceOf[sch4.AvroType])
      val x5 = sch5.fromAvro(record.get(4).asInstanceOf[sch5.AvroType])
      val x6 = sch6.fromAvro(record.get(5).asInstanceOf[sch6.AvroType])
      (x1, x2, x3, x4, x5, x6)
    }

    def toAvro(x: (T1, T2, T3, T4, T5, T6)): GenericData.Record = {
      val record = new GenericData.Record(schema)
      record.put(0, sch1.toAvro(x._1))
      record.put(1, sch2.toAvro(x._2))
      record.put(2, sch3.toAvro(x._3))
      record.put(3, sch4.toAvro(x._4))
      record.put(4, sch5.toAvro(x._5))
      record.put(5, sch6.toAvro(x._6))
      record
    }
  }

  implicit def Tuple7Schema[T1: AvroSchema, T2: AvroSchema, T3: AvroSchema, T4: AvroSchema, T5: AvroSchema, T6: AvroSchema, T7: AvroSchema] = new AvroSchema[(T1, T2, T3, T4, T5, T6, T7)] {
    type AvroType = GenericData.Record

    val sch1 = implicitly[AvroSchema[T1]]
    val sch2 = implicitly[AvroSchema[T2]]
    val sch3 = implicitly[AvroSchema[T3]]
    val sch4 = implicitly[AvroSchema[T4]]
    val sch5 = implicitly[AvroSchema[T5]]
    val sch6 = implicitly[AvroSchema[T6]]
    val sch7 = implicitly[AvroSchema[T7]]

    val schema: Schema = mkRecordSchema(List(sch1, sch2, sch3, sch4, sch5, sch6, sch7))

    def fromAvro(record: GenericData.Record): (T1, T2, T3, T4, T5, T6, T7) = {
      val x1 = sch1.fromAvro(record.get(0).asInstanceOf[sch1.AvroType])
      val x2 = sch2.fromAvro(record.get(1).asInstanceOf[sch2.AvroType])
      val x3 = sch3.fromAvro(record.get(2).asInstanceOf[sch3.AvroType])
      val x4 = sch4.fromAvro(record.get(3).asInstanceOf[sch4.AvroType])
      val x5 = sch5.fromAvro(record.get(4).asInstanceOf[sch5.AvroType])
      val x6 = sch6.fromAvro(record.get(5).asInstanceOf[sch6.AvroType])
      val x7 = sch7.fromAvro(record.get(6).asInstanceOf[sch7.AvroType])
      (x1, x2, x3, x4, x5, x6, x7)
    }

    def toAvro(x: (T1, T2, T3, T4, T5, T6, T7)): GenericData.Record = {
      val record = new GenericData.Record(schema)
      record.put(0, sch1.toAvro(x._1))
      record.put(1, sch2.toAvro(x._2))
      record.put(2, sch3.toAvro(x._3))
      record.put(3, sch4.toAvro(x._4))
      record.put(4, sch5.toAvro(x._5))
      record.put(5, sch6.toAvro(x._6))
      record.put(6, sch7.toAvro(x._7))
      record
    }
  }

  implicit def Tuple8Schema[T1: AvroSchema, T2: AvroSchema, T3: AvroSchema, T4: AvroSchema, T5: AvroSchema, T6: AvroSchema, T7: AvroSchema, T8: AvroSchema] = new AvroSchema[(T1, T2, T3, T4, T5, T6, T7, T8)] {
    type AvroType = GenericData.Record

    val sch1 = implicitly[AvroSchema[T1]]
    val sch2 = implicitly[AvroSchema[T2]]
    val sch3 = implicitly[AvroSchema[T3]]
    val sch4 = implicitly[AvroSchema[T4]]
    val sch5 = implicitly[AvroSchema[T5]]
    val sch6 = implicitly[AvroSchema[T6]]
    val sch7 = implicitly[AvroSchema[T7]]
    val sch8 = implicitly[AvroSchema[T8]]

    val schema: Schema = mkRecordSchema(List(sch1, sch2, sch3, sch4, sch5, sch6, sch7, sch8))

    def fromAvro(record: GenericData.Record): (T1, T2, T3, T4, T5, T6, T7, T8) = {
      val x1 = sch1.fromAvro(record.get(0).asInstanceOf[sch1.AvroType])
      val x2 = sch2.fromAvro(record.get(1).asInstanceOf[sch2.AvroType])
      val x3 = sch3.fromAvro(record.get(2).asInstanceOf[sch3.AvroType])
      val x4 = sch4.fromAvro(record.get(3).asInstanceOf[sch4.AvroType])
      val x5 = sch5.fromAvro(record.get(4).asInstanceOf[sch5.AvroType])
      val x6 = sch6.fromAvro(record.get(5).asInstanceOf[sch6.AvroType])
      val x7 = sch7.fromAvro(record.get(6).asInstanceOf[sch7.AvroType])
      val x8 = sch8.fromAvro(record.get(7).asInstanceOf[sch8.AvroType])
      (x1, x2, x3, x4, x5, x6, x7, x8)
    }

    def toAvro(x: (T1, T2, T3, T4, T5, T6, T7, T8)): GenericData.Record = {
      val record = new GenericData.Record(schema)
      record.put(0, sch1.toAvro(x._1))
      record.put(1, sch2.toAvro(x._2))
      record.put(2, sch3.toAvro(x._3))
      record.put(3, sch4.toAvro(x._4))
      record.put(4, sch5.toAvro(x._5))
      record.put(5, sch6.toAvro(x._6))
      record.put(6, sch7.toAvro(x._7))
      record.put(7, sch8.toAvro(x._8))
      record
    }
  }

  /* Helper methods. */
  private[scoobi] def mkRecordSchema(ss: Seq[AvroSchema[_]]): Schema = {
    val fields: List[Schema.Field] =
      ss.toList.zipWithIndex map { case (s, ix) => new Schema.Field("v" + ix, s.schema, "", null) }
    val record =
      Schema.createRecord("tup" + UUID.randomUUID().toString.replace('-', 'x'), "", "scoobi", false)
    record.setFields(fields)
    record
  }
}

trait AvroParsingImplicits {
  implicit class EnhancedSymbol(symbol: Symbol) {

    /**
     * Pull out any ErrorAction Symbols.
     *
     * Note: Pulling errors out like this is fine because scoobi currently doesn't produce any
     *       union types in the reader schema. If it did, its possible to have ErrorAction symbols
     *       in union branches that may never be followed because the data isn't in that format.
     */
    def getErrors: List[Symbol.ErrorAction] = {
      symbol match {
        case errSym: Symbol.ErrorAction => List(errSym)
        case otherSym: Symbol => Option(symbol.production).map {
          _.filterNot(_ == symbol).toList.flatMap(_.getErrors)
        }.getOrElse(Nil)
      }
    }
  }
}
object AvroParsingImplicits extends AvroParsingImplicits
