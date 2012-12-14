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
package guide

import application.ScoobiApp

class DataTypes extends ScoobiPage { def is = "Data Types".title^
  """
### Standard types

We've seen in many of the examples that it's possible for `DList` objects to be parameterised by normal Scala primitive (*value*) types. Not surprisingly, Scoobi supports `DList` objects that are parameterised by any of the Scala primitive types:

    val x: DList[Byte] = ...
    val x: DList[Char] = ...
    val x: DList[Int] = ...
    val x: DList[Long] = ...
    val x: DList[Double] = ...

And as we've also see, although not a primitive, Scoobi supports `DList`s of `String`s:

    val x: DList[String] = ...

Some of the examples also use `DList` objects that are parameterised by a pair (Scala [`Tuple2`](http://www.scala-lang.org/api/current/scala/Tuple2.html) type). In fact, Scoobi supports `DList` objects that are parameterised by Scala tuples up to arity 8, and in addition, supports arbitrary nesting:

    val x: DList[(Int, String)] = ...
    val x: DList[(String, Long)] = ...
    val x: DList[(Int, String, Long)] = ...
    val x: DList[(Int, (String, String), Int, (Long, Long, Long))] = ...
    val x: DList[(Int, (String, (Long, Long)), Char)] = ...

Finally, Scoobi also supports `DList` objects that are parameterised by the Scala [`Option`](http://www.scala-lang.org/api/rc/scala/Option.html)
and [`Either`](http://www.scala-lang.org/api/current/scala/Either.html) types, which can also be combined with any of the `Tuple` and primitive types:

    val x: Option[Int] = ...
    val x: Option[String] = ...
    val x: Option[(Long, String)] = ...

    val x: Either[Int, String] = ...
    val x: Either[String, (Long, Long)] = ...
    val x: Either[Long, Either[String, Int]] = ...
    val x: Either[Int, Option[Long]] = ...

Notice that in all these cases, the `DList` object is parameterised by a *standard* Scala type and not some wrapper type. This is really convenient. It means, for example, that the use of a higher-order function like `map` can directly call any of the methods associated with those types. In contrast, programming MapReduce jobs directly using Hadoop's API requires that all types implement the [`Writable`](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/io/Writable.html) interface, resulting in the use of wrapper types such as `IntWritable` rather than just `int`. Of course the reason for this is that `Writable` specifies methods for serialization and deserialization of data within the Hadoop framework. However, given that `DList` objects eventually result in code that is executed by the Hadoop framework, how is serialization and deserialization specified?

### Custom types

#### WireFormat

Scoobi requires that the type parameterizing a `DList` object has an implementation of the [`WireFormat`](${SCOOBI_API_PAGE}#com.nicta.scoobi.WireFormat) type class (Scala [context bound](http://stackoverflow.com/questions/2982276/what-is-a-context-bound-in-scala)). Thus, the `DList` class is actually specified as:

    class DList[A : WireFormat] { ... }

If the compiler cannot find a `WireFormat` implementation for the type parameterizing a specific `DList` object, that code will not compile.  Implementations of `WireFormat` specify serialization and deserialization in their `toWire` and `fromWire` methods, which end up finding their way into `Writable`'s `write` and `readFields` methods.

To make life easy, the [`WireFormat`](${SCOOBI_API_PAGE}#com.nicta.scoobi.WireFormat$) object includes `WireFormat` implementations for the types listed above (that is why they work out of the box). However, the real advantage of using type classes is they allow you to extend the set of types that can be used with `DList` objects and that set can include types that already exist, maybe even in some other compilation unit.  So long as a type has a `WireFormat` implementation, it can parameterise a `DList`. This is extremely useful because while, say, you can represent a lot with nested tuples, much can be gained in terms of type safety, readability and maintenance by using custom types. For example, say we were building an application to analyze stock ticker-data. In that situation it would be nice to work with `DList[Tick]` objects. We can do that if we write a `WireFormat` implementation for `Tick`:

    case class Tick(val date: Int, val symbol: String, val price: Double)

    implicit def TickFmt = new WireFormat[Tick] {
      def toWire(tick: Tick, out: DataOutput) = {
        out.writeInt(tick.date)
        out.writeUTF(tick.symbol)
        out.writeDouble(tick.price)
      }
      def fromWire(in: DataInput): Tick = {
        val date = in.readInt
        val symbol = in.readUTF
        val price = in.readDouble
        Tick(date, symbol, price)
      }
      def show(tick: Tick): String = tick.toString
    }

    val ticks: DList[Tick] = ...  /* OK */

Then we can actually make use of the `Tick` type:

    /* Function to compute Hi and Low for a stock for a given day */
    def hilo(ts: Iterable[Tick]): (Double, Double) = {
      val start = ts.head.price
      ts.tail.foldLeft((start, start)) { case ((high, low), tick) => (max(high, tick.price), min(low, tick.price)) }
    }

    /* Group tick data by date and symbol */
    val ticks: DList[Tick] = ...
    val ticksGrouped = ticks.groupBy(t => (t.symbol, t.date))

    /* Compute highs and lows for each stock for each day */
    val highLow = ticksGrouped map { case ((symbol, date), ticks) => (symbol, date, hilo(ticks)) }

Notice that by using the custom type `Tick` it's obvious what fields we are using. If instead the type of `ticks` was `DList[(Int, String, Double)]`, the code would be far less readable, and maintenance would be more difficult if, for example, we added new fields to `Tick` or modified the order of existing fields.

#### For case classes

Being able to have `DList` objects of custom types is a huge productivity boost. However, there is still the boiler-plate, mechanical work associated with the `WireFormat` implementation. To overcome this, the `WireFormat` object also provides a utility function called `mkCaseWireFormat` that automatically constructs a `WireFormat` for **case classes**:

    case class Tick(date: Int, symbol: String, price: Double)
    implicit val tickFmt = mkCaseWireFormat(Tick, Tick.unapply _)

    val ticks: DList[Tick] = ...  /* Still OK */

`mkCaseWireFormat` takes as arguments the case class's automatically generated `apply` and `unapply` methods. The only requirement on case classes when using `mkCaseWireFormat` is that all its fields have `WireFormat` implementations. If not, your `DList` objects won't type check. The upside to this is that all of the types above that do have `WireFormat` implementations can be fields in a case class when used in conjunction with `mkCaseWireFormat`:

    case class Tick(date: Int, symbol: String, price: Double, high_low: (Double, Double))
    implicit val tickFmt = mkCaseWireFormat(Tick, Tick.unapply _)

    val ticks: DList[Tick] = ...  /* Amazingly, still OK */

Of course, this will also extend to other case classes as long as they have `WireFormat` implementations. Thus, it's possible to have nested case classes that can parameterise `DList` objects:

    case class PriceAttr(price: Double, high_low: (Double, Double))
    implicit val priceAttrFmt = mkCaseWireFormat(PriceAttr, PriceAttr.unapply _)

    case class Tick(date: Int, symbol: String, attr: PriceAttr)
    implicit val tickFmt = mkCaseWireFormat(Tick, Tick.unapply _)

    val ticks: DList[Tick] = ...  /* That's right, amazingly, still OK */

In summary, the way data types work in Scoobi is definitely one of its killer features, basically because they don't get in the way!


#### Default WireFormat

Temporarily, during your development, you can import a default WireFormat instance which should work with most Java types:

     import com.nicta.scoobi.core.WireFormat.AnythingFmt

     class Timestamp(val date: Date)

     implicit val timestampFormat = AnythingFmt[Timestamp]

     val timestamps: DList[Timestamps] = ...  /* Compiles OK */

The `AnythingFmt` is a `WireFormat` using Java serialization to serialise/deserialise the objects. It is however very ineffecient so it is not provided as an implicit conversion, you need to explicitely import it to be able to use it.

  """
}
