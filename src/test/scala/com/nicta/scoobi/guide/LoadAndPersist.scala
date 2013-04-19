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

import Scoobi._
import io.text._
import org.apache.hadoop.io.{Writable, Text, IntWritable}
import org.apache.avro.Schema
import util.Random

class LoadAndPersist extends ScoobiPage { def is = "Load and persist data".title^ s2"""

`DList` objects are merely nodes in a graph describing a series of data computation we want to perform. However, at some point we need to specify what the inputs and outputs to that computation are. In the [WordCount example](Application.html) we simply use in memory data and we print out the result of the computations. However the data used by Hadoop jobs is generally *loaded* from files and the results *persisted* to files. Let's see how to specify this.

### Loading

#### DLists

Most of the time when we create `DList` objects, it is the result of calling a method on another `DList` object (e.g. `map`). *Loading*, on the other hand, is the only way to create a `DList` object that is not based on any others. It is the means by which we associate a `DList` object with some data files on HDFS. Scoobi provides functions to create `DList` objects associated with text files on HDFS, which are implemented in the object [`${fullName[TextInput]}`]($API_PAGE#com.nicta.scoobi.io.text.TextInput$$).

#### Text files

There are a number of ways in which to construct a `DList` object from a text file. The simplest is `${termName(fromTextFile(""))}`. It takes one or more paths (globs are supported) to text files on HDFS (or whichever file system Hadoop has been configured for) and returns a `DList[String]` object, where each element of the distributed list refers to one of the lines of text from the files: ${snippet{

// load a single text file
val lines1: DList[String] = fromTextFile("hdfs://path/to/file")

// load multiple text files
val lines2: DList[String] = fromTextFile("hdfs://path/to/file1", "hdfs://path/to/file2")

// load from a list of text files
val lines3: DList[String] = fromTextFile(Seq("hdfs://path/to/file1", "hdfs://path/to/file2"):_*)
}}

Whilst some problems involve working with entire lines of text, often it's the case that we are interested in loading delimited text files, for example, comma separated value (CSV) or tab separated value (TSV) files and want to extract values from *fields*.  In this case, we could use `${termName(fromTextFile(""))}` followed by a `map` that pulls out fields of interest: ${snippet{

// load CSV with schema "id,first_name,second_name,age"
val lines: DList[String] = fromTextFile("hdfs://path/to/CVS/files/*")

// pull out id and second_name
val names: DList[(Int, String)] = lines map { line =>
  val fields = line.split(",")
  (fields(0).toInt, fields(2))
}
}}

Given that these types of field extractions from delimited text files are such a common task, Scoobi provides a more convenient mechanism for achieving this:

    // load CSV and pull out id and second_name
    val names: DList[(Int, String)] = fromDelimitedTextFile("hdfs://path/to/CVS/files/*", ",") {
      case AnInt(id) :: first_name :: second_name :: age :: _ => (id, second_name)
    }

As this example illustrates, the call to `fromDelimitedTextFile` takes a number of arguments. The first argument specifies the path and the second is the delimiter, in this case a comma. Following is a second *parameter list* that is used to specify how to extract fields once they are separated out. This is specified by supplying a *partial function* that takes a list of separated `String` fields as its input and returns a value whose type will set the type of the resulting `DList` - i.e. a `PartialFunction[List[String], A]` will create a `DList[A]` (where `A` is `(Int, String)` above). In this example, we use Scala's [pattern matching](http://www.scala-lang.org/node/120) feature to *pull out* the four fields and return the first and third.

In addition Scoobi also provides a number of [extractors](http://www.scala-lang.org/node/112) for automatically checking and converting of fields to an expected type. In the above example, the `AnInt` extractor is used to specify that the `id` field must be an integer in order for the `case` statement to match. In the case of a match, it also has the effect of typing `id` as an `Int`. Field extractors are provided for `Int`, `Long`, `Double` and `Float` (called `AnInt`, `ALong`, `ADouble`, `AFloat`).

One of the advantages of using `fromDelimitedTextFile` is that we have at our disposal all of the Scala pattern matching features, and because we are providing a partial function, any fields that don't match against the supplied pattern will not be present in the returned `DList`. This allows us to implement simple filtering inline with the extraction:

    // load CSV and pull out id and second_name if first_name is "Harry"
    val names = fromDelimitedTextFile("hdfs://path/to/CSV/files/*", ",") {
      case AnyInt(id) :: "Harry" :: second_name :: age :: _ => (id, second_name)
    }

We can of course supply multiple patterns:

    // load CSV and pull out id and second_name if first_name is "Harry" or "Lucy"
    val names: DList[(Int, String)] = fromDelimitedTextFile("hdfs://path/to/CSV/files/*", ",") {
      case AnInt(id) :: "Harry" :: second_name :: age :: _ => (id, second_name)
      case AnInt(id) :: "Lucy"  :: second_name :: age :: _ => (id, second_name)
    }

And, a more interesting example is when the value of one field influences the semantics of another. For example:

    val thisYear: Int = 2013

    // load CSV with schema "event,year,year_designation" and pull out event and how many years ago it occurred
    val yearsAgo: DList[(String, Int)] = fromDelimitedTextFile("hdfs://path/to/CSV/files/*", ",") {
      case event :: AnInt(year) :: "BC" :: _ => (event, thisYear + year - 1) // No 0 AD
      case event :: AnInt(year) :: "AD" :: _ => (event, thisYear - year)
    }

#### Sequence files

Sequence files are the built-in binary file format used in Hadoop. Scoobi provides a number of ways to load existing Sequence files as `DList`s as well as for persisting `DList`s as Sequence files.  For more detail refer to the API docs for both Sequence file [input]($API_PAGE#com.nicta.scoobi.io.sequence.SeqInput$$) and [output]($API_PAGE#com.nicta.scoobi.io.sequence.SeqOutput$$).

##### Reading files

In a Sequence file there are key-value pairs where the types of the key and value must be `Writable` (i.e. are classes that implement the `Writable` interface). Given a Sequence file of `Writable` key-value pairs, a `DList` can be constructed: ${snippet{

// load a sequence file
val events1: DList[(TimestampWritable, TransactionWritable)] = fromSequenceFile("hdfs://path/to/transactions")

// alternatively, you can specify the key and value types
val events2 = fromSequenceFile[TimestampWritable, TransactionWritable]("hdfs://path/to/transactions")
}}

In this example, a Sequence file is being loaded where the key is of type `TimestampWritable` and the value is of type `TransactionWritable`. The result is a `DList` paramterised by the same key-value types. Note that whilst the classes associated with the key and value are specified within the header of a Sequence file, when using `${termName(fromSequenceFile[IW,IW]())}` they must also be specified. The signature of `fromSequenceFile` will enforce that the key and value types do implement the `Writable` interface, however, there are no static checks to ensure that the specified types actually match the contents of a Sequence file. It is the responsibility of the user to ensure there is a match else a run-time error will result.

Like `fromTextFile`, `fromSequenceFile` can also be passed multiple input paths as long as all files contain keys and values of the same type: ${snippet{

// load multiple sequence file
val events1: DList[(TimestampWritable, TransactionWritable)] =
  fromSequenceFile("hdfs://path/to/transactions1", "hdfs://path/to/transaction2")

// load from a list of sequence files
val transactionFiles = List("hdfs://path/to/transactions1", "hdfs://path/to/transaction2")
val events2: DList[(TimestampWritable, TransactionWritable)] = fromSequenceFile(transactionFiles)
}}

In some situations only the key or value needs to be loaded. To make this use case more convient, Scoobi provides two additional methods: `${termName(keyFromSequenceFile[Int]())}` and `${termName(valueFromSequenceFile[Int]())}`. When using `${termName(keyFromSequenceFile[Int]())}` or `${termName(valueFromSequenceFile[Int]())}`, Scoobi ignores the value or key, respectively, assuming it is just some `Writable` type: ${snippet{

// load keys only from an IntWritable-Text Sequence file
val ints: DList[IntWritable] = keyFromSequenceFile("hdfs://path/to/file")

// load values only from an IntWritable-Text Sequence file
val strings: DList[Text] = valueFromSequenceFile("hdfs://path/to/file")

}}

Hadoop's Sequence files provide a convenient mechanism for persisting data of custom types (so long as they implement `Writable`) in a binary file format. Hadoop also includes a number of common `Writable` types, such as `IntWritable` and `Text` that can be used within an application. For Sequence files containing keys and/or values of these common types, Scoobi provides additional convenience methods for constructing a `DList` and automatically converting values to common Scala types: ${snippet{

// load a IntWritable-Text sequence file
val data: DList[(Int, String)] = fromSequenceFile("hdfs://path/to/file")
}}

In the above code, a Sequence file of `IntWritable`-`Text` pairs is being loaded as a `DList` of `Int`-`String` pairs. Just as with `fromSequenceFile`, type annotations are necessary, but in this case, the `(Int, String)` annotation is signalling that the Sequence file is contains `IntWritable`-`Text` pairs, not `Int`-`String` pairs. The table below lists the `Writable` conversions supported by `fromSequenceFile`:

 Writable type          | Scala type
 ---------------------- | ----------
 `BooleanWritable`      | `Boolean`
 `IntWritable`          | `Int`
 `FloatWritable`        | `Float`
 `LongWritable`         | `Long`
 `DoubleWritable`       | `Double`
 `Text`                 | `String`
 `ByteWritable`         | `Byte`
 `BytesWritable`        | `Traversable[Byte]`

Conversion support for `BytesWritable` is interesting as the type of Scala collection it converts to is not fixed and can be controlled by the user. For example, it is possible to specify conversion to `List[Byte]` or `Seq[Byte]`: ${snippet{

// load a DoubleWritable-BytesWritable sequence file
val data1: DList[(Double, List[Byte])] = fromSequenceFile("hdfs://path/to/file")

// also ok
val data2: DList[(Double, Seq[Byte])] = fromSequenceFile("hdfs://path/to/file")
}}

#### Avro files

[Avro](http://avro.apache.org/) is a language-agnostic specification for data serialization. From a Hadoop perspective it has a lot of the attributes of Sequence files with the addition of features such as evolvable schemas.

Avro *schemas* describe the structure of data and are the key to creating or loading an Avro file. Scoobi provides a mechansim for mapping between Avro schemas and Scala types such that an Avro file can be easily loaded as a `DList` with the correct type parameterization, and a `DList` can be easily persisted as an Avro file with the correct schema.

##### Avro schemas

The mechanism for mapping between Avro schemas and Scala types is the [`AvroSchema`]($API_PAGE#com.nicta.scoobi.io.avro.AvroSchema) type class. Instances are provided for all Scala types that have sensbile mappings to Avro schema elements:

 Scala type                | Avro Schema
 ----------                | -----------
 `Boolean`                 | `boolean`
 `Int`                     | `int`
 `Float`                   | `gloat`
 `Long`                    | `long`
 `Double`                  | `double`
 `String`                  | `string`
 `Traversable[_]`          | `array`
 `Array[_]`                | `array`
 `Map[_,_]`                | `map`
 `Tuple2[_,_]`             | `record`
 `Tuple3[_,_,_]`           | `record`
 `Tuple4[_,_,_,_]`         | `record`
 `Tuple5[_,_,_,_,_]`       | `record`
 `Tuple6[_,_,_,_,_,_]`     | `record`
 `Tuple7[_,_,_,_,_,_,_]`   | `record`
 `Tuple8[_,_,_,_,_,_,_,_]` | `record`

Note that, like Avro schemas, the Scala types can be fully nested. For example, the Scala type:

    (Int, Seq[(Float, String)], Map[String, Int])

would map to the Avro schema:

    {
      "type": "record",
      "name": "tup74132vn1nc193418",      // Scoobi-generated UUID
      "fields" : [
        {
          "name": "v0",
          "type": "int"
        },
        {
          "name": "v1",
          "type": {
            "type": "array",
            "items": {
              "type": {
                "type": "record",
                "name": "tup44132vr1ng198419",
                "fields": [
                  {
                    "name": "v0",
                    "type": "float"
                  },
                  {
                    "name": "v1",
                    "type": "string"
                  }
                ]
              }
            }
          }
        },
        {
          "name": "v2",
          "type": {
            "type": "map",
            "values": "int"
          }
        }
      ]
    }

##### Reading files

The method [`${termName(fromAvroFile[I](""))}`]($API_PAGE#com.nicta.scoobi.io.avro.AvroInput$$) is used to load an Avro file as a `DList`: ${snippet{

val xs = fromAvroFile[(Int, Seq[(Float, String)], Map[String, Int])]("hdfs://path/to/file")

}}

As with `${termName(fromSequenceFile[I, I](""))}`, the compiler needs to know the type of avroFile you are loading. If the file doesn't match this schema, a runtime error will occur. `${termName(fromAvroFile[I](""))}` has a default argument `checkSchemas` that tries to fail-fast by verifying the schema matches.

Note that for compilation to succeed, there must be an `AvroSchema` instance for the particular type you are using. For example, the following will fail unless an `AvroSchema` type class instance for `Person` is implemented and in scope: ${snippet{

// assuming case class Person(name: String, age: Int)
// will not compile, unless you provide an AvroSchema
val people = fromAvroFile[Person]("hdfs://path/to/file")

}}

However, there is is a scala-avro plugin to make this pretty painless (See: examples/avro for an example)

And naturally, `fromAvroFile` supports loading from multiple files: ${snippet{

// load multiple Avro files
val xs1: DList[(Int, String, Float)] = fromAvroFile("hdfs://path/to/file1", "hdfs://path/to/file2")

// load from a list of Avro file
val files = Seq("hdfs://path/to/file1", "hdfs://path/to/file2")
val xs2: DList[(Int, String, Float)] = fromAvroFile(files)

}}

#### Without files

Because Scoobi is a library for constructing Hadoop applications, *data* input and ouput is typically synonymous with *file* input and output. Whilst Scoobi provides numerous mechanism for creating new `DList` objects from files (and multiple file types), it also has some simple ways for constructing a `DList` without files.

The simplest way of creating a new `DList` object is to use the `DList` companion object's `apply` method. This behaves just like the Scala `List` version: ${snippet{

// create a DList[Int] object
val ints = DList(1, 2, 3, 4)

// create a DList[String] object
val strings = DList("bob", "mary", "jane", "fred")

// create a DList[(String, Int)] object
val ages = DList(("bob", 12), ("mary", 33), ("jane", 61), ("fred", 24))

}}

As a convenience, the `apply` method is also overloaded to handle the special case of integer ranges. This allows a `DList` of `Int` values to be constructed than can span a range: ${snippet{

// all integers from 0 to 1023
val manyInts: DList[Int] = DList(0 to 1023)

}}

Whilst using `apply` is simple, this is typically not all that useful in practice. The purpose of a `DList` is to abstract large volumes of data. Using the `apply` method in this way, only memory-bound data sizes can be handled. As an alternative, the `tabulate` method can be used to create much larger `DList` objects where an element *value* can be specified by a function applied to an element *index*. This is particularly useful for creating randomized `DList` objects: ${snippet{

// random integer values
val randomInts = DList.tabulate(1000 * 1000)(_ => Random.nextInt)

// words pairs taken randomly from a bag of words
val words: Seq[String] = Seq(???)
def hash(i: Int) = (i * 314 + 56) % words.size
val randomWords: DList[(String, String)] = DList.tabulate(1000 * 1000)(ix => (words(hash(ix)), words(hash(ix + 1))))

}}

Finally, for pure convenience, with Scoobi all Scala `Traversable` collections can be converted to `DList` objects `toDList` method: ${snippet{

val wordList = List("hello", "big", "data", "world")
val wordDList: DList[String] = wordList.toDList

val numbersMap = Map("one" -> 1, "two" -> 2, "three" -> 3)
val numbersDList: DList[(String, Int)] = numbersMap.toDList

}}

#### DObjects

It is also possible to load and persist DObjects. A DObject, when persisted, is either stored like a `DList[A]` if it is a `DObject[Iterable[A]]` or a `DList[A]` containing just one element if it is a `DObject[A]`. In the first case, you can load the `DObject` by loading the file as a `DList[T]` and materialising it:  ${snippet{

  val sums: DObject[Iterable[Int]] = fromAvroFile[Int]("hdfs://path/to/average").materialise
}}

In the second case you can use methods which are very similar to `DList` methods, having `object` appended in front of them: ${snippet{

val average1: DObject[String] = objectFromTextFile("hdfs://path/to/text/average")
val average2: DObject[Int]    = objectKeyFromSequenceFile[Int]("hdfs://path/to/seq/average")
val average3: DObject[Int]    = objectFromAvroFile[Int]("hdfs://path/to/avro/average")

}}

Note however that those methods are unsafe. They are merely a shortcut to access the first element of a persisted `DList`. Another possibility is to load a `DList` and use `headOption` to create a `DObject`: ${snippet{

val average: DObject[Option[Int]] = fromAvroFile[Int]("hdfs://path/to/avro/average").headOption

}}

### Persisting

*Persisting* is the mechanism Scoobi uses for specifying that the result of executing the computational graph associated with a `DList` object is to be associated with a particular data file on HDFS. There are two parts to persisting:

1. Specifying how a `DList` object is to be persisted by using the numerous `toXXX` methods available (`toTextFile`, `toAvroFile`,...)
2. Persisting the `DList`(s) by calling `persist`

This is an example of persisting a single `DList`:

    val rankings: DList[(String, Int)] = ...

    persist(rankings.toTextFile("hdfs://path/to/output"))

And now with several `DLists`:

    val rankings: DList[(String, Int)] = ...
    val rankings_reverse: DList[(Int, String)] = rankings map { swap }
    val rankings_example: DList[(Int, String)] = rankings_reverse.groupByKey.map{ case (ranking, items) => (ranking, items.head) }

    persist(rankings.        toTextFile("hdfs://path/to/output"),
            rankings_reverse.toTextFile("hdfs://path/to/output-reverse"),
            rankings_example.toTextFile("hdfs://path/to/output-example"))

As mentioned previously, `persist` is the trigger for executing the computational graph associated with its `DList` objects. By bundling `DList` objects together, `persist` is able to determine computations that are shared by those outputs and ensure that they are only performed once.

#### DObjects

`DObjects` are results of distributed computations and can be accessed in memory with the `run` method:

    val list: DList[Int]  = DList(1, 2, 3)

    // the sum of all values
    val sum: DObject[Int] = list.sum

    // execute the computation graph and collect the result
    println(sum.run)

The call to `run` above is equivalent to calling `persist` on the `DObject` to execute the computation, then collecting the result. If you call:

    persist(sum)
    sum.run

then the first `persist` executes the computation and `run` merely retrieves the result.

Similarly, if you want to access the value of a `DList` after computation, you can call `run` on that list:

     val list: DList[Int]  = DList(1, 2, 3)

     // returns Seq(1, 2, 3)
     list.run

The code above is merely a shorthand for:

      val list: DList[Int]  = DList(1, 2, 3)

      val materialisedList = list.materialise
      // returns Seq(1, 2, 3)
      materialisedList.run

Finally, when you have several `DObjects` and `DLists` which are part of the same computation graph, you can persist them all at once:

     val list: DList[Int]    = DList(1, 2, 3)
     val plusOne: DList[Int] = list.map(_ + 1)

     // the sum of all values
     val sum: DObject[Int] = list.sum
     // the max of all values
     val max: DObject[Int] = list.max

    // execute the computation graph for the 2 DObjects and one DList
    persist(sum, max, plusOne)

    // collect results
    // (6, 3, Seq(2, 3, 4))
    (sum.run, max.run, plusOne.run)

#### Iterations

Many distributed algorithms (such as PageRank) require to iterate over DList computations. You evaluate the results of a DList computation, and based on that, you decide if you should go on with more computations.

For example, let's say we want to remove 1 to a list of positive elements (and nothing if the element is already 0) until the maximum is 10.

There are several ways to write this, which we are going to evaluate:

     val ints = DList(12, 5, 8, 13, 11)

     def iterate1(list: DList[Int]): DList[Int] = {
       if (list.max.run > 10) iterate(list.map(i => if (i <= 0) i else i - 1))
       else                   list
     }

     def iterate2(list: DList[Int]): DList[Int] = {
       persist(list)
       if (list.max.run > 10) iterate(list.map(i => if (i <= 0) i else i - 1))
       else                   list
     }

     def iterate3(list: DList[Int]): DList[Int] = {
       persist(list, list.max)
       if (list.max.run > 10) iterate(list.map(i => if (i <= 0) i else i - 1))
       else                   list
     }

     def iterate4(list: DList[Int]): DList[Int] = {
       val maximum = list.max
       persist(list, maximum)
       if (maximum.run > 10) iterate(list.map(i => if (i <= 0) i else i - 1))
       else                  list
     }

     persist(iterate1(ints).toTextFile("path"))
     persist(iterate2(ints).toTextFile("path"))
     persist(iterate3(ints).toTextFile("path"))
     persist(iterate4(ints).toTextFile("path"))

 1. no intermediary call to `persist`

In that case we get the least amount of generated MapReduce jobs, 5 jobs only: 4 jobs for the 4 main iterations, to do mapping + maximum, plus one job to write out the data to a text file

The big disadvantage of this method is that the `DList` being computed is getting bigger and bigger all being re-computed all over for each new iteration.

 2. one call to persist the intermediate `DList`

Here, before trying to evaluate the maximum value of the list, we save the mapped list first because later on we know we want to resume the computations from that stage, then we compute the maximum.
This generates 8 MapReduce jobs: 4 jobs to map the list each time we enter the loop + 4 jobs to compute the maximum. However, if we compare with 1. the computations are reduced to a mimimum for each job because we reuse previously saved data.

 3. one call to persist the intermediate `DList` and the maximum

This variation creates 12 MapReduce jobs: 4 to map the list on each iteration, 4 to compute the maximum on each iteration (because even if the list and its maximum are persisted at the same time, one depends on the other) and 4 to recompute the maximum and bring it to memory! The issue here is that we call `list.max` twice, hereby effectively creating 2 similar but duplicate `DObject`s.

 4. one call to persist the intermediate `DList` and the maximum as a variable

In this case we get a handle on the `maximum` `DObject` and accessing his value with `run` is just a matter of reading the persisted information hence the number of MapReduce jobs is 8, as in case 2.

##### Interim files

It might be useful, for debugging reasons, to save the output of each intermediary step. Here is how to do it:

     val ints = DList(12, 5, 8, 13, 11)

     def iterate5(list: DList[Int]): DList[Int] = {
       persist(list)
       if (list.max.run > 10) iterate(list.map(i => if (i <= 0) i else i - 1).toAvroFile("out", overwrite = true))
       else                   list
     }
     // no need to persist to a Text file since there is already an Avro file storing the results
     persist(iterate5(ints))

With the code above the intermediary results will be written to the same output directory. You can also create one output directory per iteration:

     def iterate6(list: DList[Int], n: Int = 0): DList[Int] = {
       persist(list)
       if (list.max.run > 10) iterate(list.map(i => if (i <= 0) i else i - 1).toAvroFile("out"+n, overwrite = true), n+1)
       else                   list
     }
     persist(iterate6(ints))

#### Checkpoints

When you have a big pipeline of consecutive computations it can be very time-consuming to start the process all over again if you've just changed some function down the track.

In order to avoid this you can create *checkpoints*, that is sinks which will persist data in between executions:

     // before
     val list = DList(1, 2, 3).map(_ + 1).
                               filter(isEven)

     // after
     val list = DList(1, 2, 3).map(_ + 1).toAvroFile("path", overwrite = true).checkpoint.
                               filter(isEven)

If you run the `after` program twice, the second time the program is run, only the `filter` operation will be executed taking its input data from the saved Avro file.

*Important limitation*: you can't use a `Text` sink as a checkpoint because Text file sinks can't not be used as source files.

  """

  implicit lazy val configuration: ScoobiConfiguration = ScoobiConfiguration()
  trait TimestampWritable extends Writable
  trait TransactionWritable extends Writable
  type IW = IntWritable
  type I = Int

  case class Person(name: String, age: Int)
  implicit def wf: WireFormat[Person] = mkCaseWireFormat(Person.apply _, Person.unapply _)
  implicit def avroSchemaForPerson: AvroSchema[Person] = new AvroSchema[Person] {
    type AvroType = Person
    def schema: Schema = ???
    def fromAvro(x: AvroType) = x
    def toAvro(x: Person): AvroType = x
  }

}

