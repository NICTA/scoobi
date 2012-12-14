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

class InputOutput extends ScoobiPage { def is = "Input and Output".title ^
                                                                                                                       """
### Text files

Text files are one of the simplest forms of input/output provided by Scoobi. The following sections describe the various ways in which `DList`s can be loaded from text files as well as persisted to text files. For more detail refer to the API docs for both text [input](${SCOOBI_API_PAGE}#com.nicta.scoobi.io.text.TextInput$) and [output](${SCOOBI_API_PAGE}#com.nicta.scoobi.io.text.TextOutput$).

#### Text file input

There are a number of ways in which to construct a `DList` object from a text file. The simplest, which we have seen already, is `fromTextFile`. It takes one or more paths (globs are supported) to text files on HDFS (or whichever file system Hadoop has been configured for) and returns a `DList[String]` object, where each element of the distributed list refers to one of the lines of text from the files:

    // load a single text file
    val lines: DList[String] = fromTextFile("hdfs://path/to/file")

    // load multiple text files
    val lines: DList[String] = fromTextFile("hdfs://path/to/file1", "hdfs://path/to/file2")

    // load from a list of text files
    val textFiles = List("hdfs://path/to/file1", "hdfs://path/to/file2")
    val lines: DList[String] = fromTextFile(textFiles)

In the case where mulitple paths are specified, in out `DList` we may also want to know which file a particular line of text orginated from. This can be achieved with `fromTextFileWithPath`:

    // load a list of text files
    val textFiles = List("hdfs://path/to/file1", "hdfs://path/to/file2")
    val lines: DList[(String, String)] = fromTextFileWithPath(textFiles)

The resultant `DList` in this example is of type `(String, String)`. Here the second part of the pair is a line of text, just as you would have if `fromTextFile` was used. The first part of the pair is the path of the file the text file originated from.

Whilst some problems involve working with entire lines of text, often it's the case that we are interested in loading delimited text files, for example, comma separated value (CSV) or tab separated value (TSV) files and want to extract values from *fields*.  In this case, we could use `fromTextFile` followed by a `map` that pulls out fields of interest:

    // load CSV with schema "id,first_name,second_name,age"
    val lines: DList[String] = fromTextFile("hdfs://path/to/CVS/files/*")

    // pull out id and second_name
    val names: DList[(Int, String)] = lines map { line =>
      val fields = line.split(",")
      (fields(0).toInt, fields(2))
    }

Given that these types of field extractions from delimited text files are such a common task, Scoobi provides a more convenient mechanism for achieving this:

    // load CSV and pull out id and second_name
    val names: DList[(Int, String)] = fromDelimitedTextFile("hdfs://path/to/CVS/files/*", ",") {
      case Int(id) :: first_name :: second_name :: age :: _ => (id, second_name)
    }

As this example illustrates, the call to `fromDelimitedTextFile` takes a number of arguements. The first argument specifies the path and the second is the delimiter, in this case a comma. Following is a second *parameter list* that is used to specify how to extract fields once they are separated out. This is specified by supplying a *partial function* that takes a list of separated `String` fields as its input and returns a value whose type will set the type of the resulting `DList` - i.e. a `PartialFunction[List[String], A]` will create a `DList[A]` (where `A` is `(Int, String)` above). In this example, we use Scala's [pattern matching](http://www.scala-lang.org/node/120) feature to *pull out* the four fields and return the first and third.

In addition Scoobi also provides a number of [extractors](http://www.scala-lang.org/node/112) for automatically checking and converting of fields to an expected type. In the above example, the `Int` extractor is used to specify that the `id` field must be an integer in order for the `case` statement to match. In the case of a match, it also has the effect of typing `id` as an `Int`. Field extractors are provided for `Int`, `Long`, `Double` and `Float`.

One of the advantages of using `fromDelimitedTextFile` is that we have at our disposal all of the Scala pattern matching features, and because we are providing a partial function, any fields that don't match against the supplied pattern will not be present in the returned `DList`. This allows us to implement simple filtering inline with the extraction:

    // load CSV and pull out id and second_name if first_name is "Harry"
    val names: DList[(Int, String)] = fromDelimitedTextFile("hdfs://path/to/CSV/files/*", ",") {
      case Int(id) :: "Harry" :: second_name :: age :: _ => (id, second_name)
    }

We can of course supply multiple patterns:

    // load CSV and pull out id and second_name if first_name is "Harry" or "Lucy"
    val names: DList[(Int, String)] = fromDelimitedTextFile("hdfs://path/to/CSV/files/*", ",") {
      case Int(id) :: "Harry" :: second_name :: age :: _ => (id, second_name)
      case Int(id) :: "Lucy"  :: second_name :: age :: _ => (id, second_name)
    }

And, a more interesting example is when the value of one field influences the semantics of another. For example:

    val thisYear: Int = ...

    // load CSV with schema "event,year,year_designation" and pull out event and how many years ago it occurred
    val yearsAgo: DList[(String, Int)] = fromDelimitedTextFile("hdfs://path/to/CSV/files/*", ",") {
      case event :: Int(year) :: "BC" :: _ => (event, thisYear + year - 1) // No 0 AD
      case event :: Int(year) :: "AD" :: _ => (event, thisYear - year)
    }

#### Text file output

The simplest mechanism for persisting a `DList` of any type is to store it as a text file using `toTextFile`. This will simply invoke the `toString` method of the type that the `DList` is parameterised on:

    // output text file of the form:
    //    34
    //    3984
    //    732
    val ints: DList[Int] = ...
    persist(toTextFile(ints, "hdfs://path/to/output"))

    // output text file of the form:
    //    (foo, 6)
    //    (bob, 23)
    //    (joe, 91)
    val stringsAndInts: DList[(String, Int)] = ...
    persist(toTextFile(stringsAndInts, "hdfs://path/to/output"))

    // output text file of the form:
    //    (foo, List(6, 3, 2))
    //    (bob, List(23, 82, 1))
    //    (joe, List(91, 388, 3))
    val stringsAndListOfInts: DList[(String, List[Int])] = ...
    persist(toTextFile(stringsAndListOfInts, "hdfs://path/to/output"))

In the same way that `toString` is used primarily for debugging purposes, `toTextFile` is best used for the same purpose. The reason is that the string representation for any reasonably complex type is generally
not convenient for input parsing. For cases where text file output is still important, and the output must be easily parsed, there are two options.

The first is to simply `map` the `DList` elements to formatted strings that are easily parsed. For example:

    // output text file of the form:
    //    foo,6
    //    bob,23
    //    joe,91
    val stringsAndInts: DList[(String, Int)] = ...
    val formatted: DList[String] = stringAndInts map { case (s, i) => s + "," + i }
    persist(toTextFile(stringsAndInts, "hdfs://path/to/output"))

The second option is for cases when the desired output is a delimited text file, for example, a CSV or TSV. In this case, if the `DList` is parameterised on a `Tuple`, *case class*, or any `Product` type, `toDelimitedTextFile` can be used:

    // output text file of the form:
    //    foo,6
    //    bob,23
    //    joe,91
    val stringsAndInts: DList[(String, Int)] = ...
    persist(toDelimitedTextFile(stringsAndInts, "hdfs://path/to/output", ","))

    // output text file of the form:
    //    foo,6
    //    bob,23
    //    joe,91
    case class PeopleAges(name: String, age: Int)
    val peopleAndAges: DList[PeopleAges] = ...
    persist(toDelimitedTextFile(peopleAndAges, "hdfs://path/to/output", ","))

### Sequence files

Sequence files are the built-in binary file format used in Hadoop. Scoobi provides a number of ways to load existing Sequence files as `DList`s as well as for persisting `DList`s as Sequence files.  For more detail refer to the API docs for both Sequence file [input](${SCOOBI_API_PAGE}#com.nicta.scoobi.io.sequence.SeqInput$) and [output](${SCOOBI_API_PAGE}#com.nicta.scoobi.io.sequence.SeqOutput$).

#### Sequence file input

A Sequence file is a binary file of key-value pairs where the types of the key and value must be `Writable` (i.e. are classes that implement the `Writable` interface). Given a Sequence file of `Writable` key-value pairs, a `DList` can be constructed:

    // load a sequence file
    val events: DList[(TimestampWritable, TransactionWritable)] = fromSequenceFile("hdfs://path/to/transactions")

    // alternatively
    val events = fromSequenceFile[(TimestampWritable, TransactionWritable)]("hdfs://path/to/transactions")

In this example, a Sequence file is being loaded where the key is of type `TimestampWritable` and the value is of type `TransactionWritable`. The result is a `DList` paramterised by the same key-value types. Note that whilst the classes associated with the key and value are specified within the header of a Sequence file, when using `fromSequenceFile` they must also be specified. The signature of `fromSequenceFile` will enforce that the key and value types do implement the `Writable` interface, however, there are no static checks to ensure that the specified types actually match the contents of a Sequence file. It is the responsibility of the user to ensure there is a match else a run-time error will result.

Like `fromTextFile`, `fromSequenceFile` can also be passed multiple input paths as long as all files contain keys and values of the same type:

    // load multiple sequence file
    val events: DList[(TimestampWritable, TransactionWritable)] =
      fromSequenceFile("hdfs://path/to/transactions1", "hdfs://path/to/transaction2")

    // load from a list of sequence file
    val transactionFiles = List("hdfs://path/to/transactions1", "hdfs://path/to/transaction2")
    val events: DList[(TimestampWritable, TransactionWritable)] = fromSequenceFile(transactionFiles)

In some situations only the key or value needs to be loaded. To make this use case more convient, Scoobi provides two additional methods: `keyFromSequenceFile` and `valueFromSequnceFile`. When using `keyFromSequenceFile` or
`valueFromSequnceFile`, Scoobi ignores the value or key, respectively, assuming it is just some `Writable` type:

    // load keys only from an IntWritable-Text Sequence file
    val ints: DList[IntWritable] = keyFromSequenceFile("hdfs://path/to/file")

    // load values only from an IntWritable-Text Sequence file
    val strings: DList[Text] = valueFromSequenceFile("hdfs://path/to/file")

Hadoop's Sequence files provide a convenient mechanism for persisting data of custom types (so long as they implement `Writable`) in a binary file format. Hadoop also includes a number of a number of common `Writable` types, such as `IntWritable` and `Text` that can be used within an application. For Sequence files containing keys and/or values of these common types, Scoobi provides additional convenience methods for constructing a `DList` and
automatically converting values to common Scala types:

    // load a IntWritable-Text sequence file
    val data: DList[(Int, String)] = convertFromSequenceFile("hdfs://path/to/file")

In the above code, a Sequence file of `IntWritable`-`Text` pairs is being loaded as a `DList` of `Int`-`String` pairs. Just as with `fromSequenceFile`, type annotations are necessary, but in this case, the `(Int, String)` annotation is signalling that the Sequence file is contains `IntWritable`-`Text` pairs, not `Int`-`String` pairs. The table below lists the `Writable` conversions supported by `convertFromSequenceFile`:

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

Conversion support for `BytesWritable` is interesting as the type of Scala collection it converts to is not fixed and can be controlled by the user. For example, it is possible to specify conversion to `List[Byte]` or `Seq[Byte]`:

    // load a DoubleWritable-BytesWritable sequence file
    val data: DList[(Double, List[Byte])] = convertFromSequenceFile("hdfs://path/to/file")

    // also ok
    val data: DList[(Double, Seq[Byte])] = convertFromSequenceFile("hdfs://path/to/file")

Finally, two additional conversion methods are provided for loading only the key or value component, `convertKeyFromSequenceFile` and `convertValueToSequenceFile`:

    // load keys only from an IntWritable-Text Sequence file
    val ints: DList[Int] = convertKeyFromSequenceFile("hdfs://path/to/file")

    // load values only from an IntWritable-Text Sequence file
    val strings: DList[String] = convertValueFromSequenceFile("hdfs://path/to/file")

#### Sequence file output

The available mechanism for persisting a `DList` to a Sequence file mirror those for persisting. The `toSequenceFile` method can be used to persist a `DList` of a `Writable` pair:

    val intText: DList[(IntWritable, Text)] = ...
    persist(toSequenceFile(intText, "hdfs://path/to/output"))

In cases where we want to persist a `DList` to a Sequence file but its type parameter is not a `Writable` pair,  single `Writable` can be stored as the key or the value, the other being `NullWritable`:

    // persist as IntWritable-NullWritable Sequence file
    val ints: DList[IntWritable] = ...
    persist(keyToSequenceFile(ints, "hdfs://path/to/output"))

    // persist as NullWritable-IntWritable Sequence file
    val ints: DList[IntWritable] = ...
    persist(valueToSequenceFile(ints, "hdfs://path/to/output"))

Like loading, `DList`s of simple Scala types can be automatically converted to `Writable` types and persisted as Sequence files. The extent of these automatic conversions is limited to the types listed in the table above. Value- and key-only veesions are also provided:

    // persist as Int-String Sequence fille
    val intString: DList[(Int, String)] = ...
    persist(convertToSequenceFile(intString, "hdfs://path/to/output"))

    // persist as Int-NullWritable Sequence fille
    val intString: DList[(Int, String)] = ...
    persist(convetKeyToSequenceFile(intString, "hdfs://path/to/output"))

    // persist as NullWritable-Int Sequence fille
    val intString: DList[(Int, String)] = ...
    persist(convertValueFromSequenceFile(intString, "hdfs://path/to/output"))

### Avro files

[Avro](http://avro.apache.org/) is a language-agnostic specification for data serialization. From a Hadoop perspective it has a lot of the attributes of Sequence files with the addition of features such as evolvable schemas.

Avro *schemas* describe the structure of data and are the key to creating or loading an Avro file. Scoobi provides a mechansim for mapping between Avro schemas and Scala types such that an Avro file can be easily loaded as a `DList` with the correct type parameterization, and a `DList` can be easily persisted as an Avro file with the correct schema.

#### Avro schemas

The mechanism for mapping between Avro schemas and Scala types is the [`AvroSchema`](${SCOOBI_API_PAGE}#com.nicta.scoobi.io.avro.AvroSchema) type class. Instances are provided for all Scala types that have sensbile mappings to Avro schema elements:

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

#### Avro file input

The method [`fromAvroFile`](${SCOOBI_API_PAGE}#com.nicta.scoobi.io.avro.AvroInput$) is used to load an Avro file as a `DList`:

    val xs = fromAvroFile[(Int, Seq[(Float, String)], Map[String, Int])]("hdfs://path/to/file")

As with `fromSequenceFile`, the compiler needs to know the type of avroFile you are loading. If the file doesn't match this schema, a runtime error will occur. `fromAvroFile` has a default argument `checkSchemas` that tries to fail-fast by verifying the schema matches. 

Note that for compilation to succeed, there must be an `AvroSchema` instance for the particular type you are using. For example, the following will fail unless an `AvroSchema` type class instance for `Person` is implemented and in scope:

    case class Person(name: String, age: Int)

    // will not compile, unless you provide an AvroSchema
    val people = fromAvroFile[Person]("hdfs://path/to/file")

However, there is is a scala-avro plugin to make this pretty painless (See: examples/avro for an example)

 
And naturally, `fromAvroFile` supports loading from multiple files:

    // load multiple Avro files
    val xs: DList[(Int, String, Float)] = fromAvroFile("hdfs://path/to/file1", "hdfs://path/to/file2")

    // load from a list of Avro file
    val files = List("hdfs://path/to/file1", "hdfs://path/to/file2")
    val xs: DList[(Int, String, Float)] = fromAvroFile(files)

#### Avro file output

To persist a `DList` to an Avro file, Scoobi provides the method [`toAvroFile`](${SCOOBI_API_PAGE}#com.nicta.scoobi.io.avro.AvroOutput$). Again, in order for compilation to succeed, the `DList` must be paramterised on a type that has an `AvroSchema` type class instance implemented:

    val xs: DList[(Int, Seq[(Float, String)], Map[String, Int])] = ...
    persist(toAvroFile(xs, "hdfs://path/to/file")

#### With a predefined avro schema

Any type that extends org.apache.avro.generic.GenericContainer scoobi knows how to generate a WireFormat for. This means that scoobi is capable of seemlessly interoperating with the Java classes, including the auto-generated ones (and sbt-avro is capable of generating a Java class for a given avro record/protocol. See `examples/avro` for an example of this plugin in action

### Without files

Because Scoobi is a library for constructing Hadoop applications, *data* input and ouput is typically synonymous with *file* input and output. Whilst Scoobi provides numerous mechanism for creating new `DList` objects from files (and multiple file types), it also has some simple ways for constructing a `DList` without files.

The simplest way of creating a new `DList` object is to use the `DList` companion object's `apply` method. This behaves just like the Scala `List` version:

    // create a DList[Int] object
    val ints = DList(1, 2, 3, 4)

    // create a DList[String] object
    val strings = DList("bob", "mary", "jane", "fred")

    // create a DList[(String, Int)] object
    val ages = DList(("bob", 12), ("mary", 33), ("jane", 61), ("fred", 24))

As a convenience, the `apply` method is also overloaded to handle the special case of integer ranges. This allows a `DList` of `Int` values to be constructed than can span a range:

    // all integers from 0 to 1023
    val manyInts: DList[Int] = DList(0 to 1023)

Whilst using `apply` is simple, this is typically not all that useful in practice. The purpose of a `DList` is to abstract large volumes of data. Using the `apply` method in this way, only memory-bound data sizes can be handled. As an alternative, the `tabulate` method can be used to create much larger `DList` objects where an element *value* can be specified by a function applied to an element *index*. This is particularly useful for creating randomized `DList` objects:

    // random integer values
    val randomInts = DList.tabulate(1000 * 1000)(_ => Random.nextInt())

    // words pairs taken randomly from a bag of words
    val words: Set[String] = ...
    def hash(i: Int) = (i * 314 + 56) % words.size
    val randomWords: DList[(String, String)] = DList.tabulate(1000 * 1000)(ix => (hash(ix), hash(ix + 1)))

Finally, for pure convenience, with Scoobi all Scala `Traversable` collections can be converted to `DList` objects via *pimping* and `toDList`:

    val wordList = List("hello", "big", "data", "world")
    val wordDList: DList[String] = wordList.toDList

    val numbersMap = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val numbersDList: DList[(String, Int)] = numbersMap.toDList

### Custom sources and sinks

Scoobi is not locked to loading and persisting the data sources and sinks that have been described. Instead,
the Scoobi API is designed in a way to make it relatively simple to implement support for custom data sources
and sinks.

#### Custom input sources

We have seen that Scoobi provides many *factory* methods for creaing `DList` objects, for example, `fromTextFile` and `fromAvroFile`. At their heart, all of these methods are built upon a single primitive mechanism: `DList` companion object's `fromSource` factory method:

    def fromSource[K, V, A : Manifest : WireFormat](source: DataSource[K, V, A]): DList[A]

`fromSource` takes as input an object implementing the `DataSource` trait. Implementing the `DataSource` trait is all that is required to create a `DList` from a custom data source. If we look at the `DataSource` trait, we can see that it is tightly coupled with the Hadoop `InputFormat` interface:

    trait DataSource[K, V, A] {
      def inputFormat: Class[_ <: InputFormat[K, V]]

      def inputConverter: InputConverter[K, V, A]

      def inputCheck()

      def inputConfigure(job: Job): Unit

      def inputSize(): Long
    }

    trait InputConverter[K, V, A] {
      type InputContext = MapContext[K, V, _, _]
      def fromKeyValue(context: InputContext, key: K, value: V): A
    }

The core role of a `DataSource` is to provide a mechanism for taking the key-value records produced by an `InputFormat` and converting them into the values contained within a `DList`. Following the type parameters is a good way to understand this:

 * `inputFormat` specifies an `InputFormat` class
 * The `InputFormat` class will produce key-value records of type `K`-`V`
 * `inputConverter` specifies an `InputConverter` object
 * The `InputConverter` object implments `fromKeyValue` which converts a key of type `K` and a value of type `V` (as produced by the `InputFormat`) to a value of type `A`
 * Calling `fromSource` with this `DataSource` object will produce a `DList` parameterised on type `A`

The other methods that must be implemented in the `DataSource` trait provide hooks for configuration and giving Scoobi some visibility of the data source:

 * `inputCheck`: This method is called before any MapReduce jobs are run. It is provided as a hook to check the valiidity of data source input. For example, it could check that the input exists and if not
throw an exception.
 * `inputConfigure`: This method is provided as a hook to configure the `DataSource`. Typically it is used to configure the `InputFormat` by adding or modifying properties in the job's `Configuration`. It
is called prior to running the specific MapReduce job this `DataSoure` provides input data to.
 * `inputSize`: This method should returns an estimate of the size in bytes of the input data source. It does not need to be exact. Scoobi will use this value as one metric in determining how to configure the execution of MapReduce jobs.

The following Scala objects provided great working examples of `DataSource` implementations in Scoobi:

 * [TextInput](${SCOOBI_API_PAGE}#com.nicta.scoobi.io.text.TextInput$)
 * [SeqInput](${SCOOBI_API_PAGE}#com.nicta.scoobi.io.sequence.SeqInput$)
 * [AvroInput](${SCOOBI_API_PAGE}#com.nicta.scoobi.io.avro.AvroInput$)
 * [FunctionInput](${SCOOBI_API_PAGE}#com.nicta.scoobi.io.func.FunctionInput$)

#### Custom output sources

We have seen that to persist a `DList` object we use the `persist` method:

    persist(toTextFile(dogs, "hdfs://path/to/dogs"), toAvroFile(names, "hdfs://path/to/names))

But what is the type of `toTextFile`, `toAvroFile` and the other output methods? The `persist` method takes as input one or more `DListPersister` objects:

    case class DListPersister[A](dlist: DList[A], val sink: DataSink[_, _, A])

The `DListPersister` class is simply the `DList` object to be persisted and an accompanying *sink* object that implements the `DataSink` trait. The `DataSink` trait is, not surpringly, the reverse of the `DataSource` trait. It is tightly coupled with the Hadoop `OutputFormat` interface and must requires the specification of an `OutputConverter` that converts values contained within the `DList` to key-value records to be persisted by the `OutputFormat`:

    trait DataSink[K, V, B] {

      def outputFormat: Class[_ <: OutputFormat[K, V]]
      def outputConverter: OutputConverter[K, V, B]
      def outputKeyClass: Class[K]
      def outputValueClass: Class[V]
      def outputCheck()
      def outputConfigure(job: Job): Unit
    }

    trait OutputConverter[K, V, B] {
      def toKeyValue(x: B): (K, V)
    }

Again, we can follow the types through to get a sense of how it works:

 * `persist` is called with a `DListPersister` object that is created from a `DList[B]` object and an object implementing the trait `DataSink[K, V, B]`
 * The `DataSink` object specifies the class of an `OutputFormat` that can persist or write key-values of type `K`-`V`, which are specified by `outputKeyClass` and `outputValueClass`, respectively
 * An object implementing the `OutputConverter[K, V, B]` trait is specified by `outputConverter`, which converts values of type `B` to `(K, V)`

 Like `DataSouce`, some additional methods are included in the `DataSink` trait that provide configuation hooks:

 * `outputCheck`: This method is called before any MapReduce jobs are run. It is provided as a hook to check the validity of the target data output. For example, it could check if the output already exists and if so throw an exception
 * `outputConfigure`: This method is provided as a hook for configuring the `DataSink`. Typically it is used to configure the `OutputFormat` by adding or modifying properties in the job's `Configuration`. It is called prior to running the specific MapReduce job this `DataSink` consumes output data from

The following Scala objects provided great working examples of `DataSink` implementations in Scoobi:

 * [TextOutput](${SCOOBI_API_PAGE}#com.nicta.scoobi.io.text.TextOutput$)
 * [SeqOutput](${SCOOBI_API_PAGE}#com.nicta.scoobi.io.sequence.SeqOutput$)
 * [AvroOutput](${SCOOBI_API_PAGE}#com.nicta.scoobi.io.avro.AvroOutput$)


                                                                                                                        """
}
