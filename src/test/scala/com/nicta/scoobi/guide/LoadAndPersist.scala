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

class LoadAndPersist extends ScoobiPage { def is = "Loading and persisting data".title^
                                                                                                                        """
`DList` objects are merely nodes in a graph describing a series of data computation we want to perform. However, at some point we need to specify what the inputs and outputs to that computation are. We have already seen this in the previous example with `fromTextFile(...)` and `persist(toTextFile(...))`. The former is an example of *loading* data and the latter is an example of *persisting* data.

### Loading

Most of the time when we create `DList` objects, it is the result of calling a method on another `DList` object (e.g. `map`). *Loading*, on the other hand, is the only way to create a `DList` object that is not based on any others. It is the means by which we associate a `DList` object with some data files on HDFS. Scoobi provides functions to create `DList` objects associated with text files on HDFS, which are implemented in the object [`com.nicta.scoobi.io.text.TextInput`](${SCOOBI_API_PAGE}#com.nicta.scoobi.io.text.TextInput$).

The simplest, which we have seen already, is `fromTextFile`. It takes a path (globs are supported) to text files on HDFS (or whichever file system Hadoop has been configured for) and returns a `DList[String]` object, where each element of the distributed collection refers to one of the lines of text from the files.

Often we are interested in loading delimited text files, for example, comma separated value (CSV) files. In this case, we can use `fromTextFile` followed by a `map` to pull out fields of interest:

    // load CSV with schema "id,first_name,second_name,age"
    val lines: DList[String] = fromTextFile("hdfs://path/to/CVS/files/*")

    // pull out id and second_name
    val names: DList[(Int, String)] = lines map { line =>
      val fields = line.split(",")
      (fields(0).toInt, fields(2))
    }

This works fine, but because it's such a common task, `TextInput` also provides the function `fromDelimitedTextFile` specifically for these types of field extractions:

    // load CSV and pull out id and second_name
    val names: DList[(Int, String)] = fromDelimitedTextFile("hdfs://path/to/CVS/files/*", ",") {
      case id :: first_name :: second_name :: age :: _ => (id.toInt, second_name)
    }

When using `fromDelimitedTextFile`, the first argument specifies the delimiter and the second is the path. However, there is also a second *parameter list* which is used to specify what to do with fields once they are separated out. This is specified by supplying a *partial function* that takes a list of separated `String` fields as its input and returns a value whose type will set the type of the resulting `DList` - i.e. a `PartialFunction[List[String], A]` will create a `DList[A]` (where `A` is `(Int, String)` above). In this example, we use Scala's [pattern matching](http://www.scala-lang.org/node/120) feature to *pull out* the four fields
and return the first and third.

One of the advantages of this approach is that we have at our disposal all of the Scala pattern matching features, and because we are providing a partial function, any fields that don't match against the supplied pattern will not be present in the returned `DList`.This allows us implement simple filtering inline with the extraction:

    // load CSV and pull out id and second_name if first_name is "Harry"
    val names: DList[(Int, String)] = fromDelimitedTextFile("hdfs://path/to/CSV/files/*", ",") {
      case id :: "Harry" :: second_name :: age :: _ => (id.toInt, second_name)
    }

We can of course supply multiple patterns:

    // load CSV and pull out id and second_name if first_name is "Harry" or "Lucy"
    val names: DList[(Int, String)] = fromDelimitedTextFile("hdfs://path/to/CSV/files/*", ",") {
      case id :: "Harry" :: second_name :: age :: _ => (id.toInt, second_name)
      case id :: "Lucy"  :: second_name :: age :: _ => (id.toInt, second_name)
    }

And, a more interesting example is when the value of one field influences the semantics of another. For example:

    val thisYear: Int = ...

    // load CSV with schema "event,year,year_designation" and pull out event and how many years ago it occurred
    val yearsAgo: DList[(String, Int)] = fromDelimitedTextFile("hdfs://path/to/CSV/files/*", ",") {
      case event :: year :: "BC" :: _ => (event, thisYear + year.toInt - 1) // No 0 AD
      case event :: year :: "AD" :: _ => (event, thisYear - year.toInt)
    }

These are nice features. However, one of the problems with these examples is their conversion of a `String` fields into an `Int`. If the field is not supplied (e.g. empty string) or the files are simply erroneous, a run-time exception will occur when `toInt` is called. This exception will be caught by Hadoop and likely cause the MapReduce job to fail. As a solution to this problem, `TextInput` provides Scala [extractors](http://www.scala-lang.org/node/112) for `Int`s, `Long`s and `Double`s. Using the `Int` extractor we can rewrite one of the above examples:

    // load CSV and pull out id and second_name
    val names: DList[(Int, String)] = fromDelimitedTextFile("hdfs://path/to/CSV/files/*", ",") {
      case Int(id) :: first_name :: second_name :: Int(age) :: _ => (id, second_name)
    }

Here, the pattern will only match if the `id` (and `age`) field(s) can be converted successfully from a `String` to an `Int`.  If not, the pattern will not match and that line will not be extracted into the resulting `DList`.

### Persisting

*Persisting* is the mechanism Scoobi uses for specifying that the result of executing the computational graph associated with a `DList` object is to be associated with a particular data file on HDFS. There are two parts to persisting:

1. Calling `persist`, which bundles all `DList` objects being persisted;
2. Specifying how each `DList` object is to be persisted.

Scoobi currently only provides one mechanism for specifying how a `DList` is to be persisted. It is `toTextFile` and is implemented in the object [`com.nicta.scoobi.io.text.TextOutput`](${SCOOBI_API_PAGE}#com.nicta.scoobi.io.text.TextOutput$). As we have seen previously, `toTextFile` takes two arguments: the `DList` object being persisted and the directory path to write the resulting data:

    val rankings: DList[(String, Int)] = ...

    persist(toTextFile(rankings, "hdfs://path/to/output"))

`persist` can of course bundle together more than one `DList`. For example:

    val rankings: DList[(String, Int)] = ...
    val rankings_reverse: DList[(Int, String)] = rankings map { swap }
    val rankings_example: DList[(Int, String)] = rankings_reverse.groupByKey.map{ case (ranking, items) => (ranking, items.head) }

    persist(toTextFile(rankings,         "hdfs://path/to/output"),
            toTextFile(rankings_reverse, "hdfs://path/to/output-reverse"),
            toTextFile(rankings_example, "hdfs://path/to/output-example"))

As mentioned previously, `persist` is the trigger for executing the computational graph associated with its `DList` objects. By bundling `DList` objects together, `persist` is able to determine computations that are shared by those outputs and ensure that they are only performed once.
                                                                                                                        """^
                                                                                                                        end
}
