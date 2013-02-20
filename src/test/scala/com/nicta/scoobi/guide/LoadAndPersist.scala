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

import testing.mutable.NictaSimpleJobs
import Scoobi._
import com.nicta.scoobi.impl.plan.comp.CompNodeData._

class LoadAndPersist extends ScoobiPage { def is = "Load and persist data".title^
  """
`DList` objects are merely nodes in a graph describing a series of data computation we want to perform. However, at some point we need to specify what the inputs and outputs to that computation are. In the [WordCount example](Application.html) we simply use in memory data and we print out the result of the computations. However the data used by Hadoop jobs is generally *loaded* from files and the results *persisted* to files. Let's see how to specify this.

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


  """ ^ end


}

