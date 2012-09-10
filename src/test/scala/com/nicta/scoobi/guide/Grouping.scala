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

class Grouping extends ScoobiPage { def is = "Grouping".title ^
                                                                                                                        """

The sort and shuffle phase of MapReduce is abstracted by `DList.groupByKey`, it allows us to have a `DList[(K, V)]` and obtain a `DList[(K, Iterable[V])]` where all the values *"with the same"* K are grouped together. Grouping is a scala trait, that defines exactly what constitutes *"with the same K"* means.

### The Grouping trait

The [Grouping](${SCOOBI_API_PAGE}]#com.nicta.scoobi.Grouping) type class is automatically provided for anything with a `scala.math.Ordering`, or that implements Java's `java.lang.Comparable` interface. This means all common types (e.g. String, Int etc.) can be grouped out of the box. If you have a more complex type (or complex grouping requirements) you will need to write some code to group by the type. The three options are:

 * Provide a [scala.math.Ordering](http://www.scala-lang.org/api/milestone/scala/math/Ordering.html) for your type.
 * Make your type extend [java.lang.Comparable](http://docs.oracle.com/javase/6/docs/api/java/lang/Comparable.html).
 * Directly provide a [scoobi.Grouping](${SCOOBI_API_PAGE}]#com.nicta.scoobi.Grouping).

Since the third option is the only one Scoobi specific, and a little more powerful, we'll focus on that.

### Basic grouping

Let's say we have a user-defined type, `case class Point(x: Integer, y: Integer)` and are using it in a `DList[(Point, String)]`. Now if want to group all the same points together, so as to end up with a: `DList[(Point, Iterable[String])` we will need to write a function that defines what makes two points equal. It would not be enough to merely provide a pure equality function, as that would require every point to be check against every point (quadratic time).

Instead, we need to provide strict total ordering. That is, provide a function that can reliably create an ordering for Points. It doesn't really matter how this ordering is done, as long as it is. The scheme normally used is to break the type down into all its members, and provide ordering based on it. So in the `Point` case, we can order by `Point.x`, if they're the same, order by `Point.y` and finally, if they're the same -- we can say the two points are the same.

The `Grouping[T]` trait has a method called `sortCompare` that we will override. It takes two T's, and like Java's `Comparable` it returns an Int. A negative number to indicate the first `T` is less than the second. Zero if the two `T`s are equal. And a positive number if the second `T` is after the first `T`.

Sample code for our `Point`:

    implicit val pointGrouping = new Grouping[Point] {
      override def sortCompare(first: Point, second: Point) {
        val xResult = first.x.compareTo(second.x)
        if (xResult != 0)
          xResult
        else {
          val yResult = first.y.compareTo(second.y)
          if (yResult != 0) yResult
          else              0
        }
      }
    }

### Secondary sort

Assuming you have read and understood the previous sections, we will move on to some advanced usage of Grouping by example. Let's start some type alias's to make the code more understandable:

    type FirstName = String
    type LastName = String

And let's start with a DList with some easily understandable data:

    val names: DList[(FirstName, LastName)] = DList.apply(
      ("Michael", "Jackson"),
      ("Leonardo", "Da Vinci"),
      ("John", "Kennedy"),
      ("Mark", "Twain"),
      ("Bat", "Man"),
      ("Michael", "Jordan"),
      ("Mark", "Edison"),
      ("Michael", "Landon"),
      ("Leonardo", "De Capro"),
      ("Michael", "J. Fox"))

Based on the previous sections, you should know that we could simply do `names.groupByKey` to obtain a `DList[(FirstName, Iterable[String])]. However, there's no ordering associated with the `Iterable[LastName]` this means, if order is required (e.g. we're processing a time series) or outputting the last names in alphabetical order -- we'd have to use a parallelDo to load the entire reducers collection to memory, then sort it there. This is both slow, and going to likely to use too much memory.

So this is where a "Secondary Sort", comes into play. A secondary sort allows us to make sure the `Iterable[LastName]` comes to us in a defined ordering, so we can efficiently process it.

In hadoop (and thus scoobi) sorting only happens on the Key, so what we need to do, is make our Key contain enough information to sort the last names for any given first name.

Your first instinct might be to *move* the information to the key, e.g. make the type: `DList[((FirstName, LastName), Unit)]` however this will *not* work. We need to instead *duplicate* the information to the key, otherwise while things arrive in the correct order, it will not be possible to get the value!

    val bigKey: DList[((FirstName, LastName), LastName)] = names.map(a => ((a._1, a._2), a._2))

Now the key part of `bigKey` is `(FirstName, LastName)` so this is what we need to provide a `Grouping` object for. Our goal is to make sure that two keys with the same FirstName evaluate to being the same, so `groupCompare` and `partition` should only consider the FirstName part. However, `sortCompare` should put everything in the correct order (so it should consider both parts).

    implicit val grouping = new Grouping[(FirstName, LastName)] {

      override def partition(key: (FirstName, LastName), howManyReducers: Int): Int = {
        // This function says what reducer this particular 'key' should go to. We must override the
        // default impl, because it looks at the entire key, and makes sure all the same
        // keys go to the same reducer. But we want to only 'look' at the 'FirstName' part
        // so that everything with the same FirstName goes to the same reducer (even if it has a different LastName)

        // So we'll just use the default (string) partition, and only on the first name
        implicitly[Grouping[FirstName]].partition(key._1, howManyReducers)
      }


      override def sortCompare(a: (FirstName, LastName), b: (FirstName, LastName)): Int = {
        // Ok, here's where the fun is! Everything that is sent to the same reducer now needs
        // an ordering. So this function is called. Here we return -1 if 'a' should be before 'b',
        // and 0 if they're they same, and 1 if they're different.

        // So the first thing we want to do, is look at first names

        val firstNameOrdering = a._1.compareTo(b._1)

        firstNameOrdering match {
          case 0 => {
            // Interesting! Here the firstName's are the same. So what we want to do, is order by
            // the lastNames

            a._2.compareTo(b._2)
          }
          case x => x // otherwise, just return the result for which of the FirstName's is first
        }
      }

      override def groupCompare(a: (FirstName, LastName), b: (FirstName, LastName)): Int = {
        // So now everything going to the reducer has a proper ordering (thanks to our 'sortCompare' function)
        // now hadoop allows us to "collapse" everything that is logically the same. So two keys are logically
        // the same if the FirstName's are equal
        a._1.compareTo(b._1)
      }

    }

Now calling `bigKey.groupByKey` will work as intended, with all lastNames arriving in order.

                                                                                                                        """
}
