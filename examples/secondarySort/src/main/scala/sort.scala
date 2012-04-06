/**
  * Copyright 2011 National ICT Australia Limited
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
package com.nicta.scoobi.examples

import com.nicta.scoobi.Scoobi._

object SeconarySort extends ScoobiApp {
	// Problem: Let's start some first and second names

  type FirstName = String
  type LastName = String

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
    ("Michael", "J. Fox")
    )


  // let's say I want to group everyone with the same first name, I could simply do:
  // names.groupByKey


  // The problem is, however, there's no ordering associated with the Iterable[LastName]
  // this means, if order is required (e.g. we're processing a time series) or outputting
  // the last names in alphabetical order -- we'd have to use a parallelDo to load the entire
  // reducers collection to memory, then sort it there. This is both slow, and going to likely
  // use too much memory.

  // look at tmp-out/names to see the un-orderness of it all
  


  // So our solution is to do a "Secondary Sort", this is exposed with Scoobi's Grouping[K]
  // In hadoop (and thus scoobi) a seconary sort can only happen on the Key, so what we need
  // to do, is make our Key contain enough information to sort the last names for any given
  // first name. 

  val bigKey: DList[((FirstName, LastName), LastName)] = names.map(a => ((a._1, a._2), a._2))

  // So if we started with ("Jonny", "Cash"), new key is (("Jonny", "Cash"), "Cash")

  // So we have duplicated enough into the key to do our sort, while making sure the value is
  // still useful. (Your first instinct might be to make the DList of type DList[((FirstName, LastName), Unit)])
  // but this will *not* work. As while you'll get your stuff in-order, you won't know what the actual value is!

  // So here is where the magic happens:
  // create a new grouping method for anything with the key (FirstName, LastName) // Warning: this is just a Tuple2[String, String]
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

      firstNameOrdering match  {
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

  val data: DList[((FirstName, LastName), Iterable[LastName])] = bigKey.groupByKey // scala's implicit magic picks up
  // the 'Grouping[(FirstName, LastName)'

  // and our data dlist's Iterable[LastName] has ordering! Normally at this point, you'd go through
  // it with a function that relies on it's order. 

  // the key thing to keep in mind, is the LastName in the *Key* is almost totally useless
  // because of the way the grouping has "collapsed" things. So even though the type of 'data' is
  // a 'DList[((FirstName, LastName), Iterable[LastName])]' you should treat it as if its type was:
  // DList[((FirstName, Unit), Iterable[LastName])]


  // writing this to disk, so you can make sure it meets your expectations!

  persist(toTextFile(data, "tmp-out/secondary-sort", true)) // write to 

}
