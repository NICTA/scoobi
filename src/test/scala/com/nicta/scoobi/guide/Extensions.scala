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

class Extensions extends ScoobiPage { def is = "Extensions".title ^
                                                                                                                        """

### Introduction
  
Scoobi includes a growing set of reusable components, ready for use in your application. They are located `com.nicta.scoobi.lib`, take a look at the API documentation to get a real sense on how to use them.

### Grouping

Understanding how `Grouping` works is important before moving on to use the Scoobi libraries. This is because the libraries use the grouping in order to determain how values are grouped. While normally the defaults are very sane, in some case they may not be what you want. For instance, if you're doing a full outer join, where you're grouping based on an `Option[T]` it is quite likely you would not `None`s to be grouped (and cartesian product expanded) together. So to do this, you would specify a Grouping implicit that will make two `None`'s not group together.

In the future, this will be more explicit and some convience ones will be provided for you out of the box.
  
### Joins
  
  [Background Reading](http://en.wikipedia.org/wiki/Join_%28relational_algebra%29#Joins_and_join-like_operators)

  Often you two collections, and would like to collate them somehow. While you might expect the logic to be around a `DList[A]`, with a `DList[B]` and predicate `(A, B) => Bool` to create `DList[(A, B)]` -- it would not be possible to do (remotely) efficiently in this manner, and be rather cumbersome. So instead our join code is structured around having a `DList[(K, A)]` and a `DList[(K, B)]` where the `K` has to have an `Ordering` available (a requirement for `DList.groupByKey`)
  
  From this, you generally end up with a `DList[(K, (A, B)]`.
  
  In addition to a normal (equi)join, often you want to do something for a `K` when theres no corresponding values in the left (`A`) or right (`B`) DList. This forms the basis of left, right, inner and outter joins. See: [wikipedia](http://en.wikipedia.org/wiki/Join_%28SQL%29) for an almost tutorial like walkthrough of the different join types.  
  
### Cogroup
  
  Sometimes when dealing a `DList[(K, (A, B)]` where there's multiple K's, its easier to work with in the form `DList[(K, (Iterable[A], Iterable[B]))]` where each K is unique. If this is what you need, it's a lot more efficient to use `CoGroup` then a relational join, followed by a `.groupByKey`
  
### DMatrix
  
  `DMatrix[Elem, V]` provides you with a distributed matrix currently backed by a coordinate oriented DList ( `DList[((Elem, Elem), V)]` ) which makes it ideal for storing huge sparse matrixes. When multiplying by vectors that can fit in memory, it often makes sense to convert it to a RowWise or ColWise matrix and use it from there. There are implicit conversions that can happen, but remember the conversion isn't cheap -- so try save the result if you're going to keep using it (e.g. doing an iterative algorithm with the same matrix)
  
### DVector
  
  `DVector[Elem, V]` is in the same spirit of a DMatrix, it's backed by a coordinate form DList ( `DList[(Elem, V)]` ) ideal for huge and spare vectors.

### In Memory Vector

  `InMemVector[Elem, V]` and `InMemDenseVector[V]` are for vectors that can fit into memory. They are based around a `DObject` which means a copy  gets efficiently sent to each mapper. For things like Matrix by Vector operations, this can result in a huge speedup. The obvious catch is, they need to fit into memory.

                                                                                                                        """

}
