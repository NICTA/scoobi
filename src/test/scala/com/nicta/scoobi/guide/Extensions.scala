package com.nicta.scoobi.guide

class Extensions extends ScoobiPage { def is = "Extensions".title ^
"""
Scoobi includes a growing set of reusable components, ready to use in your application. They are in scoobi.lib, take a look at the API documentation
to get a real sense on how to use them. 
  
### Joins
  
  [Background Reading](http://en.wikipedia.org/wiki/Join_%28relational_algebra%29#Joins_and_join-like_operators)

  Often you two collections, and would like to collate them somehow. While you might expect the logic to be around a `DList[A]`
  a `DList[B]` and predicate `(A, B) => Bool` to create `DList[(A, B)]` -- it would not be possible to do (remotely) efficiently
  in this manner, and be rather cumbersome. So instead our join code is structured around having a `DList[(K, A)]` and a `DList[(K, B)]`
  where the K has to have an `Ordering` available (a requirement for `.groupByKey`)
  
  From this, you generally end up with a `DList[(K, (A, B)]`.
  
  In addition to a normal (equi)join, often you want to do something if there's no corresponding values in the first (left) or second (right)
  dlist. These are the basis of left, right, inner and outter joins. See: [wikipedia](http://en.wikipedia.org/wiki/Join_%28SQL%29) for an almost
  tutorial like walkthrough of the different join types.  
  
### Cogroup
  
  Sometimes when dealing a `DList[(K, (A, B))` where there's multiple K's, its nicer to have it form `DList[(K, (Iterable[A], Iterable[B]))]` so that
  each K is unique. If Cogroup is what you need, it's a lot more efficient to use it then a relational join, followed by a `.groupByKey`
  
### DMatrix
  
  `DMatrix[Elem, V]` provides you with a distributed matrix currently backed by a coordinate oriented DList ( `DList[((Elem, Elem), V)]` ) which makes it
  ideal for storing huge sparse matrixes. When multiplying by vectors that can fit in memory, it often makes sense to convert it to a RowWise or ColWise matrix
  and use it from there. There are implicit conversions if the conversion only needs to once
  
### DVector
  
  `DVector[Elem, V]` is in the same spirit of a DMatrix, backed by a coordinate form DList ( `DList[(Elem, V)]` ) ideal for huge and spare vectors.

### In Memory Vector
  `InMemVector[Elem, V]` and `InMemDenseVector[V]` are for vectors that can fit into memory. They are based around a `DObject` which means a copy
  gets efficiently sent to each mapper. For things like Matrix by Vector operations, this can result in a huge speedup. The obvious catch is, they
  need to fit into memory.


"""

}
