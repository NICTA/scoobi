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
package impl
package plan
package comp

import org.specs2.specification.AllExpectations
import testing.mutable.UnitSpecification

class GraphNodesSpec extends UnitSpecification with AllExpectations {

  /**
   * this graph is used for the following examples
   *
   *         ld
   *         ^
   *        /
   *       pd1
   *       ^ ^
   *      /   \
   *    gbk1   \
   *     ^      \
   *      \      \
   *        pds1
   *        ^
   *        |
   *       mat1
   */

  /**
   * root, descendents, parents, uses, transitiveUses
   */
  "from any given node it is possible to get the root of the graph" >> new nodes {
    forall(Seq(l1, pd1, gbk1, pds1, mat1)) { n => root(n) ==== mat1 }
  }
  "the parents of a node are all its direct parents" >> new nodes {
    (pd1 -> parents) ==== Vector(pds1, mat1)
    (l1 -> parents)  ==== Vector(pd1, pds1, mat1)
  }
  "the descendents of a node is the recursive list of all children" >> new nodes {
    (mat1 -> descendents) ==== Vector(pds1, gbk1, pd1, pds1.env, l1, pd1.env)
  }
  "the uses of a node are all the nodes having this node in their children" >> new nodes {
    (pds1 -> uses) ==== Set(mat1)
    (pd1 -> uses) ==== Set(pds1, gbk1)
    (l1 -> uses)  ==== Set(pd1)
  }
  "the transitiveUses of a node are all the nodes having this node in their descendents" >> new nodes {
    (pds1 -> transitiveUses) ==== Set(mat1)
    (pd1 -> transitiveUses) ==== Set(pds1, gbk1, mat1)
    (l1 -> transitiveUses)  ==== Set(pd1, pds1, gbk1, mat1)
  }

  "two nodes related by a common input are not strict parents" >> new factory {
    val ld1 = load
    val (pd1, pd2) = (pd(ld1), pd(ld1))
    val (gbk1, gbk2) = (gbk(pd1), gbk(pd2))
    (gbk1 -> isStrictParentOf(gbk2)) === false
    (gbk2 -> isStrictParentOf(gbk1)) === false
  }

}
