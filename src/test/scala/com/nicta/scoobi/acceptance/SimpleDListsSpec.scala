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
package acceptance

import testing.NictaSimpleJobs
import com.nicta.scoobi.Scoobi._

class SimpleDListsSpec extends NictaSimpleJobs {
  override def context = local
  override def keepFiles = true
  override def quiet = false
  override def level = application.level("ALL")

  args.select(ex = "combine")

  "load" >> { implicit sc: SC =>
    DList("hello").run === Seq("hello")
  }

  "load + map" >> { implicit sc: SC =>
    DList("hello").map(_.size).run === Seq(5)
  }

  "load + map + groupByKey" >> { implicit sc: SC =>
    DList((1, "hello"), (1, "world")).groupByKey.run === Seq((1, Seq("hello", "world")))
  }

  "load + map + groupByKey + combine" >> { implicit sc: SC =>
    DList((1, "hello"), (1, "world")).groupByKey.combine((_:String)+(_:String)).run must be_==(Seq((1, "helloworld"))) or be_==(Seq((1, "worldhello")))
  }

}
