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

import com.nicta.scoobi._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.io.text._
import java.io._
import com.nicta.scoobi.io.sequence.SequenceFileInput
import org.apache.hadoop.io.Text

object SequenceExample {

  def main(args: Array[String]) = withHadoopArgs(args) { a =>
    val lines: DList[(Text, Text)] = SequenceFileInput.fromSequenceFile[Text, Text]("dw_users_info")
    val l2 = lines.map { case (k, v) => { println(k, v); v.toString } }

    // We can evalute this, and write it to a text file
    DList.persist(TextOutput.toTextFile(l2, "out/word-results"))
  }
}
