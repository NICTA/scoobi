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


object NumberPartitioner {

  def main(args: Array[String]) = withHadoopArgs(args) { _ =>

    if (!new File("output-dir").mkdir) {
      sys.error("Could not make output-dir for results. Perhaps it already exists (and you should delete/rename the old one)")
    }

    val fileName = "output-dir/all-ints.txt"

    // Write 50 (new line seperated) ints to a file. We do this to make the example self contained
    generateInts(fileName, 50)

    // fromTextFile creates a list of Strings, where each String is a line
    val data : DList[String] = TextInput.fromTextFile(fileName);

    // since they're numbers, we can easily parse them
    val intData : DList[Int] = TextInput.fromTextFile(fileName).map(_.toInt)

    // Now we can parition this data into two lists, one where they're even one where they're odd
    val (evens, odds) = intData.partition(_ % 2 == 0)

    DList.persist (
      TextOutput.toTextFile(evens, "output-dir/evens"),
      TextOutput.toTextFile(odds,  "output-dir/odds")
    )
  }

  private def generateInts(filename: String, count: Int) {
    val fstream = new FileWriter(filename)
    val r = new scala.util.Random()
    (1 to count) foreach { _ => fstream write ( r.nextInt(count * 2).toString ++ "\n" ) }
    fstream.close()
  }
}
