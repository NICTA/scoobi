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

import com.nicta.scoobi._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.io.text._
import java.io._


object WordCount {

  def main(args: Array[String]) = withHadoopArgs(args) { a =>

    val (inputPath, outputPath) =
      if (a.length == 0) {
        if (!new File("output-dir").mkdir) {
          sys.error("Could not make output-dir for results. Perhaps it already exists (and you should delete/rename the old one)")
        }

        val fileName = "output-dir/all-words.txt"

        // generate 5000 random words (with high collisions) and save at fileName
        generateWords(fileName, 5000)

        (fileName, "output-dir")

      } else if (a.length == 2) {
        (a(0), a(1))
      } else {
        sys.error("Expecting input and output path, or no arguments at all.")
      }

    // Firstly we load up all the (new-line-separated) words into a DList
    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    // What we want to do, is record the frequency of words. So we'll convert it to a key-value
    // pairs where the key is the word, and the value the frequency (which to start with is 1)
    val keyValuePair: DList[(String, Int)] = lines flatMap { _.split(" ") } map { w => (w, 1) }

    // Now let's group all words that compare the same
    val grouped: DList[(String, Iterable[Int])] = keyValuePair.groupByKey
    // Now we have it in the form (Word, ['1', '1', '1', 1' etc.])

    // So what we want to do, is combine all the numbers into a single value (the frequency)
    val combined: DList[(String, Int)] = grouped.combine((_+_))

    // We can evaluate this, and write it to a text file
    DList.persist(TextOutput.toTextFile(combined, outputPath + "/word-results"));
  }

  /* Write 'count' random words to the file 'filename', with a high amount of collisions */
  private def generateWords(filename: String, count: Int) {
    val fstream = new FileWriter(filename)
    val r = new scala.util.Random()

    // we will start off by generating count/10 different "words"
    val words = new Array[String](count / 10)

    (1 to words.length) foreach {
      v => words.update(v-1, randomWord())
    }

    // and now we will pick 'count' of them to write to file
    (1 to count) foreach {
      _ => fstream write ( words(r.nextInt(words.length)) )
    }

    fstream.close()

    // function to make a 5 letter random "word"
    def randomWord() : String = {
      val wordLength = 5;
      val sb = new StringBuilder(wordLength + 1)
      (1 to wordLength) foreach {
        _ => sb.append(('A' + r.nextInt('Z' - 'A')).asInstanceOf[Char])
      }
      sb append('\n')
      sb toString
    }
  }
}
