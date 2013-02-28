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
package examples

import Scoobi._
import core.Reduction._

/**
 * This application is duplicated from the examples directory to ease testing from within sbt. Run with:
 *
 * `test:run-main com.nicta.scoobi.examples.WordCount`
 */
object WordCount extends ScoobiApp {

  def run() {
    // Firstly we load up all the (new-line-separated) words into a DList
    val lines: DList[String] =
      if (args.length == 0)
        DList(generateWords(5000): _*)
      else if (args.length == 2)
        fromTextFile(args(0))
      else
        sys.error("Expecting input and output path, or no arguments at all.")

    // Now what we want to do, is record the frequency of words. So we'll convert it to a key-value
    // pairs where the key is the word, and the value the frequency (which to start with is 1)
    val keyValuePair: DList[(String, Int)] = lines mapFlatten { _.split(" ") } map { w => (w, 1) }

    // Now let's group all words that compare the same
    val grouped: DList[(String, Iterable[Int])] = keyValuePair.groupByKey
    // Now we have it in the form (Word, ['1', '1', '1', 1' etc.])

    // So what we want to do, is combine all the numbers into a single value (the frequency)
    val combined: DList[(String, Int)] = grouped.combine(Sum.int)

    val outputDirectory: String = if (args.length == 0) "word-count-results" else args(1)

    // We can evaluate this, and write it to a text file
    persist(combined.toTextFile(outputDirectory))
  }

  /* Generate 'count' random words with a high amount of collisions */
  private def generateWords(count: Int) = {
    val r = new scala.util.Random()

    // function to make a 5 letter random "word"
    def randomWord() : String = {
      val wordLength = 5
      val sb = new StringBuilder(wordLength)
      (1 to wordLength) foreach {
        _ => sb.append(('A' + r.nextInt('Z' - 'A')).asInstanceOf[Char])
      }
      sb toString
    }

    // we start off by generating count/10 different "words"
    var words: IndexedSeq[String] =
      for (i <- 1 to count/10)
      yield randomWord()

    // and now we will pick 'count' of them to output
    for (i <- 1 to count)
    yield words(r.nextInt(words.length))
  }
}
