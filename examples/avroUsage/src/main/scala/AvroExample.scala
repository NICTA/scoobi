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
package com.nicta.scoobi.examples

import com.ebay.ss.qs.phrases.BooksToAvro.Book
import com.nicta.scoobi.Scoobi._

object WordCount extends ScoobiApp {
  
  def run() {
    

    val book = new Book()
    book.title = "The Title"
    book.author = "Some Guy"
    book.year = 1970
    book.other = "hardback"
      
    val books = Seq(book)
    
    val booklist = books.toDList
    
    val outputDirectory = "avro_output"
    val inputDirectory = "avro_input"
    
    persist(toAvroFile[Book](booklist, outputDirectory, true))
    
    val mylist = fromAvroFile[Book](inputDirectory)
    val strings = mylist.map{book => book.getTitle.toString}
    persist(toTextFile(strings,"text_output"))
  }

  
}

