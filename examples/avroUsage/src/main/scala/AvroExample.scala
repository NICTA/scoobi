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
 // import edu.berkeley.cs.avro.marker._
 // import edu.berkeley.cs.avro.runtime._
 import java.io.{DataOutput,DataInput}


 // case class Book(var title : String, var author : Option[String], var year : Option[Int], var other : Option[String]) 
 // extends AvroRecord 


 object WordCount extends ScoobiApp {



    def run() {

    // implicit def bookFmt : WireFormat[Book] = new AvroRecordWireFormat[Book](Book.apply, Book.unapply _)
    // val book = Book("The Title", "Some Guy", 1970, "hardback")
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
    
    val mylist = fromAvroFile[Book](outputDirectory)
    val strings = mylist.map{book => book.title.toString}
    persist(toTextFile(strings,"text_output", true))
}


}

