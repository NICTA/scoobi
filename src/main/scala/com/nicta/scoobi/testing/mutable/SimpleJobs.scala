package com.nicta.scoobi
package testing
package mutable

import org.specs2.matcher.{ThrownExpectations, ThrownMessages}
import application._
import core._

/**
 * This trait helps in the creation of DLists and Scoobi jobs where the user doesn't have to track the creation of files.
 * All data is written to temporary files and is deleted after usage.
 */
trait SimpleJobs extends ThrownMessages { outer: ThrownExpectations with LocalHadoop =>

  implicit def testInputToSimpleJob[T](input: InputTestFile[T])
                                      (implicit configuration: ScoobiConfiguration, m: Manifest[T], w: WireFormat[T]): SimpleJob[T] =
    SimpleJob(input.lines, input.keepFile)

  case class SimpleJob[T](list: DList[T], keepFiles: Boolean = false)
                         (implicit val configuration: ScoobiConfiguration, m: Manifest[T], w: WireFormat[T]) {
    /**
     * simply persist the list to an output file and return the lines
     */
    def run: Seq[String] = outer.run(list, keepFiles)

    /**
     * transform the input list, persist it to an output file and return a list of lines
     */
    def run[S : Manifest : WireFormat](transform: DList[T] => DList[S]): Seq[String] = outer.run(transform(list), keepFiles)
  }

  def run[T, S](lists: =>(DList[T], DList[S]))
               (implicit configuration: ScoobiConfiguration, m1: Manifest[T], w1: WireFormat[T], m2: Manifest[S], w2: WireFormat[S]): (Seq[String], Seq[String]) =
    run(lists, keepFiles = false)

  def run[T, S](lists: =>(DList[T], DList[S]), keepFiles: Boolean)
               (implicit configuration: ScoobiConfiguration, m1: Manifest[T], w1: WireFormat[T], m2: Manifest[S], w2: WireFormat[S]): (Seq[String], Seq[String]) =
    runWithTestFiles {
      OutputTestFiles(lists._1, lists._2).lines match {
        case Left(msg)     => { if (!quiet) println(msg); (Seq[String](), Seq[String]()) }
        case Right(result) => result
      }
    }

  def run[T](list: =>DList[T], keepFiles: Boolean = false)
            (implicit configuration: ScoobiConfiguration, m: Manifest[T], w: WireFormat[T]): Seq[String] =
    runWithTestFiles {
      OutputTestFile(list, keepFiles).lines match {
        case Left(msg)     => { if (!quiet) println(msg); Seq[String]() }
        case Right(result) => result
      }
    }

  private def runWithTestFiles[T](t: =>T)(implicit configuration: ScoobiConfiguration) = try {
    t
  } finally {
    if (!keepFiles) TestFiles.deleteFiles
  }

  /**
   * @return a simple job from a list of strings (for the input file) and the current configuration
   */
  def fromInput(ts: String*)(implicit c: ScoobiConfiguration) = InputStringTestFile(ts)
  /**
   * @return a DList input keeping track of its temporary input file
   */
  def fromDelimitedInput(ts: String*)(implicit c: ScoobiConfiguration) = new InputTestFile[List[String]](ts, mapping = (_:String).split(",").toList)

}
