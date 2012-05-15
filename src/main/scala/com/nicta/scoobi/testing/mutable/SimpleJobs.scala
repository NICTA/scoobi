package com.nicta.scoobi.testing.mutable

import org.specs2.matcher.{ThrownExpectations, ThrownMessages}
import com.nicta.scoobi.ScoobiConfiguration
import com.nicta.scoobi.testing.TestFiles
import com.nicta.scoobi.Scoobi._


trait SimpleJobs extends ThrownMessages { outer: ThrownExpectations =>

  case class SimpleJob(ts: Seq[String], keepFiles: Boolean = false)(implicit val configuration: ScoobiConfiguration) extends TestFiles {

    /**
     * simply persist the list to an output file and return the lines
     */
    def run: Seq[String] = run[String](identity)

    /**
     * transform the input list, persist it to an output file and return a list of lines
     */
    def run[S : Manifest](transform: DList[String] => DList[S]): Seq[String] = try {
      persist(configuration)(toTextFile(transform(inputLines(ts)), outputPath, overwrite = true))
      if (outputFiles.isEmpty) fail("There are no output files in "+ outputDir.getName)
      outputLines
    } finally { if (!keepFiles) deleteFiles }

  }

  /**
   * @return a simple job from a list of strings (for the input file) and the current configuration
   */
  def fromInput(keepFiles: Boolean)(ts: String*)(implicit c: ScoobiConfiguration) = SimpleJob(ts, keepFiles = keepFiles)
  /**
   * @return a simple job from a list of strings (for the input file) and the current configuration
   */
  def fromInput(ts: String*)(implicit c: ScoobiConfiguration) = SimpleJob(ts)

}