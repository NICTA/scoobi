package com.nicta.scoobi
package acceptance

import java.io.{OutputStream, FileInputStream, FileOutputStream, File}
import java.util.zip.GZIPOutputStream
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.GzipCodec
import org.specs2.matcher.Matcher

import testing.{TestFiles, TempFiles, NictaSimpleJobs}
import impl.io.FileSystems

import impl.control.Exceptions._
import Scoobi._

class CompressionSpec extends NictaSimpleJobs with CompressedFiles {

  "INPUTS".txt
  "gzipped files can be used as an input to a Scoobi job, just using fromTextFile" >> { implicit sc: SC =>
    val lines: DList[String] = writeToTempFile("text", Seq.fill(5)("hello"))(".gz")
    lines.run must have size(5)
  }

  "bzip2 files can be used as an input to a Scoobi job, just using fromTextFile" >> { implicit sc: SC =>
    val lines: DList[String] = writeToTempFile("text", Seq.fill(5)("hello"))(".bz2")
    lines.run must have size(5)
  }

  "OUTPUTS".newp
  "gzipped files can be used as an output to a Scoobi job, just using toTextFile and specifying a codec" >> { implicit sc: SC =>
    val list = DList.fill(5)(1)
    val resultDir = TestFiles.createTempDir("result")
    list.toTextFile(outputPath(resultDir)).compressWith(new GzipCodec).run

    copyResults(resultDir) must containFiles(".gz")
  }

  "it is possible to compress outputs independently" >> { implicit sc: SC =>
    val (list1, list2) = (DList.fill(5)(1), DList.fill(5)(2))

    val (resultDir1, resultDir2) = (TestFiles.createTempDir("result1"), TestFiles.createTempDir("result2"))

    list1.toTextFile(outputPath(resultDir1)).compress.run
    list2.toTextFile(outputPath(resultDir2)).run

    copyResults(resultDir1) must containFiles(".gz")
    copyResults(resultDir2) must notContainFiles(".gz")
  }

  "INTERMEDIATE".newp
  "Mapper outputs can also be compressed" in pending
  "Reducer outputs can also be compressed" in pending


  def containFiles(extension: String): Matcher[File] = (resultDir: File) =>  {
    resultDir.list must not (beEmpty)
    resultDir.listFiles.toSeq.filter(_.getName.matches("ch.*"+extension)) must not (beEmpty)
  }
  def notContainFiles(extension: String): Matcher[File] = (resultDir: File) =>  {
    resultDir.list must not (beEmpty)
    resultDir.listFiles.toSeq.filter(_.getName.matches("ch.*"+extension)) must beEmpty
  }
  def containFilesWithNoExtension: Matcher[File] = (resultDir: File) =>  {
    resultDir.list must not (beEmpty)
    resultDir.listFiles.toSeq.filter(_.getName.matches("ch[^\\.]*")) must not(beEmpty)
  }
  def copyResults(resultDir: File)(implicit sc: SC) = {
    if (sc.isRemote) {
      sc.fileSystem.listStatus(new Path(resultDir.getName)).foreach { f =>
        sc.fileSystem.copyToLocalFile(f.getPath, new Path(resultDir.getPath))
      }
    }
    resultDir
  }

  private def outputPath(resultDir: File)(implicit sc: ScoobiConfiguration): String =
    if (sc.isRemote) resultDir.getName
    else             resultDir.getPath
}

trait CompressedFiles {
  def writeToTempFile[T](prefix: String, values: Seq[T])(suffix: String)(implicit sc: ScoobiConfiguration): DList[String] = {
    // create text file
    val textFile = TempFiles.createTempFile(prefix)
    TempFiles.writeLines(textFile, values.map(_.toString), sc.isRemote)(FileSystems.fileSystem)

    // compress it if necessary
    val filePath =
      if      (Seq(".gz", ".gzip").contains(suffix)) gzCompressFile(textFile)
      else if (Seq(".bz2").contains(suffix))         bz2(textFile)
      else                                           textFile.getPath

    fromTextFile(filePath)
  }

  /**
   * @return the path of a GZIP compressed file with ".gz" appended to the original file path
   */
  def gzCompressFile(file: File)(implicit sc: ScoobiConfiguration): String =
    compressFile(file, ".gz", new GZIPOutputStream(_))

  /**
   * @return the path of a BZ2 compressed file with ".bz2" appended to the original file path
   */
  def bz2(file: File)(implicit sc: ScoobiConfiguration): String =
    compressFile(file, ".bz2", new BZip2CompressorOutputStream(_))

  /**
   * @return the path of a compressed file with "suffix" appended to the original file path
   */
  private def compressFile(file: File, suffix: String, compressor: OutputStream => OutputStream { def finish() })(implicit sc: ScoobiConfiguration): String = {
    tryo {
      val outputFile = new File(file.getPath+suffix)
      val out = compressor(new FileOutputStream(outputFile.getPath))
      val in = new FileInputStream(file.getPath)
      var buf = new Array[Byte](1024)
      var len = in.read(buf)
      while (len > 0) {
        out.write(buf, 0, len)
        len = in.read(buf)
      }
      in.close; out.finish; out.close;
      if (sc.isRemote) {
        val path = sc.workingDirectory+outputFile.getName
        FileSystems.fileSystem.copyFromLocalFile(true, new Path(outputFile.getPath), new Path(path))
        FileSystems.deleteFiles(file.getName)
        path
      } else outputFile.getPath
    }((e:Exception) => println("error compressing a "+suffix+" file: "+e.getMessage)).getOrElse(file.getPath)
  }
}