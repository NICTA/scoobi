/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.SequenceFileInputFormat
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapred.SequenceFileOutputFormat


/** A data store represents data that is exterrnal to an MSCRs. As a consequence it is
  * external to a Hadoop job which means it must be perisisted somewhere, at least
  * temporarily, between jobs. There are three kinds: Inputs, Outputs and
  * Bridges. */
sealed abstract class DataStore(val node: AST.Node[_]) {

  /* The name to be given to the type of this connector. Two different types can
   * not share the same name. */
  val typeName: String = "V" + node.id
}


/* An input data store to a MapReduce job. */
sealed trait DataSource {
  def inputTypeName: String
  def inputPath: Path
  def inputFormat: Class[_ <: FileInputFormat[_,_]]
}


/* An output store from a MapReduce job. */
sealed trait DataSink {
  def outputTypeName: String
  def outputPath: Path
  def outputFormat: Class[_ <: FileOutputFormat[_,_]]
}


/** An input store is synonomous with a 'Load' node. It already exists and
  * must persist. */
abstract class InputStore(n: AST.Load[_]) extends DataStore(n) with DataSource


/** An output store is data that must first be computed. Once computed it
  * must persist. A single output channel can have multiple output stores. */
abstract class OutputStore(n: AST.Node[_]) extends DataStore(n) with DataSink


/** A bridge store is any data that moves between MSCRs. It must first be computed, but
  * may be removed once all successor MSCRs have consumed it. */
final case class BridgeStore(n: AST.Node[_], val path: Path) extends DataStore(n) with DataSource with DataSink {

  def inputTypeName = typeName
  val inputPath = new Path(path, "ch*")
  val inputFormat = classOf[SequenceFileInputFormat[_,_]]

  def outputTypeName = typeName
  val outputPath = path
  val outputFormat = classOf[SequenceFileOutputFormat[_,_]]

  /** Free up the disk space being taken up by this intermediate data. */
  def freePath: Unit = {
    val fs = outputPath.getFileSystem(Scoobi.conf)
    fs.delete(outputPath)
  }
}

/** Companion object for automating the creation of random temporary paths as the
  * location for bridge stores. */
object BridgeStore {

  import scala.collection.mutable.{Map => MMap}

  private object TmpId extends UniqueInt

  def apply(node: AST.Node[_]): BridgeStore = {
    val tmpPath = new Path(Scoobi.getWorkingDirectory, "bridges/" + TmpId.get.toString)
    BridgeStore(node, tmpPath)
  }

  def getFromMMap(n: AST.Node[_], m: MMap[AST.Node[_], BridgeStore]): BridgeStore = {
    m.get(n) match {
      case Some(bs) => bs
      case None     => {
        val newBS: BridgeStore = BridgeStore(n)
        m += ((n, newBS))
        newBS
      }
    }
  }


}
