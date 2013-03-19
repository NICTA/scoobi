package com.nicta.scoobi
package core

import java.util.UUID

/** Marks a Sink as a possible checkpoint in the computations */
trait Checkpoint { outer: Sink =>
  private var name: Option[String] = None
  /** set this sink as a checkpoint */
  def checkpoint(implicit sc: ScoobiConfiguration) = {
    outer.name =
      if (outputPath.isDefined) outputPath.map(_.toString)
      else                      Some(UUID.randomUUID().toString)
    this
  }
  /** @return true if this Sink is a checkpoint */
  def isCheckpoint: Boolean = name.isDefined
  /** @return true if this Sink is a checkpoint */
  def checkpointName: Option[String] = name
  /** @return true if this Sink has been filled with data */
  def checkpointExists(implicit sc: ScoobiConfiguration): Boolean = {
    isCheckpoint && outputPath.exists(p => sc.fileSystem.exists(p) && Option(sc.fileSystem.listStatus(p)).map(_.nonEmpty).getOrElse(false))
  }
}

