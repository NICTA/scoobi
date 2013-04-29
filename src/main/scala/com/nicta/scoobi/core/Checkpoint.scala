package com.nicta.scoobi
package core

import java.util.UUID

/** store the output path of a Sink as a checkpoint */
case class Checkpoint(name: String)

object Checkpoint {
  def create(path: Option[String], doIt: Boolean)(implicit sc: ScoobiConfiguration) =
    if (doIt) Some(Checkpoint(path.map(_.toString).getOrElse(UUID.randomUUID().toString))) else None
}