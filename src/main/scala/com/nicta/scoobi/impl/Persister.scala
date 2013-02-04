package com.nicta.scoobi
package impl

import org.apache.commons.logging.LogFactory
import exec.{HadoopMode, InMemoryMode}
import core.{DObject, DList, ScoobiConfiguration, Persistent}
import plan.comp._
import core.Mode._

class Persister(sc: ScoobiConfiguration) {
  private implicit val configuration = sc
  private implicit lazy val logger = LogFactory.getLog("scoobi.Persister")

  private val inMemoryMode = InMemoryMode()
  private val hadoopMode   = HadoopMode(sc)

  def persist[A](ps: Seq[Persistent[_]]) = {
    val asOne = Root(ps.map(_.getComp))
    sc.mode match {
      case InMemory        => inMemoryMode.execute(asOne)
      case Local | Cluster => hadoopMode  .execute(asOne)
    }
    ps
  }

  def persist[A](list: DList[A]) = {
    sc.mode match {
      case InMemory        => inMemoryMode.execute(list)
      case Local | Cluster => hadoopMode  .execute(list)
    }
    list
  }

  def persist[A](o: DObject[A]): A = {
    sc.mode match {
      case InMemory        => inMemoryMode.execute(o).asInstanceOf[A]
      case Local | Cluster => hadoopMode  .execute(o).asInstanceOf[A]
    }
  }
}
