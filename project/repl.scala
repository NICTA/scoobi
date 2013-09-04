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
import sbt._
import Keys._

import sbtassembly.Plugin._
import AssemblyKeys._

object repl {
  type Settings = Def.Setting[_]

  lazy val Repl = config("repl")

  lazy val settings: Seq[Settings] =
    inConfig(Repl)(assemblySettings) ++
    inConfig(Repl)(Defaults.projectBaseSettings) ++
    repl

  lazy val deps =
    libraryDependencies in Repl :=
      dependencies.scoobi(scalaVersion.value)                ++
      dependencies.hadoop(version.value).map(_ % "provided") ++
      dependencies.scalaz()                                  ++
      dependencies.specs2()                                  ++
      dependencies.repl

  lazy val repl = Seq(
    deps,
    fullClasspath in (Repl, assembly)   <<= (update in Repl, exportedProducts in Compile) map (jars),
    jarName       in (Repl, assembly)   <<= (version in ThisBuild) map { v => s"scoobi-repl-$v.jar" },
    test          in (Repl, assembly)   :=  {},
    mergeStrategy in (Repl, assembly)   <<= mergeStrategy in (Repl, assembly) apply (merge)
  )

  def jars(report: UpdateReport, main: Classpath) =
    report.select((_: String) == "runtime").distinct.map(Attributed.blank) ++ main

  def merge(old: String => MergeStrategy): String => MergeStrategy =
    x => {
      val oldstrat = old(x)
      if (oldstrat == MergeStrategy.deduplicate) MergeStrategy.first  else oldstrat
    }
}
