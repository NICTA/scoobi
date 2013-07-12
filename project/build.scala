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
import complete.DefaultParsers._
import Keys._
import com.typesafe.sbt._
import pgp.PgpKeys._
import sbt.Configuration
import Defaults._

object build extends Build {
  type Settings = Project.Setting[_]

  lazy val scoobi = Project(
    id = "scoobi",
    base = file("."),
    settings = Defaults.defaultSettings ++
               scoobiSettings           ++
               dependencies.settings    ++
               compilationSettings      ++
               testingSettings          
  ) 

  lazy val scoobiVersion = SettingKey[String]("scoobi-version", "defines the current Scoobi version")
  lazy val scoobiSettings: Seq[Settings] = Seq(
    name := "scoobi",
    organization := "com.nicta",
    scoobiVersion in GlobalScope <<= version,
    scalaVersion := "2.10.2")

  lazy val compilationSettings: Seq[Settings] = Seq(
    (sourceGenerators in Compile) <+= (sourceManaged in Compile) map GenWireFormat.gen,
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:implicitConversions,reflectiveCalls,postfixOps,higherKinds,existentials"),
    scalacOptions in Test ++= Seq("-Yrangepos")
  )

  lazy val testingSettings: Seq[Settings] = Seq(
    testOptions := Seq(Tests.Filter(s => s.endsWith("Spec") || s.contains("guide") || Seq("Index", "All", "UserGuide", "ReadMe").exists(s.contains))),
    // run each test in its own jvm
    fork in Test := true,
    javaOptions in Test ++= Seq("-Xmx1g")
  )

  lazy val publicationSettings: Seq[Settings] = Seq(
    publishTo <<= version { v: String =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
      else                             Some("staging" at nexus + "service/local/staging/deploy/maven2")
    },
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { x => false },
    pomExtra := (
      <url>http://nicta.github.io/scoobi</url>
      <licenses>
        <license>
          <name>Apache 2.0</name>
          <url>http://www.opensource.org/licenses/Apache-2.0</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>http://github.com/NICTA/scoobi</url>
        <connection>scm:http:http://NICTA@github.com/NICTA/scoobi.git</connection>
      </scm>
      <developers>
        <developer>
          <id>blever</id>
          <name>Ben Lever</name>
          <url>http://github.com/blever</url>
        </developer>
         <developer>
          <id>espringe</id>
          <name>Eric Springer</name>
          <url>http://github.com/espringe</url>
        </developer>
        <developer>
          <id>etorreborre</id>
          <name>Eric Torreborre</name>
          <url>http://etorreborre.blogspot.com/</url>
        </developer>
      </developers>
    ),
    credentials := Seq(Credentials(Path.userHome / ".sbt" / "scoobi.credentials"))
  )

  /**
   * EXAMPLE PROJECTS
   */
  def project(name: String) = Project(id = name, base = file("examples/"+name))
  lazy val pageRank  = project("pageRank")
  lazy val scoobding = project("scoobding")
  lazy val wordCount = project("wordCount")
  lazy val cdh4Version = (s: String) => { if (!(s contains "-cdh4")) (s+"-cdh4") else s }
}


