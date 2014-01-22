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

object dependencies {

  lazy val settings = dependencies ++ resolversSettings
  lazy val dependencies = libraryDependencies ++=
    scoobi(scalaVersion.value) ++
    hadoop(version.value)      ++
    scalaz()             ++
    specs2()

  // Libraries
  def scoobi(scalaVersion: String) = Seq(
    "org.scala-lang"                    %  "scala-compiler"            % scalaVersion,
    "org.apache.avro"                   %  "avro"                      % "1.7.4",
    "com.thoughtworks.xstream"          %  "xstream"                   % "1.4.4"            intransitive(),
    "javassist"                         %  "javassist"                 % "3.12.1.GA",
    "com.googlecode.kiama"              %% "kiama"                     % "1.5.2",
    "com.github.mdr"                    %% "ascii-graphs"              % "0.0.3",
    "com.chuusai"                       %  "shapeless_2.10.2"          % "2.0.0-M1",
    "org.apache.commons"                %  "commons-math"              % "2.2"              % "test",
    "org.apache.commons"                %  "commons-compress"          % "1.0"              % "test")

  def hadoop(version: String, hadoopVersion: String = "2.2.0") =
    if (version.contains("hadoop2"))   Seq("org.apache.hadoop" % "hadoop-common"                     % hadoopVersion,
                                           "org.apache.hadoop" % "hadoop-hdfs"                       % hadoopVersion,
                                           "org.apache.hadoop" % "hadoop-mapreduce-client-app"       % hadoopVersion,
                                           "org.apache.hadoop" % "hadoop-mapreduce-client-core"      % hadoopVersion,
                                           "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion,
                                           "org.apache.hadoop" % "hadoop-mapreduce-client-core"      % hadoopVersion,
                                           "org.apache.hadoop" % "hadoop-annotations"                % hadoopVersion,
                                           "org.apache.avro"   % "avro-mapred"                       % "1.7.4" classifier "hadoop2")

    else if (version.contains("cdh3")) Seq("org.apache.hadoop" % "hadoop-core"   % "0.20.2-cdh3u1",
                                           "org.apache.avro"   % "avro-mapred"   % "1.7.4")

    else                               Seq("org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.0.1" exclude("asm", "asm"),
                                           "org.apache.hadoop" % "hadoop-core"   % "2.0.0-mr1-cdh4.0.1",
                                           "org.apache.avro"   % "avro-mapred"   % "1.7.4" classifier "hadoop2")

  def scalaz(scalazVersion: String = "7.0.2") = Seq(
    "org.scalaz"                        %% "scalaz-core"               % scalazVersion,
    "org.scalaz"                        %% "scalaz-concurrent"         % scalazVersion,
    "org.scalaz"                        %% "scalaz-scalacheck-binding" % scalazVersion intransitive(),
    "org.scalaz"                        %% "scalaz-typelevel"          % scalazVersion intransitive(),
    "org.scalaz"                        %% "scalaz-xml"                % scalazVersion intransitive())

  def specs2(specs2Version: String = "2.3.2") = Seq(
    "org.specs2"                        %% "specs2-core"               % specs2Version      % "optional") ++ Seq(
    "org.specs2"                        %% "specs2-mock"               % specs2Version      ,
    "org.specs2"                        %% "specs2-scalacheck"         % specs2Version      ,
    "org.specs2"                        %% "specs2-junit"              % specs2Version      ,
    "org.specs2"                        %% "specs2-html"               % specs2Version      ,
    "org.specs2"                        %% "specs2-analysis"           % specs2Version      ).map(_ % "test")

  def repl = Seq(
    "org.scala-lang"                    %  "jline"                     % "2.10.2"
  )

  lazy val resolversSettings = resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snaspshots"),
    "cloudera"             at "https://repository.cloudera.com/content/repositories/releases",
    "hortonworks-releases" at "http://repo.hortonworks.com/content/repositories/releases")
}
