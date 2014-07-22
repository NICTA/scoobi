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
    "com.googlecode.kiama"              %% "kiama"                     % "1.6.0",
  if (scalaVersion.contains("2.10"))
    "com.chuusai"                       %  s"shapeless_$scalaVersion"  % "2.0.0"
  else
    "com.chuusai"                       %% "shapeless"                 % "2.0.0",
    "org.apache.commons"                %  "commons-math"              % "2.2"              % "test",
    "org.apache.commons"                %  "commons-compress"          % "1.0"              % "test")

  def hadoop(version: String, hadoopVersion: String = "2.2.0") =

    if (version.contains("cdh3"))      Seq("com.nicta" %% "scoobi-compatibility-cdh3"    % "1.0.2")
    else if (version.contains("cdh4")) Seq("com.nicta" %% "scoobi-compatibility-cdh4"    % "1.0.2")
    else if (version.contains("cdh5")) Seq("com.nicta" %% "scoobi-compatibility-cdh5"    % "1.0.2")
    else                               Seq("com.nicta" %% "scoobi-compatibility-hadoop2" % "1.0.2")

  def scalaz(scalazVersion: String = "7.0.6") = Seq(
    "org.scalaz"                        %% "scalaz-core"               % scalazVersion,
    "org.scalaz"                        %% "scalaz-iteratee"           % scalazVersion,
    "org.scalaz"                        %% "scalaz-concurrent"         % scalazVersion,
    "org.scalaz"                        %% "scalaz-scalacheck-binding" % scalazVersion intransitive(),
    "org.scalaz"                        %% "scalaz-typelevel"          % scalazVersion intransitive(),
    "org.scalaz"                        %% "scalaz-xml"                % scalazVersion intransitive())

  def specs2(specs2Version: String = "2.3.12") = Seq(
    "org.specs2"                        %% "specs2-core"               % specs2Version      % "optional") ++ Seq(
    "org.specs2"                        %% "specs2-mock"               % specs2Version      ,
    "org.specs2"                        %% "specs2-scalacheck"         % specs2Version      ,
    "org.specs2"                        %% "specs2-junit"              % specs2Version      ,
    "org.specs2"                        %% "specs2-html"               % specs2Version      ,
    "org.specs2"                        %% "specs2-analysis"           % specs2Version      ).map(_ % "test")

  def repl(scalaVersion: String) = Seq(
    if (scalaVersion.contains("2.10"))
    "org.scala-lang"                    %  "jline"                     % scalaVersion
    else
    "jline"                             %  "jline"                     % scalaVersion.split("\\.").take(2).mkString(".")
  )

  lazy val resolversSettings = resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots"),
    "cloudera"             at "https://repository.cloudera.com/content/repositories/releases",
    "hortonworks-releases" at "http://repo.hortonworks.com/content/repositories/releases")
}
