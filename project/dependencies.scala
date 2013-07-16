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
  lazy val dependencies = libraryDependencies <<= (version, scalaVersion) { (version, scalaVersion) =>
    scoobi(scalaVersion) ++
    hadoop(version)      ++
    scalaz()             ++
    specs2()
  }

  // Libraries
  def scoobi(scalaVersion: String) = Seq(
    "org.scala-lang"                    %  "scala-compiler"            % scalaVersion,
    "org.apache.avro"                   %  "avro"                      % "1.7.4",
    "com.thoughtworks.xstream"          %  "xstream"                   % "1.4.4"            intransitive(),
    "javassist"                         %  "javassist"                 % "3.12.1.GA",
    "com.googlecode.kiama"              %% "kiama"                     % "1.5.1",
    "com.github.mdr"                    %% "ascii-graphs"              % "0.0.3",
    "com.chuusai"                       %% "shapeless"                 % "1.2.4",
    "org.apache.commons"                %  "commons-math"              % "2.2"              % "test",
    "org.apache.commons"                %  "commons-compress"          % "1.0"              % "test")

  def hadoop(version: String) =
    if (version.contains("cdh3")) Seq("org.apache.hadoop" % "hadoop-core"   % "0.20.2-cdh3u1",
                                      "org.apache.avro"   % "avro-mapred"   % "1.7.4")
    else                          Seq("org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.0.1" exclude("asm", "asm"),
                                      "org.apache.hadoop" % "hadoop-core"   % "2.0.0-mr1-cdh4.0.1",
                                      "org.apache.avro"   % "avro-mapred"   % "1.7.4" classifier "hadoop2")

  def scalaz(scalazVersion: String = "7.0.0") = Seq(
    "org.scalaz"                        %% "scalaz-core"               % scalazVersion,
    "org.scalaz"                        %% "scalaz-concurrent"         % scalazVersion,
    "org.scalaz"                        %% "scalaz-scalacheck-binding" % scalazVersion intransitive(),
    "org.scalaz"                        %% "scalaz-typelevel"          % scalazVersion intransitive(),
    "org.scalaz"                        %% "scalaz-xml"                % scalazVersion intransitive())

  def specs2(specs2Version: String = "2.1") = Seq(
    "org.specs2"                        %% "specs2"                    % specs2Version      % "optional",
    "org.scalacheck"                    %% "scalacheck"                % "1.10.0"           % "optional",
    "org.mockito"                       %  "mockito-all"               % "1.9.0"            % "optional",
    "org.pegdown"                       %  "pegdown"                   % "1.2.1"            % "test",
    "org.scala-tools.testing"           %  "test-interface"            % "0.5"              % "test",
    "org.hamcrest"                      %  "hamcrest-all"              % "1.1"              % "test",
    "org.specs2"                        %  "classycle"                 % "1.4.1"            % "test",
    "junit"                             %  "junit"                     % "4.7"              % "test")


  lazy val resolversSettings = resolvers ++= Seq(
    "cloudera"           at "https://repository.cloudera.com/content/repositories/releases",
    "sonatype-releases"  at "http://oss.sonatype.org/content/repositories/releases",
    "sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots")
}
