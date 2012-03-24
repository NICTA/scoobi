/**
  * Copyright 2011 National ICT Australia Limited
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
name := "scoobi"

organization := "com.nicta"

version := "0.3.0"

scalaVersion := "2.9.1"

libraryDependencies ++= Seq(
  "com.odiago.avro" % "odiago-avro" % "1.0.5",
  "javassist" % "javassist" % "3.12.1.GA",
  "org.apache.hadoop" % "hadoop-core" % "0.20.2-cdh3u1",
  "org.apache.avro" % "avro-mapred" % "1.6.0",
  "com.thoughtworks.xstream" % "xstream" % "1.4.2"
)

publishArtifact in packageDoc := false

compileOrder := CompileOrder.ScalaThenJava

resolvers += "Cloudera Maven Repository" at "https://repository.cloudera.com/content/repositories/releases/"

resolvers += "Packaged Avro" at "http://nicta.github.com/scoobi/releases/"

scalacOptions ++= Seq("-deprecation", "-Ydependent-method-types")

javacOptions ++= Seq("-Xlint:deprecation", "-Xlint:unchecked")
