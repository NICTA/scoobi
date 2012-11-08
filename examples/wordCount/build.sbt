import AssemblyKeys._

assemblySettings

name := "ScoobiWordCount"

version := "1.0"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

libraryDependencies ++= Seq(
   "com.nicta" %% "scoobi" % "0.6.0-cdh4-SNAPSHOT" intransitive(),
   "javassist" % "javassist" % "3.12.1.GA",
   "org.apache.avro" % "avro-mapred" % "1.7.3-SNAPSHOT" % "provided",
   "org.apache.avro" % "avro" % "1.7.3-SNAPSHOT" % "provided",
   "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.0.1" % "provided",
   "org.apache.hadoop" % "hadoop-core" % "2.0.0-mr1-cdh4.0.1" % "provided",
   "org.scalaz" %% "scalaz-core" % "7.0.0-M3",
   "com.thoughtworks.xstream" % "xstream" % "1.4.3" intransitive()
   )

resolvers ++= Seq("nicta's avro" at "http://nicta.github.com/scoobi/releases",
                  "cloudera" at "https://repository.cloudera.com/content/repositories/releases",
                  "apache"   at "https://repository.apache.org/content/repositories/releases")
