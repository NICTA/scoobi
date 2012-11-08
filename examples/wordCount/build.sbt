import AssemblyKeys._

assemblySettings

name := "ScoobiWordCount"

version := "1.0"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

libraryDependencies ++= Seq(
   "com.nicta" %% "scoobi" % "0.6.0-cdh3-SNAPSHOT" intransitive(),
   "javassist" % "javassist" % "3.12.1.GA",
   "org.apache.avro" % "avro-mapred" % "1.7.2" % "provided",
   "org.apache.avro" % "avro" % "1.7.2" % "provided",
   "org.apache.hadoop" % "hadoop-core" % "0.20.2-cdh3u1" % "provided",
   "org.scalaz" %% "scalaz-core" % "7.0.0-M3",
   "com.thoughtworks.xstream" % "xstream" % "1.4.3" intransitive()
   )

resolvers ++= Seq("Sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "cloudera" at "https://repository.cloudera.com/content/repositories/releases",
                  "apache"   at "https://repository.apache.org/content/repositories/releases")
