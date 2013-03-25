import AssemblyKeys._

assemblySettings

name := "FatJarExample"

version := "1.0"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")


libraryDependencies ++= Seq(
  "com.nicta" %% "scoobi" % "0.7.0-cdh4-SNAPSHOT" intransitive(),
  "javassist" % "javassist" % "3.12.1.GA",
  "org.apache.avro" % "avro-mapred" % "1.7.3.1",
  "org.apache.avro" % "avro" % "1.7.3.1",
  "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.0.1",
  "org.apache.hadoop" % "hadoop-core" % "2.0.0-mr1-cdh4.0.1",
  "com.thoughtworks.xstream" % "xstream" % "1.4.3" intransitive(),
  "com.googlecode.kiama" %% "kiama" % "1.4.0",
  "com.github.mdr" %% "ascii-graphs" % "0.0.2",
  "org.scalaz" %% "scalaz-core" % "7.0.0-M9",
  "org.scalaz" %% "scalaz-concurrent" % "7.0.0-M9",
  "org.specs2" %% "specs2" % "1.12.4" % "optional",
  "org.specs2" % "classycle" % "1.4.1"% "test",
  "com.chuusai" %% "shapeless" % "1.2.2")

resolvers ++= Seq("nicta's avro" at "http://nicta.github.com/scoobi/releases",
                  "sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "cloudera" at "https://repository.cloudera.com/content/repositories/releases",
                  "apache"   at "https://repository.apache.org/content/repositories/releases")
