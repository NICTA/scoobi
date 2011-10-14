name := "scoobi"

version := "0.0.1"

scalaVersion := "2.9.1"

libraryDependencies ++= Seq(
  "javassist" % "javassist" % "3.12.1.GA",
  "org.apache.hadoop" % "hadoop-core" % "0.20.2-cdh3u1"
)

resolvers += "Cloudera Maven Repository" at "https://repository.cloudera.com/content/repositories/releases/"
