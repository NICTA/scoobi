name := "Scoobi Word Count"

version := "1.0"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

libraryDependencies += "com.nicta" %% "scoobi" % "0.4.0-SNAPSHOT" % "provided"

resolvers += "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
