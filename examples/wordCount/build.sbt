name := "Scoobi Word Count"

version := "1.0"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

libraryDependencies += "com.nicta" %% "scoobi" % "0.5.0-cdh3-SNAPSHOT"

resolvers += "Sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
