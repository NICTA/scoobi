name := "PageRank"

version := "1.0"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

libraryDependencies += "com.nicta" %% "scoobi" % "0.6.1-cdh4"

resolvers += "Sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
