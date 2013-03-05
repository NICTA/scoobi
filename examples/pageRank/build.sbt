name := "PageRank"

version := "1.0"

scalaVersion := "2.9.2"

libraryDependencies ++= 
  Seq("com.nicta" %% "scoobi" % "0.7.0-cdh4-SNAPSHOT") 

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

resolvers += "Sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
