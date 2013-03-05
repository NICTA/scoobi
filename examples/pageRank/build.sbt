name := "PageRank"

version := "1.0"

scalaVersion := "2.9.2"

libraryDependencies ++= 
  Seq("com.nicta" %% "scoobi" % "0.6.2-cdh4") 

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

resolvers += "Sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
