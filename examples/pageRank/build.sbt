name := "PageRank"

version := "1.0"

scalaVersion := "2.9.2"

// replace this line with scoobiVersion := "0.7.0-cdh4" or another Scoobi version
scoobiVersion <<= scoobiVersion in GlobalScope 

libraryDependencies <+= (scoobiVersion) { "com.nicta" %% "scoobi" % _ }

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

resolvers += "Sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
