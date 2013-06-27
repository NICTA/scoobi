name := "PageRank"

version := "1.0"

scalaVersion := "2.10.1"

libraryDependencies ++= 
  Seq("com.nicta" %% "scoobi" % "0.7.0-cdh4") 

scalacOptions ++= Seq("-deprecation")

resolvers ++= Seq("sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "cloudera" at "https://repository.cloudera.com/content/repositories/releases")
