name := "PageRank"

version := "1.0"

scalaVersion := "2.10.1"

libraryDependencies ++= 
  Seq("com.nicta" %% "scoobi" % "0.7.0-cdh4-SNAPSHOT") 

scalacOptions ++= Seq("-deprecation")

resolvers ++= Seq("nicta" at "http://nicta.github.io/scoobi/releases",
                  "sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "cloudera" at "https://repository.cloudera.com/content/repositories/releases")
