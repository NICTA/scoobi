name := "ScoobiWordCount"

version := "1.0"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

libraryDependencies += "com.nicta" %% "scoobi" % "0.6.0-cdh4-SNAPSHOT"

resolvers ++= Seq("nicta's avro" at "http://nicta.github.com/scoobi/releases",
                  "cloudera" at "https://repository.cloudera.com/content/repositories/releases",
                  "apache"   at "https://repository.apache.org/content/repositories/releases")
