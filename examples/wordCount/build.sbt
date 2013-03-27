name := "ScoobiWordCount"

version := "1.0"

scalaVersion := "2.10.0"

scalacOptions ++= Seq("-deprecation")

libraryDependencies ++= Seq("com.nicta" %% "scoobi" % "0.7.0-cdh4-SNAPSHOT")

resolvers ++= Seq(//"nicta's avro" at "http://nicta.github.com/scoobi/releases",
                  "sonatype releases" at "http://oss.sonatype.org/content/repositories/releases",
                  "sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "cloudera" at "https://repository.cloudera.com/content/repositories/releases")
