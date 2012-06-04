name := "Scoobi Word Count"

version := "1.0"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

libraryDependencies += "com.nicta" %% "scoobi" % "0.4.0-SNAPSHOT" % "provided"

resolvers ++= Seq("Cloudera Maven Repository" at "https://repository.cloudera.com/content/repositories/releases/",
                  "Packaged Avro" at "http://nicta.github.com/scoobi/releases/",
                  "Sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots")
