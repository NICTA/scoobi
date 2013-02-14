name := "ScoobiWordCount"

version := "1.0"

scalaVersion := "2.9.2"

// replace this line with scoobiVersion := "0.7.0-cdh4" or another Scoobi version
scoobiVersion <<= scoobiVersion in GlobalScope 

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

libraryDependencies <+= (scoobiVersion) { version => "com.nicta" %% "scoobi" % version }

resolvers ++= Seq("nicta's avro" at "http://nicta.github.com/scoobi/releases",
                  "sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "cloudera" at "https://repository.cloudera.com/content/repositories/releases")
