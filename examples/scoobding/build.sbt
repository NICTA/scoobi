name := "scoobding"

version := "1.0"

scalaVersion := "2.10.1"

scalacOptions ++= Seq("-deprecation")

libraryDependencies ++= 
  Seq("com.nicta" %% "scoobi" % "0.7.0-cdh4",
      "org.scala-lang" % "scala-swing" % "2.9.2",
      "jfree" % "jfreechart" % "1.0.13",
      "jfree" % "jcommon" % "1.0.16",
      "cc.co.scala-reactive" %% "reactive-core" % "0.3.0") 


resolvers ++= Seq("sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "cloudera" at "https://repository.cloudera.com/content/repositories/releases")
