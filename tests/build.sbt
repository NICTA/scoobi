name := "Scoobi Tests"

version := "0.1"

scalaVersion := "2.9.1"

libraryDependencies += "com.nicta" %% "scoobi" % "0.4.0-SNAPSHOT" % "provided"

libraryDependencies += "org.scala-tools.testing" %% "scalacheck" % "1.9"

mainClass := Some("com.nicta.scoobi.test.Main")
