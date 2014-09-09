seq(sbtavro.SbtAvro.avroSettings:_*)

name := "Scoobi Avro Example"

version := "0.8.5"

// Make sure to sync this with a stable release of Scoobi or else weird errors
// like "java.lang.ClassNotFoundException: Class scala.runtime.Nothing when running ..."
// will occur!
scalaVersion := "2.10.4"

resolvers ++= Seq("nicta" at "http://nicta.github.io/scoobi/releases",
                  "sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "cloudera" at "https://repository.cloudera.com/content/repositories/releases",
                  "sbt-plugin-releases" at "http://repo.scala-sbt.org/scalasbt/sbt-plugin-releases",
                  //Below fixes the "org.scala-tools#vscaladoc;1.1-md3: not found" error:
                  "scala-tools" at "http://repo.typesafe.com/typesafe/akka-releases-cache")

// Below timesout with error, not sure what it is needed for:
//                  "Radlab Repository" at "http://scads.knowsql.org/nexus/content/groups/public/"

libraryDependencies ++= Seq("com.nicta" %% "scoobi" % "0.8.5",
                            "org.scala-tools" % "vscaladoc" % "1.1-md-3")

scalacOptions <++= update map { report =>
  val pluginClasspath = report matching configurationFilter(Configurations.CompilerPlugin.name)
  pluginClasspath.map("-Xplugin:" + _.getAbsolutePath).toSeq
}
