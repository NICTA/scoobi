seq(sbtavro.SbtAvro.avroSettings : _*)

name := "Scoobi Avro Example"

version := "1.0"

scalaVersion := "2.10.1"

scalacOptions ++= Seq("-deprecation")

resolvers ++= Seq("nicta" at "http://nicta.github.com/scoobi/releases",
                  "sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "cloudera" at "https://repository.cloudera.com/content/repositories/releases",
                  "Radlab Repository" at "http://scads.knowsql.org/nexus/content/groups/public/")

libraryDependencies ++= Seq("com.nicta" %% "scoobi" % "0.7.0-cdh4-SNAPSHOT", 
                            "edu.berkeley.cs" % "avro-plugin_2.10" % "2.1.4.1")

scalacOptions <++= update map { report =>
  val pluginClasspath = report matching configurationFilter(Configurations.CompilerPlugin.name)
  pluginClasspath.map("-Xplugin:" + _.getAbsolutePath).toSeq
}

addCompilerPlugin("edu.berkeley.cs" % "avro-plugin_2.10" % "2.1.4.1" % "plugin")


