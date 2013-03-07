seq(sbtavro.SbtAvro.avroSettings : _*)

name := "Scoobi Avro Example"

version := "1.0"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

resolvers ++= Seq("nicta's avro" at "http://nicta.github.com/scoobi/releases",
                  "sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "cloudera" at "https://repository.cloudera.com/content/repositories/releases",
                  "Radlab Repository" at "http://scads.knowsql.org/nexus/content/groups/public/")

libraryDependencies ++= Seq("com.nicta" %% "scoobi" % "0.7.0-cdh4-SNAPSHOT", 
                            "edu.berkeley.cs" %% "avro-plugin" % "2.1.4-SNAPSHOT")

scalacOptions <++= update map { report =>
  val pluginClasspath = report matching configurationFilter(Configurations.CompilerPlugin.name)
  pluginClasspath.map("-Xplugin:" + _.getAbsolutePath).toSeq
}

addCompilerPlugin("edu.berkeley.cs" %% "avro-plugin" % "2.1.4-SNAPSHOT" % "plugin")


