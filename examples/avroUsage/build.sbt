name := "Scoobi Avro Example"

version := "1.0"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

scalacOptions <++= update map { report =>
  val pluginClasspath = report matching configurationFilter(Configurations.CompilerPlugin.name)
    pluginClasspath.map("-Xplugin:" + _.getAbsolutePath).toSeq
}

libraryDependencies += "com.nicta" %% "scoobi" % "0.6.1-cdh3-SNAPSHOT"

resolvers    += "Radlab Repository" at "http://scads.knowsql.org/nexus/content/groups/public/"

resolvers += "Sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "edu.berkeley.cs" %% "avro-plugin" % "2.1.4-SNAPSHOT"

addCompilerPlugin("edu.berkeley.cs" %% "avro-plugin" % "2.1.4-SNAPSHOT" % "plugin")


