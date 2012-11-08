name := "Scoobi Avro Example"

version := "1.0"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation")

resolvers += "Radlab Repository" at "http://scads.knowsql.org/nexus/content/groups/public/"

libraryDependencies += "com.nicta" %% "scoobi" % "0.6.0-cdh3-SNAPSHOT"

libraryDependencies += "edu.berkeley.cs" %% "avro-plugin" % "2.1.4-SNAPSHOT"

scalacOptions <++= update map { report =>
  val pluginClasspath = report matching configurationFilter(Configurations.CompilerPlugin.name)
  pluginClasspath.map("-Xplugin:" + _.getAbsolutePath).toSeq
}

addCompilerPlugin("edu.berkeley.cs" %% "avro-plugin" % "2.1.4-SNAPSHOT" % "plugin")



