import AssemblyKeys._

assemblySettings

name := "FatJarExample"

version := "1.0"

scalaVersion := "2.10.1"

scalacOptions ++= Seq("-deprecation")


libraryDependencies ++= Seq("com.nicta" %% "scoobi" % "0.7.0-cdh4")

resolvers ++= Seq("sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "cloudera" at "https://repository.cloudera.com/content/repositories/releases",
                  "apache"   at "https://repository.apache.org/content/repositories/releases")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
    case entry => {
      val strategy = mergeStrategy(entry)
      if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
      else strategy
    }
  }
}
