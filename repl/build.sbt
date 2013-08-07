import AssemblyKeys._

assemblySettings

name := "scoobi-repl"

version := "0.8.0-SNAPSHOT"

scalaVersion := "2.10.2"

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-feature",
  "-language:implicitConversions,reflectiveCalls,postfixOps,higherKinds,existentials"
)

libraryDependencies ++= Seq(
  "com.nicta"                         %% "scoobi"                     % "0.8.1-cdh3-SNAPSHOT"  intransitive(),
  "org.scala-lang"                    %  "scala-compiler"             % "2.10.2",
  "org.scala-lang"                    %  "jline"                      % "2.10.2",
  "com.thoughtworks.xstream"          %  "xstream"                    % "1.4.4"           intransitive(),
  "javassist"                         %  "javassist"                  % "3.12.1.GA",
  "com.googlecode.kiama"              %% "kiama"                      % "1.5.1",
  "com.github.mdr"                    %% "ascii-graphs"               % "0.0.3",
  "com.chuusai"                       %% "shapeless"                  % "1.2.4",
  "org.apache.avro"                   %  "avro"                       % "1.7.4",
  "org.apache.avro"                   %  "avro-mapred"                % "1.7.4",
  "org.apache.hadoop"                 %  "hadoop-client"              % "2.0.0-mr1-cdh4.0.1" % "provided",
  "org.apache.hadoop"                 %  "hadoop-core"                % "2.0.0-mr1-cdh4.0.1" % "provided",
  "org.scalaz"                        %% "scalaz-core"                % "7.0.2",
  "org.scalaz"                        %% "scalaz-concurrent"          % "7.0.2",
  "org.scalaz"                        %% "scalaz-scalacheck-binding"  % "7.0.2"           intransitive(),
  "org.scalaz"                        %% "scalaz-typelevel"           % "7.0.2"           intransitive(),
  "org.scalaz"                        %% "scalaz-xml"                 % "7.0.2"           intransitive()
)

resolvers ++= Seq(
  "sonatype"           at "http://oss.sonatype.org/content/repositories/releases",
  "sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { old =>
  {
    case x => {
      val oldstrat = old(x)
      if (oldstrat == MergeStrategy.deduplicate) MergeStrategy.first
      else oldstrat
    }
  }
}
