import AssemblyKeys._

assemblySettings

name := "FatJarExample"

version := "1.0"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
   "org.scala-lang"           % "scala-compiler" % "2.10.2",
   "com.googlecode.kiama"     %% "kiama"         % "1.5.1",
   "com.nicta"                %% "scoobi"        % "0.7.2" intransitive(),
   "com.chuusai"              %% "shapeless"     % "1.2.4",
   "javassist"                % "javassist"      % "3.12.1.GA",
   "org.apache.avro"          % "avro-mapred"    % "1.7.4" % "provided",
   "org.apache.avro"          % "avro"           % "1.7.4" % "provided",
   "org.apache.hadoop"        % "hadoop-client"  % "2.0.0-mr1-cdh4.0.1" % "provided",
   "org.apache.hadoop"        % "hadoop-core"    % "2.0.0-mr1-cdh4.0.1" % "provided",
   "org.scalaz"               %% "scalaz-core"   % "7.0.2",
   "com.thoughtworks.xstream" % "xstream"        % "1.4.3" intransitive())

resolvers ++= Seq("sonatype releases" at "http://oss.sonatype.org/content/repositories/releases",
                  "sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                  "cloudera" at "https://repository.cloudera.com/content/repositories/releases",
                  "apache"   at "https://repository.apache.org/content/repositories/releases")
