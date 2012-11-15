import com.jsuereth.sbtsite.SiteKeys._
import com.jsuereth.git.{GitKeys,GitRunner}
import GitKeys.{gitBranch, gitRemoteRepo}
import com.jsuereth.ghpages.GhPages.ghpages._

/** Definition */
name := "scoobi"

organization := "com.nicta"

version := "0.6.0-cdh3-RC1"

scalaVersion := "2.9.2"

libraryDependencies ++= Seq(
  "javassist" % "javassist" % "3.12.1.GA",
  "org.apache.avro" % "avro-mapred" % "1.7.2",
  "org.apache.avro" % "avro" % "1.7.2",
  "org.apache.hadoop" % "hadoop-core" % "0.20.2-cdh3u1",
  "com.thoughtworks.xstream" % "xstream" % "1.4.3" intransitive(),
  "org.scalaz" %% "scalaz-core" % "7.0.0-M3",
  "org.specs2" %% "specs2" % "1.12.3" % "optional",
  "com.chuusai" %% "shapeless" % "1.2.2",
  "org.specs2" % "classycle" % "1.4.1"% "test",
  "org.scalacheck" %% "scalacheck" % "1.9" % "test",
  "org.scala-tools.testing" % "test-interface" % "0.5" % "test",
  "org.hamcrest" % "hamcrest-all" % "1.1" % "test",
  "org.mockito" % "mockito-all" % "1.9.0" % "optional",
  "org.pegdown" % "pegdown" % "1.0.2" % "test",
  "junit" % "junit" % "4.7" % "test",
  "org.apache.commons" % "commons-math" % "2.2" % "test",
  "org.apache.commons" % "commons-compress" % "1.0" % "test"
)

(sourceGenerators in Compile) <+= (sourceManaged in Compile) map GenWireFormat.gen

resolvers ++= Seq("cloudera" at "https://repository.cloudera.com/content/repositories/releases",
                  "sonatype" at "http://oss.sonatype.org/content/repositories/snapshots")

/** Compilation */
scalacOptions ++= Seq("-deprecation", "-Ydependent-method-types", "-unchecked")

/** Testing */
testOptions := Seq(Tests.Filter(s => s.endsWith("Spec") ||
                                     Seq("Index", "All", "UserGuide", "ReadMe").exists(s.contains)))

fork in Test := true

javaOptions ++= Seq("-Djava.security.krb5.realm=OX.AC.UK",
                    "-Djava.security.krb5.kdc=kdc0.ox.ac.uk:kdc1.ox.ac.uk")

publishArtifact in packageDoc := false // disable building docs, as it takes so much time


/** Publishing */
publishTo <<= version { v: String =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
  else                             Some("staging" at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { x => false }

pomExtra := (
  <url>http://nicta.github.com/scoobi</url>
  <licenses>
    <license>
      <name>Apache 2.0</name>
      <url>http://www.opensource.org/licenses/Apache-2.0</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>http://github.com/NICTA/scoobi</url>
    <connection>scm:http:http://NICTA@github.com/NICTA/scoobi.git</connection>
  </scm>
  <developers>
    <developer>
      <id>blever</id>
      <name>Ben Lever</name>
      <url>http://github.com/blever</url>
    </developer>
     <developer>
      <id>espringe</id>
      <name>Eric Springer</name>
      <url>http://github.com/espringe</url>
    </developer>
    <developer>
      <id>etorreborre</id>
      <name>Eric Torreborre</name>
      <url>http://etorreborre.blogspot.com/</url>
    </developer>
  </developers>
)

/** Site building */
site.settings

seq(site.settings:_*)

siteSourceDirectory <<= target (_ / "specs2-reports")

// depending on the version, copy the api files to a different directory
siteMappings <++= (mappings in packageDoc in Compile, version) map { (m, v) =>
  for((f, d) <- m) yield (f, if (v.trim.endsWith("SNAPSHOT")) ("api/master/" + d) else ("api/SCOOBI-"+v+"/"+d))
}

/** Site publication */
seq(ghpages.settings:_*)

// override the synchLocal task to avoid removing the existing files
synchLocal <<= (privateMappings, updatedRepository, GitKeys.gitRunner, streams) map { (mappings, repo, git, s) =>
  val betterMappings = mappings map { case (file, target) => (file, repo / target) }
  IO.copy(betterMappings)
  repo
}

git.remoteRepo := "git@github.com:NICTA/scoobi.git"

/** Notification */
seq(lsSettings :_*)

(LsKeys.ghBranch in LsKeys.lsync) := Some("cdh3-publish")

(LsKeys.ghUser in LsKeys.lsync) := Some("nicta")

(LsKeys.ghRepo in LsKeys.lsync) := Some("scoobi")

