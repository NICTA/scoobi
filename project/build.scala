import sbt._
import Keys._
import com.typesafe.sbt._
import SbtSite._
import SiteKeys._
import SbtGit._
import GitKeys._
import SbtGhPages._
import GhPagesKeys._
import sbtrelease._
import ReleasePlugin._
import ReleaseKeys._
import ReleaseStateTransformations._
import ls.Plugin._
import Utilities._

object build extends Build {
  type Settings = Project.Setting[_]

  lazy val scoobi = Project(
    id = "scoobi",
    base = file("."),
    settings = Defaults.defaultSettings ++
               scoobiSettings           ++
               dependenciesSettings     ++
               compilationSettings      ++
               testingSettings          ++
               siteSettings             ++
               publicationSettings      ++
               notificationSettings     ++
               releaseSettings
  )

  lazy val scoobiSettings: Seq[Settings] = Seq(
    name := "scoobi",
    organization := "com.nicta",
    version := "0.7.1-RELEASE-TRIAL-cdh4",
    scalaVersion := "2.9.2")

  lazy val dependenciesSettings: Seq[Settings] = Seq(
    libraryDependencies := Seq(
      "javassist" % "javassist" % "3.12.1.GA",
      "org.apache.avro" % "avro-mapred" % "1.7.3.1",
      "org.apache.avro" % "avro" % "1.7.3.1",
      "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.0.1",
      "org.apache.hadoop" % "hadoop-core" % "2.0.0-mr1-cdh4.0.1",
      "com.thoughtworks.xstream" % "xstream" % "1.4.3" intransitive(),
      "com.googlecode.kiama" %% "kiama" % "1.4.0",
      "com.github.mdr" %% "ascii-graphs" % "0.0.2",
      "org.scalaz" %% "scalaz-core" % "7.0.0-M7",
      "org.scalaz" %% "scalaz-concurrent" % "7.0.0-M7",
      "org.specs2" %% "specs2" % "1.12.3" % "optional",
      "org.specs2" % "classycle" % "1.4.1"% "test",
      "com.chuusai" %% "shapeless" % "1.2.2",
      "org.scalacheck" %% "scalacheck" % "1.9" % "test",
      "org.scala-tools.testing" % "test-interface" % "0.5" % "test",
      "org.hamcrest" % "hamcrest-all" % "1.1" % "test",
      "org.mockito" % "mockito-all" % "1.9.0" % "optional",
      "org.pegdown" % "pegdown" % "1.0.2" % "test",
      "junit" % "junit" % "4.7" % "test",
      "org.apache.commons" % "commons-math" % "2.2" % "test",
      "org.apache.commons" % "commons-compress" % "1.0" % "test"
    ),
    resolvers ++= Seq("nicta's avro" at "http://nicta.github.com/scoobi/releases",
      "cloudera" at "https://repository.cloudera.com/content/repositories/releases",
      "sonatype" at "http://oss.sonatype.org/content/repositories/snapshots")
  )

  lazy val compilationSettings: Seq[Settings] = Seq(
    (sourceGenerators in Compile) <+= (sourceManaged in Compile) map GenWireFormat.gen,
    scalacOptions ++= Seq("-deprecation", "-Ydependent-method-types", "-unchecked")
  )

  lazy val testingSettings: Seq[Settings] = Seq(
    testOptions := Seq(Tests.Filter(s => s.endsWith("Spec") || Seq("Index", "All", "UserGuide", "ReadMe").exists(s.contains))),
    // run each test in its own jvm
    fork in Test := true,
    javaOptions ++= Seq("-Djava.security.krb5.realm=OX.AC.UK",
                        "-Djava.security.krb5.kdc=kdc0.ox.ac.uk:kdc1.ox.ac.uk",
                        "-Xms3072m",
                        "-Xmx3072m",
                        "-XX:MaxPermSize=768m",
                        "-XX:ReservedCodeCacheSize=1536m")
  )

  lazy val siteSettings: Seq[Settings] = ghpages.settings ++ SbtSite.site.settings ++ Seq(
    siteSourceDirectory <<= target (_ / "specs2-reports"),
    // depending on the version, copy the api files to a different directory
    siteMappings <++= (mappings in packageDoc in Compile, version) map { (m, v) =>
      for((f, d) <- m) yield (f, if (v.trim.endsWith("SNAPSHOT")) ("api/master/" + d) else ("api/SCOOBI-"+v+"/"+d))
    },
    // override the synchLocal task to avoid removing the existing files
    synchLocal <<= (privateMappings, updatedRepository, gitRunner, streams) map { (mappings, repo, git, s) =>
      val betterMappings = mappings map { case (file, target) => (file, repo / target) }
      IO.copy(betterMappings)
      repo
    },
    gitRemoteRepo := "git@github.com:NICTA/scoobi.git"
  )

  lazy val notificationSettings: Seq[Settings] = lsSettings ++ Seq(
    (LsKeys.ghBranch in LsKeys.lsync) := Some("master-publish"),
    (LsKeys.ghUser in LsKeys.lsync) := Some("nicta"),
    (LsKeys.ghRepo in LsKeys.lsync) := Some("scoobi")
  )

  lazy val publicationSettings: Seq[Settings] = Seq(
    publishTo <<= version { v: String =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
      else                             Some("staging" at nexus + "service/local/staging/deploy/maven2")
    },
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { x => false },
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
  )

  /** Release process */
  lazy val releaseSettings = ReleasePlugin.releaseSettings ++ Seq(
    tagName <<= (version in ThisBuild) map (v => "SCOOBI-" + v),
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      updateLicences,
      generateSite,
      inquireVersions,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      publishArtifacts,
      setNextVersion,
      commitNextVersion,
      pushChanges
    )
  )

  lazy val updateLicences = ReleaseStep { st =>
    st.log.info("Updating the license headers")
    "mvn license:format" !! st.log
    commitCurrent("added license headers where missing")(st)
  }

  lazy val generateSite = ReleaseStep { st: State =>
    st.log.info("Generating the documentation")
    val extracted = Project.extract(st)
    val ref: ProjectRef = extracted.get(thisProjectRef)
    val testState = reapply(Seq[Setting[_]](testOptions += Tests.Argument("include", "notfound")), st)
    extracted.runTask(test, testState)._1
  }

  val guideTask = TaskKey[Unit]("guide")

  private def commitCurrent(commitMessage: String): State => State = { st: State =>
    vcs(st).add(".") !! st.log
    val status = (vcs(st).status !!) trim

    if (status.nonEmpty) {
      vcs(st).commit(commitMessage) ! st.log
      st
    } else st
  }

  private def vcs(st: State): Vcs = {
    st.extract.get(versionControlSystem).getOrElse(sys.error("Aborting release. Working directory is not a repository of a recognized VCS."))
  }

}


