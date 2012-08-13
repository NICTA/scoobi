/** Definition */
name := "scoobi"

organization := "com.nicta"

version := "0.5.0-SNAPSHOT"

scalaVersion := "2.9.2"

libraryDependencies ++= Seq(
  "javassist" % "javassist" % "3.12.1.GA",
  "org.apache.avro" % "avro-mapred" % "1.7.0",
  "org.apache.avro" % "avro" % "1.7.0",
  "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.0.0",
  "org.apache.hadoop" % "hadoop-core" % "2.0.0-mr1-cdh4.0.0",
  "com.thoughtworks.xstream" % "xstream" % "1.4.2",
  "org.scalaz" %% "scalaz-core" % "6.95",
  "org.specs2" %% "specs2" % "1.12" % "optional",
  "org.specs2" % "classycle" % "1.4.1"% "test",
  "org.scalacheck" %% "scalacheck" % "1.9" % "test",
  "org.scala-tools.testing" % "test-interface" % "0.5" % "test",
  "org.hamcrest" % "hamcrest-all" % "1.1" % "test",
  "org.mockito" % "mockito-all" % "1.9.0" % "optional",
  "org.pegdown" % "pegdown" % "1.0.2" % "test",
  "junit" % "junit" % "4.7" % "test",
  "org.apache.commons" % "commons-math" % "2.2" % "test"
)

resolvers ++= Seq("cloudera" at "https://repository.cloudera.com/content/repositories/releases",
                  "apache"   at "https://repository.apache.org/content/repositories/releases",
                  "scoobi"   at "http://nicta.github.com/scoobi/releases")

/** Compilation */
scalacOptions ++= Seq("-deprecation", "-Ydependent-method-types", "-unchecked")

javaOptions += "-Xmx2G"

/** Testing */
testOptions := Seq(Tests.Filter(s => s.endsWith("Spec") ||
                                     Seq("Index", "All", "UserGuide", "ReadMe").exists(s.contains)))

fork in Test := true

publishArtifact in packageDoc := false // disable building docs, as it takes so much time

pomExtra :=
    <build>
        <plugins>
             <plugin>
                <groupId>com.mycila.maven-license-plugin</groupId>
                <artifactId>maven-license-plugin</artifactId>
                <configuration>
                    <header>notes/header.txt</header>
                </configuration>
            </plugin>
        </plugins>
    </build>

