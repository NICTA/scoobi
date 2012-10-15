/** Definition */
name := "scoobi"

organization := "com.nicta"

version := "0.6.0-cdh4-SNAPSHOT"

scalaVersion := "2.10.0-RC1"

crossVersion := CrossVersion.full

libraryDependencies ++= Seq(
  "javassist" % "javassist" % "3.12.1.GA",
  "org.apache.avro" % "avro-mapred" % "1.7.0",
  "org.apache.avro" % "avro" % "1.7.0",
  "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.0.0",
  "org.apache.hadoop" % "hadoop-core" % "2.0.0-mr1-cdh4.0.0",
  "com.thoughtworks.xstream" % "xstream" % "1.4.3" intransitive(),
  "com.googlecode.kiama" % "kiama_2.10.0-RC1" % "1.4.0-B2",
//  "com.github.mdr" % "ascii-graphs_2.10.0-RC1" % "0.0.2",
  "org.scalaz" % "scalaz-core_2.10.0-M7" % "7.0.0-M3",
  "org.specs2" % "specs2_2.10.0-RC1" % "1.12.2" % "optional",
  "org.specs2" % "classycle" % "1.4.1"% "test",
  "org.scalacheck" % "scalacheck_2.10.0-RC1" % "1.10.0" % "test",
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
                  "apache"   at "https://repository.apache.org/content/repositories/releases",
                  "scoobi"   at "http://nicta.github.com/scoobi/releases",
                  "sonatype" at "http://oss.sonatype.org/content/repositories/snapshots")

/** Compilation */
scalacOptions ++= Seq("-deprecation", "-unchecked")

/** Testing */
testOptions := Seq(Tests.Filter(s => s.endsWith("Spec") ||
                                     Seq("Index", "All", "UserGuide", "ReadMe").exists(s.contains)))

testOptions in Test += Tests.Argument("scoobi", "local")

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

