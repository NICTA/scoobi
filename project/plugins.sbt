resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"
    
addSbtPlugin("com.typesafe.sbt" % "sbt-pgp" % "0.8.1")
    
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "0.6.2")

addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.5.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.5.1")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "0.7.1")
