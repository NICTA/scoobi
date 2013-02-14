resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"

addSbtPlugin("com.typesafe.sbt" % "sbt-pgp" % "0.8")
    
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "0.6.2")

addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.5.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.5.0")

resolvers ++= Seq("less is" at "http://repo.lessis.me", "coda" at "http://repo.codahale.com")

addSbtPlugin("me.lessis" % "ls-sbt" % "0.1.2")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "0.6")

resolvers += "cavorite" at "http://files.cavorite.com/maven/"

addSbtPlugin("com.cavorite" % "sbt-avro" % "0.2")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.8.3")