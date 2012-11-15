resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"

addSbtPlugin("com.jsuereth" % "xsbt-gpg-plugin" % "0.6")

addSbtPlugin("com.jsuereth" % "sbt-site-plugin" % "0.4.0")

addSbtPlugin("com.jsuereth" % "sbt-ghpages-plugin" % "0.4.0")

resolvers ++= Seq("less is" at "http://repo.lessis.me",
                  "coda" at "http://repo.codahale.com")

addSbtPlugin("me.lessis" % "ls-sbt" % "0.1.2")
