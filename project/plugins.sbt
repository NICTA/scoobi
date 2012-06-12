addSbtPlugin("com.jsuereth" % "xsbt-gpg-plugin" % "0.6")
    
addSbtPlugin("com.jsuereth" % "sbt-site-plugin" % "0.4.0")

resolvers ++= Seq("less is" at "http://repo.lessis.me", "coda" at "http://repo.codahale.com")

//addSbtPlugin("me.lessis" % "ls-sbt" % "0.1.1", sbtVersion="0.11.3", scalaVersion="2.9.1")