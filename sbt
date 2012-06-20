#!/bin/bash
#------------------------------------------------------------------
# sbt driver script.
#------------------------------------------------------------------

sbtdir=./sbtlib
version="0.12.0-RC1"
jarname="sbt-launch-$version.jar"
if [ ! -d "$sbtdir" ] || [ ! -f "$sbtdir/$jarname" ]; then
    
    echo "Fetching sbt version $version"
    mkdir -p sbtlib
    curl "http://typesafe.artifactoryonline.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/$version/sbt-launch.jar" > sbtlib/$jarname
    echo "sbt launch fetched, starting sbt proper...."
    sleep 2
fi

maxheap=2048M
debug=

if [ -n "$debug" ]; then
    echo "Running in debug mode, port: $debug"
    JAVA_OPTIONS="$JAVA_OPTIONS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=$debug"
fi

# -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m is supposed to reduce PermGen errors.
echo env java $JAVA_OPTIONS -Xmx$maxheap -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m -jar sbtlib/$jarname "$@"
env java $JAVA_OPTIONS -Xmx$maxheap -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m -jar sbtlib/$jarname "$@"
