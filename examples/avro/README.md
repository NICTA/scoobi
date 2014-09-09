# Avro Java Code Generator

## To compile, run:

    $ sbt compile

## To generate the Weather.java source from src/main/avro/weather.avsc:

    $ sbt avro:generate

Look at the output:

    $ cat ./target/scala-2.10/src_managed/main/compiled_avro/test/Weather.java

## To run the sample AvroExample.scala code that uses the generated Weather.java file

    $ sbt "run-main com.nicta.scoobi.examples.AvroExample"
    $ ls -l avro-joined-output/  # output of the avro file
    $ cat avro-joined-output2/out-m-00000  # output of the JSON
