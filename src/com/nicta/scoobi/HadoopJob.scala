/**
  *
  * Copyright: [2011] Ben Lever
  *
  *
  */
package com.nicta.scoobi

import java.io._
import org.apache.hadoop.io._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.util._
import org.apache.hadoop.mapred._
import org.apache.hadoop.filecache._


/**
  *
  */
class HadoopJob[K1, V1, K2, V2, K3, V3](
    m: Mapper[K1, V1, K2, V2],
    r: Reducer[K2, V2, K3, V3])
    (implicit
      mk2: Manifest[K2],
      mv2: Manifest[V2],
      mk3: Manifest[K3],
      mv3: Manifest[V3]) {


  /**
    *
    */
  def run(input: String, output: String) = {

    implicit val jobConf = new JobConf //(conf); 

    jobConf.setJobName("!!!Scoobi Job!!!");
    //jobConf.setJar(userJar);    // ??


    /* Mapper */
    serializeObj(m, "/tmp/mapper.obj")
    jobConf.setMapperClass(classOf[GenericMapper[_,_,_,_]])
    FileInputFormat.setInputPaths(jobConf, new Path(input))
    //DOESN'T work for v2.9 ... jobConf.setInputFormat(classOf[KeyValueTextInputFormat].asInstanceOf[Class[_ <: InputFormat[_, _]]])
    jobConf.setInputFormat(classOf[TextInputFormat])
    jobConf.setMapOutputKeyClass(Implicits.classToWritableClass(mk2.erasure))
    jobConf.setMapOutputValueClass(Implicits.classToWritableClass(mv2.erasure))

    /* Reducer */
    serializeObj(r, "/tmp/reducer.obj")
    jobConf.setReducerClass(classOf[GenericReducer[_,_,_,_]])
    jobConf.setOutputKeyClass(Implicits.classToWritableClass(mk3.erasure))
    jobConf.setOutputValueClass(Implicits.classToWritableClass(mv3.erasure))
    FileOutputFormat.setOutputPath(jobConf, new Path(output))
    jobConf.setOutputFormat(classOf[TextOutputFormat[_, _]])

    JobClient.runJob(jobConf);
  }

  private def serializeObj(srcObj: Any, dstFile: String) = {
    val oos = new ObjectOutputStream(new FileOutputStream(dstFile))
    oos.writeObject(srcObj)
    oos.close()
  }
}
