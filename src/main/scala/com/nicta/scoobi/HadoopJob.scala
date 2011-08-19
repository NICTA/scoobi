/**
  * Copyright: [2011] Ben Lever
  */
package com.nicta.scoobi

import java.io._
import org.apache.hadoop.io._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.util._
import org.apache.hadoop.mapred._

import Mapper._
import Reducer._



// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

// Scoobi version of TextInputFormat + its reader

//*
class LongScoobiWritableComparable(x: Long) extends ScoobiWritableComparable[Long](x) {
    def write(out: DataOutput) = (new LongWritable(get)).write(out)
    def readFields(in: DataInput) = { val y = new LongWritable; y.readFields(in); set(y.get) }
    def compareTo(that: ScoobiWritableComparable[Long]) = java.lang.Double.compare(get, that.get)
}


class StringScoobiWritable(x: String) extends ScoobiWritable[String](x) {
  def write(out: DataOutput) = (new Text(get)).write(out)
  def readFields(in: DataInput) = { val y = new Text; y.readFields(in); set(y.toString) }
}


class ScoobiLineRecordReader(job: JobConf, fileSplit: FileSplit)
  extends RecordReader[LongScoobiWritableComparable, StringScoobiWritable] {

  val lrr = new LineRecordReader(job, fileSplit)

  def close() = lrr.close()

  def createKey: LongScoobiWritableComparable = {
    new LongScoobiWritableComparable(0)
  }

  def createValue: StringScoobiWritable = {
    new StringScoobiWritable("")
  }

  def getPos: Long = lrr.getPos

  def getProgress: Float = lrr.getProgress

  def next(key: LongScoobiWritableComparable, value: StringScoobiWritable) = {
    var k: LongWritable = new LongWritable
    var v: Text = new Text
    val more = lrr.next(k, v)
    key.set(k.get)
    value.set(v.toString)
    more
  }
}


class ScoobiTextInputFormat
  extends FileInputFormat[LongScoobiWritableComparable, StringScoobiWritable] {

  def getRecordReader(split: InputSplit, job: JobConf, reporter: Reporter):
    RecordReader[LongScoobiWritableComparable, StringScoobiWritable] = {
    
    reporter.setStatus(split.toString)
    new ScoobiLineRecordReader(job, split.asInstanceOf[FileSplit])
  }
}
//*/

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


/** A single Hadoop MapReduce job. */
class HadoopJob[K1 : HadoopWritable : Ordering,
                V1 : HadoopWritable,
                K2 : HadoopWritable : Ordering : Manifest,
                V2 : HadoopWritable,
                K3 : HadoopWritable : Ordering : Manifest,
                V3 : HadoopWritable](
    map: (K1, V1) => List[(K2, V2)],
    reduce: (K2, List[V2]) => List[(K3, V3)]) {

  /** Execute this Hadoop job. */
  def execute(input: String, output: String) = {

    /* NOTE:
     * Need to do all this in here as locals to the 'run' method so that
     * everything is evaluated before Hadoop is invoked. If we make them
     * member fields, Scalal doesn't evaluate them immediatley and instead
     * they get evaluated within the mapping task ... */
    val k2RtClass = ScoobiWritableComparable("ScoobiK2", null.asInstanceOf[K2])
    val v2RtClass = ScoobiWritable("ScoobiV2", null.asInstanceOf[V2])

    val k3RtClass = ScoobiWritableComparable("ScoobiK3", null.asInstanceOf[K3])
    val v3RtClass = ScoobiWritable("ScoobiV3", null.asInstanceOf[V3])

    val mapper = new Mapper(map, k2RtClass.clazz, v2RtClass.clazz)
    val reducer = new Reducer(reduce, k3RtClass.clazz, v3RtClass.clazz)


    /* Make temporary JAR file for this job */
    val tmpFile = File.createTempFile("scoobi-job-", ".jar")
    var jar = new JarBuilder(tmpFile.getName)

    jar.addContainingJar(classOf[List[_]])  //  Scala
    jar.addContainingJar(this.getClass)     //  Scoobi (+ example user app code)
    jar.addClassFromBytecode(k2RtClass.name, k2RtClass.bytecode) // generated code
    jar.addClassFromBytecode(v2RtClass.name, v2RtClass.bytecode)
    jar.addClassFromBytecode(k3RtClass.name, k3RtClass.bytecode)
    jar.addClassFromBytecode(v3RtClass.name, v3RtClass.bytecode)

    jar.close()

    /* Configuration */
    val conf = new Configuration
    val jobConf = new JobConf(conf)
    jobConf.setJobName("!!!Scoobi Job!!!")
    jobConf.setJar(jar.name)

    /* Mapper */
    DistributedObject.pushObject(jobConf, mapper, "mapper")
    jobConf.setMapperClass(classOf[GenericMapper[_,_,_,_]])

    FileInputFormat.setInputPaths(jobConf, new Path(input))
    //DOESN'T work for v2.9 ... jobConf.setInputFormat(classOf[KeyValueTextInputFormat].asInstanceOf[Class[_ <: InputFormat[_, _]]])
    jobConf.setInputFormat(classOf[ScoobiTextInputFormat])

    jobConf.setMapOutputKeyClass(k2RtClass.clazz)
    jobConf.setMapOutputValueClass(v2RtClass.clazz)

    /* Reducer */
    DistributedObject.pushObject(jobConf, reducer, "reducer")
    jobConf.setReducerClass(classOf[GenericReducer[_,_,_,_]])

    jobConf.setOutputKeyClass(k3RtClass.clazz)
    jobConf.setOutputValueClass(v3RtClass.clazz)

    FileOutputFormat.setOutputPath(jobConf, new Path(output))
    jobConf.setOutputFormat(classOf[TextOutputFormat[_, _]])

    JobClient.runJob(jobConf)

    /* Clean-up */
    tmpFile.delete
  }
}
