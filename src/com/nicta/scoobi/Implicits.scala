/**
  *
  * Copyright: [2011] Ben Lever
  *
  *
  */
package com.nicta.scoobi

import org.apache.hadoop.io._

/**
  *
  *
  *
  */
object Implicits {

  def wireToReal(t: Writable): Any = t match {
    case t: Text => t.toString;
    case arr: ArrayWritable => arr.get().map(wireToReal);
    case t => try {
      t.asInstanceOf[{def get():Any;}].get();
    } catch {
      case e => t;
    }
  }

  implicit def realToWire(t: Any): Writable = t match {
    case t: Writable => t;
    case t: Int => new IntWritable(t);
    case t: Long => new LongWritable(t);
    //case t: Byte => new ByteWritable(t);
    case t: Float => new FloatWritable(t);
    //case t: Double => new DoubleWritable(t);
    case t: Boolean => new BooleanWritable(t);
    case t: String => new Text(t);
    case t: Array[Byte] => new BytesWritable(t);
    /*
    case x : AnyRef if x.getClass.isArray => { 
      val t = x.asInstanceOf[Array[Any]];
      if(t.length == 0) new AnyWritable(t);
      else { 
        val mapped = t.map(realToWire); 
        val classes = mapped.map(_.getClass);
        if(classes.forall(classes(0)==_)) { 
          // can only use ArrayWritable if all Writables are the same.
          new ArrayWritable(classes(0),mapped);
        } else {
          // fall back on AnyWritable
          val mapped = t.map(new AnyWritable[Any](_).asInstanceOf[Writable]); 
          new ArrayWritable(classOf[AnyWritable[_]],mapped);
        }
      }
    }
    case _ => new AnyWritable(t);
    */
  }

  private val CInt        = classOf[Int];
  private val CLong       = classOf[Long];
  private val CByte       = classOf[Byte];
  private val CDouble     = classOf[Double];
  private val CFloat      = classOf[Float];
  private val CBoolean    = classOf[Boolean];
  private val CString     = classOf[String];
  private val CArrayByte  = classOf[Array[Byte]];
  private val CArray      = classOf[Array[_]];

  def mkManifest[T](c:Class[T]) = new Manifest[T] {
    def erasure = c;
  }

  private val CWritable = mkManifest(classOf[Writable]);


  def classToWritableClass[T](c: Class[T]): Class[Writable] = c match {
    case c if mkManifest(c) <:< CWritable => c.asInstanceOf[Class[Writable]];
    case CInt       => classOf[IntWritable].asInstanceOf[Class[Writable]];
    case CLong      => classOf[LongWritable].asInstanceOf[Class[Writable]];
   // case CByte => classOf[ByteWritable].asInstanceOf[Class[Writable]];
    //case CDouble => classOf[DoubleWritable].asInstanceOf[Class[Writable]];
    case CFloat     => classOf[FloatWritable].asInstanceOf[Class[Writable]];
    case CBoolean   => classOf[BooleanWritable].asInstanceOf[Class[Writable]];
    case CString    => classOf[Text].asInstanceOf[Class[Writable]];
    case CArrayByte => classOf[BytesWritable].asInstanceOf[Class[Writable]];
    case CArray     => classOf[ArrayWritable].asInstanceOf[Class[Writable]];
    //case _          => classOf[AnyWritable[_]].asInstanceOf[Class[Writable]];
  }
}
