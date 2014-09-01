package com.nicta.scoobi.io

import com.nicta.scoobi.Scoobi._

package object thrift {

  type ThriftLike = org.apache.thrift.TBase[_ <: org.apache.thrift.TBase[_, _], _ <: org.apache.thrift.TFieldIdEnum]

  implicit def ThriftWireFormat[A](implicit m: Manifest[A], ev: A <:< ThriftLike): WireFormat[A] =ThriftSchema.mkThriftFmt[A]

  implicit def ThriftSeqSchema[A](implicit m: Manifest[A], ev: A <:< ThriftLike): SeqSchema[A] = ThriftSchema.mkThriftSchema[A]
}