/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobij;

import java.io.DataInput;
import java.io.DataOutput;

import scala.Tuple2;
import scala.reflect.Manifest;

import com.nicta.scoobi.core.WireFormat;
import com.nicta.scoobij.impl.Conversions;

public class WireFormats {

	public static OrderedWireFormatType<Integer> integer() {

		return new com.nicta.scoobij.impl.WireFormatImpl<Integer>(
				Integer.class,
				com.nicta.scoobi.core.WireFormat$.MODULE$.IntegerFmt(),
				integerOrdering());
	}

	public static OrderedWireFormatType<String> string() {
		return new com.nicta.scoobij.impl.WireFormatImpl<String>(String.class,
				com.nicta.scoobi.core.WireFormat$.MODULE$.StringFmt(),
				stringOrdering());
	}

	public static Ordering<String> stringOrdering() {
		return new Ordering<String>() {
			@SuppressWarnings("unused")
			private static final long serialVersionUID = 1L;

			@Override
			public int sortCompare(String a, String b) {
				return a.compareTo(b);
			}
		};
	}

	public static Ordering<Integer> integerOrdering() {
		return new Ordering<Integer>() {
			@SuppressWarnings("unused")
			private static final long serialVersionUID = 1L;

			@Override
			public int sortCompare(Integer a, Integer b) {
				return a.compareTo(b);
			}
		};
	}

	public static <T, V> WireFormatType<scala.Tuple2<T, V>> wireFormatPair(
			final WireFormatType<T> ord1, final WireFormatType<V> ord2) {

		return new WireFormatType<scala.Tuple2<T, V>>() {

			@Override
			public com.nicta.scoobi.core.WireFormat<scala.Tuple2<T,V>> wireFormat() {
				return new WireFormat<scala.Tuple2<T, V>>() {
					@SuppressWarnings("unused")
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<T, V> fromWire(DataInput arg0) {

						T t = ord1.wireFormat().fromWire(arg0);
						V v = ord2.wireFormat().fromWire(arg0);

						return new Tuple2<T, V>(t, v);
					}

					@Override
					public void toWire(Tuple2<T, V> arg0, DataOutput arg1) {
						ord1.wireFormat().toWire(arg0._1, arg1);
						ord2.wireFormat().toWire(arg0._2, arg1);
					}
				};
			}

			@Override
			public Manifest<scala.Tuple2<T, V>> typeInfo() {
				return Conversions.toManifest(scala.Tuple2.class, ord1.typeInfo(), ord2.typeInfo());
			};



		};
	}

	public static WireFormatType<scala.runtime.BoxedUnit> wireFormatUnit() {

		return new WireFormatType<scala.runtime.BoxedUnit>() {

			@SuppressWarnings("unchecked")
			@Override
			public com.nicta.scoobi.core.WireFormat<scala.runtime.BoxedUnit> wireFormat() {
				return com.nicta.scoobi.core.WireFormat$.MODULE$.UnitFmt();
			}

			@Override
			public Manifest<scala.runtime.BoxedUnit> typeInfo() {
				return Conversions.toManifest(scala.runtime.BoxedUnit.class);
			};



		};
	}
}
