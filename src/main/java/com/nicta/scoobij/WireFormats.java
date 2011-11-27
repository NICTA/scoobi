package com.nicta.scoobij;

import java.io.DataInput;
import java.io.DataOutput;

import scala.Tuple2;
import com.nicta.scoobi.WireFormat;

public class WireFormats {

	@SuppressWarnings("unchecked")
	public static OrderedWireFormatType<Integer> integer() {

		return new com.nicta.scoobij.impl.WireFormatImpl<Integer>(
				Integer.class,
				com.nicta.scoobi.WireFormat$.MODULE$.IntegerFmt(),
				integerOrdering());
	}

	@SuppressWarnings("unchecked")
	public static OrderedWireFormatType<String> string() {
		return new com.nicta.scoobij.impl.WireFormatImpl<String>(String.class,
				com.nicta.scoobi.WireFormat$.MODULE$.StringFmt(),
				stringOrdering());
	}

	public static Ordering<String> stringOrdering() {
		return new Ordering<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public int compare(String a, String b) {
				return a.compareTo(b);
			}
		};
	}

	public static Ordering<Integer> integerOrdering() {
		return new Ordering<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public int compare(Integer a, Integer b) {
				return a.compareTo(b);
			}
		};
	}

	public static <T, V> WireFormat<scala.Tuple2<T, V>> wireFormatPair(
			final WireFormat<T> ord1, final WireFormat<V> ord2) {

		return new WireFormat<scala.Tuple2<T, V>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<T, V> fromWire(DataInput arg0) {

				T t = ord1.fromWire(arg0);
				V v = ord2.fromWire(arg0);

				return new Tuple2<T, V>(t, v);
			}

			@Override
			public String show(Tuple2<T, V> arg0) {
				return "Tuple2(" + ord1.show(arg0._1) + ", "
						+ ord2.show(arg0._2) + ")";
			}

			@Override
			public void toWire(Tuple2<T, V> arg0, DataOutput arg1) {
				ord1.toWire(arg0._1, arg1);
				ord2.toWire(arg0._2, arg1);
			}

		};
	}
}
