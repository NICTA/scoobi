/**
 * Copyright 2011 National ICT Australia Limited
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
package com.nicta.scoobij.impl;

import java.util.Vector;

import com.nicta.scoobij.DGroupedTable;
import com.nicta.scoobij.DList;
import com.nicta.scoobij.DTable;
import com.nicta.scoobij.DoFn;
import com.nicta.scoobij.Filterer;
import com.nicta.scoobij.FlatMapper;
import com.nicta.scoobij.Mapper;
import com.nicta.scoobij.OrderedWireFormatType;
import com.nicta.scoobij.TableFlatMapper;
import com.nicta.scoobij.TableMapper;
import com.nicta.scoobij.WireFormatType;
import com.nicta.scoobij.WireFormats;

public class DListImpl<A> implements com.nicta.scoobij.DList<A> {

	public DListImpl(com.nicta.scoobi.DList<A> dl) {
		impl = dl;
	}

	@Override
	public <B> DList<B> parallelDo(DoFn<A, B> fun, WireFormatType<B> wf) {
		return new DListImpl<B>(getImpl().parallelDo(Conversions.toScala(fun),
				wf.typeInfo(), wf.wireFormat()));
	}

	@Override
	public <V> DListImpl<V> flatMap(FlatMapper<A, V> fm,
			WireFormatType<V> bundle) {
		return new DListImpl<V>(impl.flatMap(Conversions.toScala(fm),
				bundle.typeInfo(), bundle.wireFormat()));
	}

	@Override
	public <B, C> DTable<B, C> tableFlatMap(TableFlatMapper<A, B, C> tm,
			WireFormatType<B> bundleB, WireFormatType<C> bundleC) {

		scala.reflect.Manifest<scala.Tuple2<B, C>> manifest = Conversions
				.toManifest(scala.Tuple2.class,
						bundleB.typeInfo(),
						bundleC.typeInfo());

		return new DTableImpl<B, C>(impl.flatMap(
				Conversions.toScala(tm),
				manifest,
				WireFormats.wireFormatPair(bundleB, bundleC).wireFormat()));
	}

	// ++ from scoobi.DList
	@Override
	public DListImpl<A> concat(DList<A>... dLists) {

		return new DListImpl<A>(impl.$plus$plus(toImplArray(dLists)));
	}

	private static <T> scala.collection.Seq<com.nicta.scoobi.DList<T>> toImplArray(
			DList<T>[] v) {

		Vector<com.nicta.scoobi.DList<T>> vecta = new Vector<com.nicta.scoobi.DList<T>>(
				v.length);

		for (int i = 0; i < v.length; ++i) {
			vecta.add(v[i].getImpl());
		}

		return scala.collection.JavaConversions.asScalaBuffer(vecta);
	}

	@Override
	public <B> DList<B> map(Mapper<A, B> mapper, WireFormatType<B> wf) {

		return new DListImpl<B>(getImpl().parallelDo(
				Conversions.toScalaDoFn(mapper),
				wf.typeInfo(), wf.wireFormat()));
	}

	@Override
	public <B, C> DTable<B, C> tableMap(TableMapper<A, B, C> tm,
			WireFormatType<B> bundleB, WireFormatType<C> bundleC) {
		return tableFlatMap(Conversions.toTableFlatMapper(tm), bundleB, bundleC);
	}

	@Override
	public DList<A> filter(Filterer<A> filter, WireFormatType<A> bundle) {
		return flatMap(Conversions.toFlatMapper(filter, false), bundle);
	}

	@Override
	public DList<A> filterNot(Filterer<A> filter, WireFormatType<A> bundle) {
		return flatMap(Conversions.toFlatMapper(filter, true), bundle);
	}

	@Override
	public <K> DGroupedTable<K, A> groupBy(Mapper<A, K> mapper,
			OrderedWireFormatType<K> bundleK, WireFormatType<A> bundleA) {
		return tableMap(Conversions.toByMap(mapper), bundleK, bundleA)
				.groupByKey(bundleK, bundleA);
	}

	@Override
	public <K> DTable<K, A> by(Mapper<A, K> mapper, WireFormatType<K> bundleK,
			WireFormatType<A> bundleA) {
		return tableMap(Conversions.toByMap(mapper), bundleK, bundleA);
	}

	public com.nicta.scoobi.DList<A> getImpl() {
		return impl;
	}

	private com.nicta.scoobi.DList<A> impl;

}
