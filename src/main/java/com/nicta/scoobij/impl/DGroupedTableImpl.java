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

import scala.Tuple2;

import com.nicta.scoobij.Combiner;
import com.nicta.scoobij.DGroupedTable;
import com.nicta.scoobij.DTable;
import com.nicta.scoobij.OrderedWireFormatType;
import com.nicta.scoobij.WireFormatType;

public class DGroupedTableImpl<K, V> extends
		DTableImpl<K, scala.collection.Iterable<V>> implements
		DGroupedTable<K, V> {

	DGroupedTableImpl(
			com.nicta.scoobi.DList<Tuple2<K, scala.collection.Iterable<V>>> impl) {
		super(impl);
	}

	@SuppressWarnings("unchecked")
	@Override
	public DTable<K, V> combine(Combiner<V> combiner,
			OrderedWireFormatType<K> keyFormat, WireFormatType<V> valueBundle) {

		Object obj = scala.Predef$.MODULE$.conforms();

		scala.Predef.$less$colon$less<com.nicta.scoobi.impl.plan.Smart.DList<scala.Tuple2<K, scala.collection.Iterable<V>>>, com.nicta.scoobi.impl.plan.Smart.DList<scala.Tuple2<K, scala.collection.Iterable<V>>>> evidence = (scala.Predef.$less$colon$less<com.nicta.scoobi.impl.plan.Smart.DList<scala.Tuple2<K, scala.collection.Iterable<V>>>, com.nicta.scoobi.impl.plan.Smart.DList<scala.Tuple2<K, scala.collection.Iterable<V>>>>) obj;

		return new DTableImpl<K, V>(getImpl().combine(
				Conversions.toScala(combiner), evidence,
				keyFormat.typeInfo(),
				keyFormat.wireFormat(),
			    keyFormat.ordering(),
				valueBundle.typeInfo(),
				valueBundle.wireFormat()));
	}
}
