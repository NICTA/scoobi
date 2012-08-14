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
package com.nicta.scoobij.impl;

import scala.Predef;
import scala.Tuple2;

import com.nicta.scoobi.impl.plan.Arr;
import com.nicta.scoobi.impl.plan.Smart;
import com.nicta.scoobij.DGroupedTable;
import com.nicta.scoobij.DTable;
import com.nicta.scoobij.OrderedWireFormatType;
import com.nicta.scoobij.WireFormatType;

public class DTableImpl<K, V> extends DListImpl<scala.Tuple2<K, V>> implements
		DTable<K, V> {

	DTableImpl(com.nicta.scoobi.core.DList<scala.Tuple2<K, V>> impl) {
		super(impl);
	}

	@SuppressWarnings("unchecked")
	@Override
	public DGroupedTable<K, V> groupByKey(OrderedWireFormatType<K> keyBundle,
			WireFormatType<V> valueBundle) {

		Object obj = scala.Predef$.MODULE$.conforms();

		Predef.$less$colon$less<Smart.DComp<Tuple2<K, V>, Arr>, Smart.DComp<Tuple2<K, V>, Arr>> confirms = (Predef.$less$colon$less<Smart.DComp<Tuple2<K, V>, Arr>, Smart.DComp<Tuple2<K, V>, Arr>>) obj;
		// .conforms();

		return new DGroupedTableImpl<K, V>(getImpl().groupByKey(confirms,
				keyBundle.typeInfo(),
				keyBundle.wireFormat(),
				keyBundle.ordering(),
				valueBundle.typeInfo(),
				valueBundle.wireFormat()));
	}
}
