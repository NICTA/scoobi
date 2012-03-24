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
package com.nicta.scoobij;


/**
 * DTable is a DList that contains a key-value pair
 */
public interface DTable<K, V> extends DList<scala.Tuple2<K, V>> {
	/** Group the values of a distributed list with key-value elements by key. */
	public DGroupedTable<K, V> groupByKey(OrderedWireFormatType<K> keyBundle,
			WireFormatType<V> valueBundle);
}
