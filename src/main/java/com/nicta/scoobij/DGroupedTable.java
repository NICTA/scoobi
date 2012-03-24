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
 * A DGroupTable is a DTable<K, V> where V is actually a list of values. It is
 * the return result of DList.groupByKey. A DGroupedTable has the additional
 * ability to use combine(), which can be very efficiently implemented in
 * MapReduce
 */
public interface DGroupedTable<K, V> extends
		DTable<K, scala.collection.Iterable<V>> {
	/**
	 * Apply an commutative function to reduce the collection of values to a
	 * single value in a key-value-collection distributed list.
	 */
	public DTable<K, V> combine(Combiner<V> combiner,
			OrderedWireFormatType<K> keyBundle, WireFormatType<V> valueBundle);
}
