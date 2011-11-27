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
 * A distributed collection of object. The central abstraction of HDFS
 */
public interface DList<A> {

	/**
	 * Apply a specified function to "chunks" of elements from the distributed
	 * list to produce zero or more output elements. The resulting output
	 * elements from the many "chunks" form a new distributed list.
	 * 
	 * @param fun
	 *            the function to execute
	 * @param wf
	 *            the runtime type info object
	 * @return a new DList that representats the results after a parallelDo
	 */
	public <B> DList<B> parallelDo(DoFn<A, B> fun, WireFormatType<B> wf);

	/**
	 * For each element of the distributed list produce zero or more elements by
	 * applying a specified function. The resulting collection of elements form
	 * a new distributed list.
	 * 
	 * @param fm
	 *            the function that is used to flatmap from A to B
	 * @param wf
	 *            the typeinfo for B
	 * @return a new DList the is result of the flatmap
	 */
	public <B> DList<B> flatMap(FlatMapper<A, B> fm, WireFormatType<B> wf);

	// Basically just a flatMap, but for when B is a key-value pair
	// and you want to make it a table. This has a god-awful signature,
	// not quite sure how to improve it yet
	/**
	 * A version of flatMap, except for when the result should be a DTable
	 * instead of a DList. For each element of the distributed list produce zero
	 * or more Key Value elements by applying a specified function. The
	 * resulting collection of elements form a new distributed list.
	 */
	public <B, C> DTable<B, C> tableFlatMap(TableFlatMapper<A, B, C> tm,
			WireFormatType<B> wfB, WireFormatType<C> wfC);

	// there should be something for a grouped table too, really

	/**
	 * Concatenate one or more distributed lists to form a new distributed list.
	 * 
	 * Is ++ from scoobi.DList
	 * 
	 * @param dLists
	 *            are all concatinated together
	 * @return a new DList that is the concatenated product
	 */
	public DList<A> concat(DList<A>... dLists);

	// derived

	/**
	 * For each element of the distributed list produce a new element by
	 * applying a specified function. The resulting collection of elements form
	 * a new distributed list.
	 */
	public <B> DList<B> map(Mapper<A, B> map, WireFormatType<B> wf);

	/**
	 * The same as a map, except for when the result should be a DTable, not a
	 * DList
	 */
	public <B, C> DTable<B, C> tableMap(TableMapper<A, B, C> tm,
			WireFormatType<B> wfB, WireFormatType<C> wfC);

	/**
	 * Keep elements from the distributedl list that pass a spcecified predicate
	 * function.
	 */
	public DList<A> filter(Filterer<A> filter, WireFormatType<A> wf);

	/**
	 * Keep elements from the distributedl list that do not pass a spcecified
	 * predicate function.
	 */
	public DList<A> filterNot(Filterer<A> filter, WireFormatType<A> wf);

	// collect intentionally missing, it's not that useful from java

	/**
	 * Group the values of a distributed list according to some discriminator
	 * function.
	 */
	public <K> DGroupedTable<K, A> groupBy(Mapper<A, K> mapper,
			OrderedWireFormatType<K> wfK, WireFormatType<A> wfA);

	// partition is intentionally missing, doesn't too useful from Java?

	// flatten is missing, as can't really be done nicely from Java

	/**
	 * Create a new distributed list that is keyed based on a specified
	 * function.
	 */
	public <K> DTable<K, A> by(Mapper<A, K> mapper, WireFormatType<K> wfK,
			WireFormatType<A> wfA);

	/**
	 * Returns the underlying scala implementation object. Hopefully not
	 * required for end users.
	 */
	public com.nicta.scoobi.DList<A> getImpl();
}
