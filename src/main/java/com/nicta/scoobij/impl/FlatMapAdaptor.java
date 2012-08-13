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

import com.nicta.scoobij.FlatMapper;


public class FlatMapAdaptor<T, V> extends scala.runtime.AbstractFunction1<T, scala.collection.Iterable<V>> {

	public FlatMapAdaptor(FlatMapper<T, V> fm) {
		flatMapper = fm;
	}

	@Override
	public scala.collection.Iterable<V> apply(T t) {
		return Conversions.toScala(flatMapper.apply(t));
	}

	private FlatMapper<T, V> flatMapper;
}
