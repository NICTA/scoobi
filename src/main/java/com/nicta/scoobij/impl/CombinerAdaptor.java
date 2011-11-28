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

import java.io.Serializable;

import com.nicta.scoobij.Combiner;

public class CombinerAdaptor<T> extends
		scala.runtime.AbstractFunction2<T, T, T> implements Serializable {

	private static final long serialVersionUID = 1L;

	public CombinerAdaptor(Combiner<T> cmb) {
		combiner = cmb;
	}

	@Override
	public T apply(T t1, T t2) {
		return combiner.apply(t1, t2);
	}

	private Combiner<T> combiner;

}
