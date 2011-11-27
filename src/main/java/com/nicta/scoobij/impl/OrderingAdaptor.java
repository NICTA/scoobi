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

import scala.Function1;
import scala.Some;

import com.nicta.scoobij.Ordering;

public class OrderingAdaptor<T> implements scala.math.Ordering<T> {

	OrderingAdaptor(Ordering<T> o) {
		ordering = o;
	}

	private static final long serialVersionUID = 1L;

	@Override
	public int compare(T arg0, T arg1) {
		return ordering.compare(arg0, arg1);
	}

	@Override
	public boolean equiv(T arg0, T arg1) {
		return ordering.compare(arg0, arg1) != 0;
	}

	@Override
	public boolean gt(T arg0, T arg1) {
		return ordering.compare(arg0, arg1) > 0;
	}

	@Override
	public boolean gteq(T arg0, T arg1) {
		return ordering.compare(arg0, arg1) >= 0;
	}

	@Override
	public boolean lt(T arg0, T arg1) {
		return ordering.compare(arg0, arg1) < 0;
	}

	@Override
	public boolean lteq(T arg0, T arg1) {
		return ordering.compare(arg0, arg1) <= 0;
	}

	@Override
	public T max(T arg0, T arg1) {
		return gteq(arg0, arg1) ? arg0 : arg1;
	}

	@Override
	public T min(T arg0, T arg1) {
		return lteq(arg0, arg1) ? arg0 : arg1;
	}

	@Override
	public scala.math.Ordering<T>.Ops mkOrderingOps(T arg0) {
		System.err.println("Don't know how to mkOrdering ops");
		System.exit(-1);
		return null;
	}

	@Override
	public <U> scala.math.Ordering<U> on(Function1<U, T> arg0) {
		System.err.println("Don't know how to on Function");
		System.exit(-1);
		return null;
	}

	@Override
	public scala.math.Ordering<T> reverse() {
		System.err.println("Reverse not implemented");
		System.exit(-1);
		return null;
	}

	@Override
	public Some<Object> tryCompare(T arg0, T arg1) {
		System.err.println("tryCompare not implemented");
		System.exit(-1);
		return null;
	}

	Ordering<T> ordering;
}