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
import scala.Option;
import scala.PartialFunction;

import com.nicta.scoobij.Extractor;

public class ExtractorAdaptor<T>
		extends
		scala.runtime.AbstractFunction1<scala.collection.immutable.List<String>, T>
		implements
		scala.PartialFunction<scala.collection.immutable.List<String>, T> {

	public ExtractorAdaptor(Extractor<T> ex) {
		extractor = ex;
	}

	@Override
	public T apply(scala.collection.immutable.List<String> strings) {
		return extractor.apply(scala.collection.JavaConversions
				.asJavaIterable(strings));
	}

	@Override
	public boolean isDefinedAt(scala.collection.immutable.List<String> strings) {
		return extractor.apply(scala.collection.JavaConversions
				.asJavaIterable(strings)) != null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <A> PartialFunction<scala.collection.immutable.List<String>, A> andThen(Function1<T, A> fn) {
		return scala.PartialFunction$class.andThen(this, fn);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Function1<scala.collection.immutable.List<String>, Option<T>> lift() {
		return scala.PartialFunction$class.lift(this);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public PartialFunction orElse(PartialFunction arg0) {
		return scala.PartialFunction$class.orElse(this, arg0);
	}

	private Extractor<T> extractor;
}