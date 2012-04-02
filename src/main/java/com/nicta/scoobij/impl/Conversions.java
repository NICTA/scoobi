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

import com.nicta.scoobi.impl.AnExtractor;
import com.nicta.scoobij.Combiner;
import com.nicta.scoobij.Extractor;
import com.nicta.scoobij.Filterer;
import com.nicta.scoobij.FlatMapper;
import com.nicta.scoobij.KeyValue;
import com.nicta.scoobij.Mapper;
import com.nicta.scoobij.TableFlatMapper;
import com.nicta.scoobij.TableMapper;
import com.nicta.scoobi.impl.ExtractorAdaptor;

public class Conversions {

	public static <T> scala.collection.Iterable<T> toScala(
			java.lang.Iterable<T> it) {
		return scala.collection.JavaConversions.iterableAsScalaIterable(it);
	}

	public static <T, V> scala.Function1<T, scala.collection.Iterable<V>> toScala(
			final FlatMapper<T, V> fm) {

		return new FlatMapAdaptor<T, V>(fm);
	}

	public static <T> scala.Function2<T, T, T> toScala(final Combiner<T> c) {
		return new CombinerAdaptor<T>(c);
	}

	public static <T> scala.PartialFunction<scala.collection.immutable.List<String>, T> toScala(
			final Extractor<T> ex) {
        AnExtractor<T> anExtractor = new AnExtractor<T>() {
            public T apply(java.lang.Iterable<String> strings) {
                return ex.apply(strings);
            }
        };
		return new ExtractorAdaptor<T>(anExtractor);
	}

	public static <T> scala.collection.Seq<T> toScalaSeq(T[] v) {
		return scala.Predef$.MODULE$.genericWrapArray(v);
	}

	public static <T> scala.reflect.Manifest<T> toManifest(Class<T> clazz) {
		return scala.reflect.Manifest$.MODULE$.classType(clazz);
	}

	@SuppressWarnings("unchecked")
	public static <T> scala.reflect.Manifest<T> toManifest(Class<?> topLevel,
			scala.reflect.Manifest<?> typeArg1,
			scala.reflect.Manifest<?>... typeArgs) {

		Class<T> clazz = (Class<T>) topLevel;

		return scala.reflect.Manifest$.MODULE$.classType(clazz, typeArg1,
				toScalaSeq(typeArgs));
	}

	public static <K, V> com.nicta.scoobi.DoFn<K, V> toScalaDoFn(
			final Mapper<K, V> mapper) {
		return new com.nicta.scoobi.DoFn<K, V>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void cleanup(com.nicta.scoobi.Emitter<V> arg0) {
			}

			@Override
			public void process(K input, com.nicta.scoobi.Emitter<V> arg1) {
				arg1.emit(mapper.apply(input));
			}

			@Override
			public void setup() {
			}

		};

	}

	public static <T, V, W> TableFlatMapper<T, V, W> toTableFlatMapper(
			final TableMapper<T, V, W> mapper) {
		return new TableFlatMapper<T, V, W>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<scala.Tuple2<V, W>> apply(T v) {
				java.util.Vector<scala.Tuple2<V, W>> list = new java.util.Vector<scala.Tuple2<V, W>>(
						1);
				list.add(toScala(mapper.apply(v)));
				return list;
			}
		};

	}

	public static <K, V> scala.Tuple2<K, V> toScala(KeyValue<K, V> kv) {
		return new scala.Tuple2<K, V>(kv.key, kv.value);
	}

	public static <T> FlatMapper<T, T> toFlatMapper(final Filterer<T> filt,
			final boolean not) {
		return new FlatMapper<T, T>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<T> apply(T value) {
				java.util.Vector<T> list = new java.util.Vector<T>();

				if (filt.apply(value) != not)
					list.add(value);
				return list;
			}
		};
	}

	public static <T, K> TableMapper<T, K, T> toByMap(final Mapper<T, K> mapper) {
		return new TableMapper<T, K, T>() {
			private static final long serialVersionUID = 1L;

			@Override
			public KeyValue<K, T> apply(T t) {
				return new KeyValue<K, T>(mapper.apply(t), t);
			}
		};
	}

	public static <T, V> com.nicta.scoobi.DoFn<T, V> toScala(
			final com.nicta.scoobij.DoFn<T, V> fun) {
		return new com.nicta.scoobi.DoFn<T, V>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void cleanup(com.nicta.scoobi.Emitter<V> emitter) {
				fun.cleanup(toJava(emitter));
			}

			@Override
			public void process(T input, com.nicta.scoobi.Emitter<V> emitter) {
				fun.process(input, toJava(emitter));
			}

			@Override
			public void setup() {
				fun.setup();
			}
		};
	}

	public static <T> com.nicta.scoobij.Emitter<T> toJava(
			final com.nicta.scoobi.Emitter<T> emitter) {
		return new com.nicta.scoobij.Emitter<T>() {

			@Override
			public void emit(T value) {
				emitter.emit(value);
			}

		};
	}
}
