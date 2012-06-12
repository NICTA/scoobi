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

import com.nicta.scoobi.application.PFn;
import com.nicta.scoobi.application.Persister;
import com.nicta.scoobi.application.Persister$;
import com.nicta.scoobi.application.ScoobiConfiguration$;
import com.nicta.scoobij.impl.WithHadoopArgExtractor;

public class Scoobi {
	@SuppressWarnings("unchecked")
	public static <T> void persist(com.nicta.scoobi.application.DListPersister<T> persister) {
		com.nicta.scoobi.application.PFn<com.nicta.scoobi.application.DListPersister<T>> pfn = (PFn<com.nicta.scoobi.application.DListPersister<T>>) com.nicta.scoobi.application.PFn$.MODULE$
				.<T> DListPersister();

		com.nicta.scoobi.application.Persister<com.nicta.scoobi.application.DListPersister<T>> per = (Persister<com.nicta.scoobi.application.DListPersister<T>>) com.nicta.scoobi.application.Persister$.MODULE$
				.tuple1persister(pfn);

		Persister$.MODULE$.persist(persister, ScoobiConfiguration$.MODULE$.apply(new String[]{}), per);
	}

	@SuppressWarnings("unchecked")
	public static <T, V> void persist(
			com.nicta.scoobi.application.DListPersister<T> persister1,
			com.nicta.scoobi.application.DListPersister<T> persister2) {

		com.nicta.scoobi.application.PFn<com.nicta.scoobi.application.DListPersister<T>> pfn1 = (PFn<com.nicta.scoobi.application.DListPersister<T>>) com.nicta.scoobi.application.PFn$.MODULE$
				.<T> DListPersister();

		com.nicta.scoobi.application.PFn<com.nicta.scoobi.application.DListPersister<V>> pfn2 = (PFn<com.nicta.scoobi.application.DListPersister<V>>) com.nicta.scoobi.application.PFn$.MODULE$
				.<V> DListPersister();

		com.nicta.scoobi.application.Persister<scala.Tuple2<com.nicta.scoobi.application.DListPersister<T>, com.nicta.scoobi.application.DListPersister<V>>> per = (com.nicta.scoobi.application.Persister<scala.Tuple2<com.nicta.scoobi.application.DListPersister<T>, com.nicta.scoobi.application.DListPersister<V>>>) com.nicta.scoobi.application.Persister$.MODULE$
				.tuple2persister(pfn1, pfn2);

		scala.Tuple2<com.nicta.scoobi.application.DListPersister<T>, com.nicta.scoobi.application.DListPersister<V>> persisters = scala.Tuple2$.MODULE$.apply(persister1,
				persister2);

		Persister$.MODULE$.persist(persisters, ScoobiConfiguration$.MODULE$.apply(new String[] {}), per);
	}

	// Helper method that parsers the command line arguments, it filters out
	// anything that relates to Hadoop and sets it. It returns the remaining
	// args
	public static String[] withHadoopArgs(String[] args) {
		WithHadoopArgExtractor ex = new WithHadoopArgExtractor();
		return ex.extract(args);
	}

}
