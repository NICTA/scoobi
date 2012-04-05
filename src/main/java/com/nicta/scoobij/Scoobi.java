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

import com.nicta.scoobi.ScoobiConfiguration;
import com.nicta.scoobi.ScoobiConfiguration$;
import com.nicta.scoobij.impl.WithHadoopArgExtractor;

public class Scoobi {
	public static void persist(com.nicta.scoobi.DListPersister<?>... persisters) {
		java.util.Vector<com.nicta.scoobi.DListPersister<?>> copy = new java.util.Vector<com.nicta.scoobi.DListPersister<?>>(
				persisters.length);
		for (int i = 0; i < persisters.length; ++i) {
			copy.add(persisters[i]);
		}

		com.nicta.scoobi.DList$.MODULE$
				.persist(scala.collection.JavaConversions.asScalaBuffer(copy), ScoobiConfiguration$.MODULE$.apply(new String[]{}));
	}

	// Helper method that parsers the command line arguments, it filters out
	// anything that relates to Hadoop and sets it. It returns the remaining
	// args
	public static String[] withHadoopArgs(String[] args) {
		WithHadoopArgExtractor ex = new WithHadoopArgExtractor();
		return ex.extract(args);
	}

}
