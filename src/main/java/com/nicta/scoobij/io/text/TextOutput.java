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
package com.nicta.scoobij.io.text;

import com.nicta.scoobij.DList;
import com.nicta.scoobij.DTable;
import com.nicta.scoobij.WireFormats;
import com.nicta.scoobij.WireFormatType;
public class TextOutput {

	// The return type of this should be used with Scoobi.Persist
	public static <T> com.nicta.scoobi.DListPersister<T> toTextFile(
			DList<T> dl, String path, boolean overwrite, WireFormatType<T> bundle) {

		return com.nicta.scoobi.io.text.TextOutput.toTextFile(dl.getImpl(),
				path, overwrite, bundle.typeInfo());
	}
//
	public static <K, V> com.nicta.scoobi.DListPersister<scala.Tuple2<K, V>> toTextFile(
			DTable<K, V> dt, String path, boolean overwrite, WireFormatType<K> bundleK,
			WireFormatType<V> bundleV) {

		return com.nicta.scoobi.io.text.TextOutput.toTextFile(
				dt.getImpl(),
				path,
				overwrite,
				WireFormats.wireFormatPair(bundleK, bundleV).typeInfo());
	}

}
