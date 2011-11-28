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
import com.nicta.scoobij.Extractor;
import com.nicta.scoobij.WireFormatType;
import com.nicta.scoobij.impl.Conversions;
import com.nicta.scoobij.impl.DListImpl;

public class TextInput {
	public static DList<String> fromTextFile(String path) {

		return new DListImpl<String>(
				com.nicta.scoobi.io.text.TextInput$.MODULE$.fromTextFile(path));
	}

	public static <T> DList<T> extractFromDelimitedTextFile(String seperator,
			String path, Extractor<T> extractor, WireFormatType<T> bundle) {

		return new DListImpl<T>(
				com.nicta.scoobi.io.text.TextInput$.MODULE$
						.extractFromDelimitedTextFile(seperator, path,
								Conversions.toScala(extractor),
								Conversions.toManifest(bundle.typeInfo()),
								bundle.wireFormat()));
	}
}
