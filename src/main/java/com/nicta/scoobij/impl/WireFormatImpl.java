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

import scala.reflect.Manifest;

import com.nicta.scoobi.core.WireFormat;
import com.nicta.scoobij.OrderedWireFormatType;
import com.nicta.scoobij.Ordering;

public class WireFormatImpl<T> implements OrderedWireFormatType<T> {
	public WireFormatImpl(Class<T> c, WireFormat<T> wf,
			Ordering<T> o) {
		clazz = c;
		wireformat = wf;
		order = o;
	}

	@Override
	public Manifest<T> typeInfo() {
		return Conversions.toManifest(clazz);
	}

	@Override
	public WireFormat<T> wireFormat() {
		return wireformat;
	}

	@Override
	public Ordering<T> ordering() {
		return order;
	}

	Class<T> clazz;
	WireFormat<T> wireformat;
	Ordering<T> order;
}
