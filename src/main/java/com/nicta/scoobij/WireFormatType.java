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

/**
 * Gives runtime information based on the type. If you have a DList<Integer>'s
 * you'll often need a WireFormatType<Integer>, this is largely because Java's
 * type erasure -- but also contains a wireFormat which says how to serialize
 * and deserialize this type
 * */
public interface WireFormatType<T> {
	public Class<T> typeInfo();

	public com.nicta.scoobi.WireFormat<T> wireFormat();
}
