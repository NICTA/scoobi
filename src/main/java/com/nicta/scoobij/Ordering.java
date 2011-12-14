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

import java.io.Serializable;

/**
 * Based on scala.Ordering, same idea as Java's comparable but designed more
 * like a "trait"
 *
 */

public abstract class Ordering<T> implements Serializable, com.nicta.scoobi.Grouping<T> {
	private static final long serialVersionUID = 1L;

	public abstract int sortCompare(T a, T b);

	public int partition(T key, int num) {
		return (key.hashCode() & Integer.MAX_VALUE) % num;
	}
	public int groupCompare(T a, T b) {
		return sortCompare(a, b);
	}
}
