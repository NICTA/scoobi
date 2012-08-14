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
package com.nicta.scoobij;

/**
 * This is a function is used by DList.combine to reduce a set of element. Using
 * a Combiner is more efficient than parallelDo or flatMap, but has the
 * additional restriction of being commutative. It also should be pure, and not
 * access any state outside of itself
 *
 * @param <T>
 */
public interface Combiner<T> {
	public T apply(T t1, T t2);
}
