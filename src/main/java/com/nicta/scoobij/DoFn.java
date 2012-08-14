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
 * Used by DList.parallelDo to process data. Setup is called before the any
 * elements. Then process is called for each element And then cleanup is called
 * (giving you the option to emit anything)
 */
abstract public class DoFn<A, B> {

	public void setup() {
	}

	public abstract void process(A input, Emitter<B> emitter);

	public void cleanup(Emitter<B> emitter) {
	}
}
