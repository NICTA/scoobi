package com.nicta.scoobi.impl

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

import runtime.AbstractFunction1

private[nicta]
class ExtractorAdaptor[T](private val extractor: AnExtractor[T]) extends AbstractFunction1[List[String], T] with scala.PartialFunction[List[String], T] {

  override def apply(strings: List[String]): T = {
    extractor.apply(scala.collection.JavaConversions.asJavaIterable(strings))
  }

  override def isDefinedAt(strings: List[String]): Boolean = {
    extractor.apply(scala.collection.JavaConversions.asJavaIterable(strings)) != null
  }
}

private[nicta]
trait AnExtractor[T] {
  def apply(strings: java.lang.Iterable[String]): T
}