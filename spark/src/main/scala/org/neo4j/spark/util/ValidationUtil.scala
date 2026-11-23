/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.spark.util

object ValidationUtil {

  def isNotEmpty(str: String, message: String) = if (str.isEmpty) {
    throw new IllegalArgumentException(message)
  }

  def isNotBlank(str: String, message: String) = if (str.trim.isEmpty) {
    throw new IllegalArgumentException(message)
  }

  def isBlank(str: String, message: String) = if (!str.trim.isEmpty) {
    throw new IllegalArgumentException(message)
  }

  def isNotEmpty(seq: Seq[_], message: String) = if (seq.isEmpty) {
    throw new IllegalArgumentException(message)
  }

  def isNotEmpty(map: Map[_, _], message: String) = if (map.isEmpty) {
    throw new IllegalArgumentException(message)
  }

  def isTrue(boolean: Boolean, message: String) = if (!boolean) {
    throw new IllegalArgumentException(message)
  }

  def isFalse(boolean: Boolean, message: String) = if (boolean) {
    throw new IllegalArgumentException(message)
  }

  def isNotValid(message: String) = throw new IllegalArgumentException(message)
}
