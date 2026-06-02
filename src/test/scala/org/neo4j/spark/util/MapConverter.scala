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

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

object MapConverter {

  def toScala(value: Any, extraValueConverter: Any => Any = identity): Any = value match {
    case map: java.util.Map[_, _] =>
      map.asScala.map { case (key, value) => key -> toScala(extraValueConverter(value), extraValueConverter) }.toMap
    case list: java.util.List[_] =>
      list.asScala.map(value => toScala(extraValueConverter(value), extraValueConverter)).toList
    case other =>
      extraValueConverter(other)
  }
}
