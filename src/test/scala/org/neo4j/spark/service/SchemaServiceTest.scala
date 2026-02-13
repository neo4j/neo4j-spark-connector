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
package org.neo4j.spark.service

import org.junit.Assert.assertEquals
import org.junit.Test
import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.mockito.Mockito.mock
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDeploymentType
import org.neo4j.caniuse.Neo4jEdition
import org.neo4j.caniuse.Neo4jVersion
import org.neo4j.spark.config.TopN
import org.neo4j.spark.util.DriverCache
import org.neo4j.spark.util.Neo4jOptions

import scala.annotation.nowarn
import scala.collection.JavaConverters

class SchemaServiceTest {

  @Test
  def does_not_overflow_when_partition_size_is_over_max_value_of_32bit_integers(): Unit = {
    // note: _ cannot be used to separate digit groups, as this requires Scala 2.13+
    val opts = options(
      "url" -> "bolt://example.com",
      "partitions" -> 2.toString,
      "query.count" -> (2L * 2147483648L).toString, // 2 * (Integer.MAX_VALUE + 1)
      "query" -> "MERGE (:Node)"
    )
    val schemaService = new SchemaService(neo4j(), opts, mock(classOf[DriverCache], RETURNS_DEEP_STUBS))

    val pages = schemaService.skipLimitFromPartition(Some(TopN(1024)))

    assertEquals(List(0, 1), pages.map(_.partitionNumber).toList)
    assertEquals(List(0, 2147483648L), pages.map(_.skip).toList)
    assertEquals(List(2147483648L, 2147483648L), pages.map(_.topN.limit).toList)
    assertEquals(List(0, 0), pages.map(_.topN.orders.size).toList)
  }

  private def options(kv: (String, String)*): Neo4jOptions = {
    new Neo4jOptions(
      JavaConverters.mapAsJavaMap(kv.toMap)
    )
  }

  private def neo4j(): Neo4j = {
    new Neo4j(new Neo4jVersion(2025, 1, 0), Neo4jEdition.COMMUNITY, Neo4jDeploymentType.SELF_MANAGED)
  }
}
