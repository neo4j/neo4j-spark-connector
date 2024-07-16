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

import org.apache.spark.sql.connector.expressions.aggregate.Count
import org.apache.spark.sql.connector.expressions.aggregate.Max
import org.apache.spark.sql.connector.expressions.aggregate.Min
import org.apache.spark.sql.connector.expressions.aggregate.Sum
import org.junit.After
import org.junit.Assert
import org.junit.FixMethodOrder
import org.junit.Test
import org.junit.runners.MethodSorters
import org.neo4j.spark.SparkConnectorScalaSuiteWithGdsBase
import org.neo4j.spark.util.DriverCache
import org.neo4j.spark.util.DummyNamedReference
import org.neo4j.spark.util.Neo4jOptions

@FixMethodOrder(MethodSorters.JVM)
class Neo4jQueryServiceIT extends SparkConnectorScalaSuiteWithGdsBase {

  @After
  def cleanUp(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)
    new DriverCache(neo4jOptions.connection, "").close()
  }

  @Test
  def testShouldDoAggregationOnGDS(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
    options.put("gds", "gds.pageRank.stream")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val field = new DummyNamedReference("score")
    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        Array.empty,
        PartitionPagination.EMPTY,
        List(
          "nodeId",
          "MAX(score)",
          "MIN(score)",
          "COUNT(score)",
          "COUNT(DISTINCT score)",
          "SUM(score)",
          "SUM(DISTINCT score)"
        ),
        Array(
          new Max(field),
          new Min(field),
          new Sum(field, false),
          new Count(field, false),
          new Count(field, true),
          new Sum(field, false),
          new Sum(field, true)
        )
      )
    ).createQuery()

    Assert.assertEquals(
      """CALL gds.pageRank.stream($graphName)
        |YIELD nodeId, score
        |RETURN nodeId AS nodeId, max(score) AS `MAX(score)`, min(score) AS `MIN(score)`, count(score) AS `COUNT(score)`, count(DISTINCT score) AS `COUNT(DISTINCT score)`, sum(score) AS `SUM(score)`, sum(DISTINCT score) AS `SUM(DISTINCT score)`"""
        .stripMargin
        .replaceAll("\n", " "),
      query
    )
  }

}
