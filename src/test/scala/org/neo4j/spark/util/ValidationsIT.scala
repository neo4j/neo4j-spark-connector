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

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.neo4j.driver.AccessMode
import org.neo4j.spark.testsupport.SparkConnectorScalaSuiteIT
import org.neo4j.spark.testsupport.SparkConnectorScalaSuiteIT.neo4j

class ValidationsIT extends SparkConnectorScalaSuiteIT {

  @Test
  def testReadQueryShouldBeSyntacticallyInvalid(): Unit = {
    // given
    val readOpts: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    val query = "MATCH (f{) RETURN f"
    readOpts.put(Neo4jOptions.URL, SparkConnectorScalaSuiteIT.server.getBoltUrl)
    readOpts.put("query", query)

    // when & then
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Validations.validate(ValidateRead(neo4j, new Neo4jOptions(readOpts), "1"))
      }
    )
    assertTrue(
      exception.getMessage.contains(
        "Query not compiled for the following exception: ClientException: Invalid input "
      )
    )
    assertTrue(
      exception.getMessage.contains(query)
    )
  }

  @Test
  def testReadQueryShouldBeSemanticallyInvalid(): Unit = {
    // given
    val query = "MERGE (n:TestNode{id: 1}) RETURN n"
    val readOpts: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    readOpts.put(Neo4jOptions.URL, SparkConnectorScalaSuiteIT.server.getBoltUrl)
    readOpts.put("query", query)

    // when & then
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Validations.validate(ValidateRead(neo4j, new Neo4jOptions(readOpts), "1"))
      }
    )
    assertTrue(
      exception.getMessage.contains(
        s"Invalid query `$query` because the accepted types are [READ_ONLY], but the actual type is READ_WRITE"
      )
    )
  }

  @Test
  def testReadQueryCountBeSyntacticallyInvalid(): Unit = {
    // given
    val query = "MATCH (f{) RETURN f"
    val readOpts: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    readOpts.put(Neo4jOptions.URL, SparkConnectorScalaSuiteIT.server.getBoltUrl)
    readOpts.put("query", "MATCH (f) RETURN f")
    readOpts.put("query.count", query)

    // when & then
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Validations.validate(ValidateRead(neo4j, new Neo4jOptions(readOpts), "1"))
      }
    )
    assertTrue(
      exception.getMessage.contains(
        "Query count not compiled for the following exception: ClientException: Invalid input "
      )
    )
    assertTrue(
      exception.getMessage.contains(s"EXPLAIN $query")
    )
  }

  @Test
  def testScriptQueryCountShouldContainAnInvalidQuery(): Unit = {
    // given
    val readOpts: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    readOpts.put(Neo4jOptions.URL, SparkConnectorScalaSuiteIT.server.getBoltUrl)
    readOpts.put("query", "MATCH (f) RETURN f")
    readOpts.put("script", "RETURN 1 AS one; RETUR 2 AS two; RETURN 3 AS three")

    // when & then
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Validations.validate(ValidateRead(neo4j, new Neo4jOptions(readOpts), "1"))
      }
    )
    assertTrue(
      exception.getMessage.contains(
        "The following queries inside the `script` are not valid,"
      )
    )

    assertTrue(
      exception.getMessage.contains(
        "Query not compiled for the following exception: ClientException: Invalid input "
      )
    )

    assertTrue(
      exception.getMessage.contains(
        "EXPLAIN RETUR 2 AS two"
      )
    )

  }

  @Test
  def testWriteQueryShouldBeSyntacticallyInvalid(): Unit = {
    // given
    val query = "MERGE (f{) RETURN f"
    val writeOpts: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    writeOpts.put(Neo4jOptions.URL, SparkConnectorScalaSuiteIT.server.getBoltUrl)
    writeOpts.put(Neo4jOptions.ACCESS_MODE, AccessMode.WRITE.toString)
    writeOpts.put("query", query)

    // when & then
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Validations.validate(ValidateWrite(neo4j, new Neo4jOptions(writeOpts), "1", null))
      }
    )
    assertTrue(
      exception.getMessage.contains(
        "Query not compiled for the following exception: ClientException: Invalid input "
      )
    )
    assertTrue(
      exception.getMessage.contains(query)
    )
  }

  @Test
  def testWriteQueryShouldBeSemanticallyInvalid(): Unit = {
    // given
    val query = "MATCH (n:TestNode{id: 1}) RETURN n"
    val writeOpts: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    writeOpts.put(Neo4jOptions.URL, SparkConnectorScalaSuiteIT.server.getBoltUrl)
    writeOpts.put(Neo4jOptions.ACCESS_MODE, AccessMode.WRITE.toString)
    writeOpts.put("query", query)

    // when & then
    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Validations.validate(ValidateWrite(neo4j, new Neo4jOptions(writeOpts), "1", null))
      }
    )
    assertTrue(
      exception.getMessage.contains(
        s"Invalid query `$query` because the accepted types are [WRITE_ONLY, READ_WRITE], but the actual type is READ_ONLY"
      )
    )
  }

}
