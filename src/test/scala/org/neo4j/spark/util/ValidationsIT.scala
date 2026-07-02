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

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import org.neo4j.driver.AccessMode
import org.neo4j.spark.testsupport.SparkConnectorScalaSuiteIT
import org.neo4j.spark.testsupport.SparkConnectorScalaSuiteIT.neo4j

class ValidationsIT extends SparkConnectorScalaSuiteIT {

  @Test
  def testReadQueryShouldBeSyntacticallyInvalid(): Unit = {
    // given
    val query = "MATCH (f{) RETURN f"
    val readOpts = Map(
      Neo4jOptions.URL -> SparkConnectorScalaSuiteIT.server.getBoltUrl,
      "query" -> query
    )

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
    val readOpts = Map(
      Neo4jOptions.URL -> SparkConnectorScalaSuiteIT.server.getBoltUrl,
      "query" -> query
    )

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
    val readOpts = Map(
      Neo4jOptions.URL -> SparkConnectorScalaSuiteIT.server.getBoltUrl,
      "query" -> "MATCH (f) RETURN f",
      "query.count" -> query
    )

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
  def testReadScriptShouldNotContainAnInvalidQuery(): Unit = {
    val readOpts = Map(
      Neo4jOptions.URL -> SparkConnectorScalaSuiteIT.server.getBoltUrl,
      "query" -> "MATCH (f) RETURN f",
      "script" -> "RETUR 1 AS one;"
    )

    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Validations.validate(ValidateRead(neo4j, new Neo4jOptions(readOpts), "1"))
      }
    )

    assertTrue(
      exception.getMessage.contains(
        "The following script queries are not valid,"
      )
    )

    assertTrue(
      exception.getMessage.contains(
        "Query not compiled for the following exception: ClientException: Invalid input "
      )
    )

    assertTrue(
      exception.getMessage.contains(
        "EXPLAIN RETUR 1 AS one;"
      )
    )
  }

  @Test
  def testReadScriptShouldNotContainMultipleQueries(): Unit = {
    val readOpts = Map(
      Neo4jOptions.URL -> SparkConnectorScalaSuiteIT.server.getBoltUrl,
      "query" -> "MATCH (f) RETURN f",
      "script" -> "RETURN 1 AS one; RETURN 2 AS two; RETURN 3 AS three;"
    )

    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Validations.validate(ValidateRead(neo4j, new Neo4jOptions(readOpts), "1"))
      }
    )

    assertTrue(
      exception.getMessage.contains(
        "The following script queries are not valid,"
      )
    )

    assertTrue(
      exception.getMessage.contains(
        "Query not compiled for the following exception: ClientException: Expected exactly one statement per query but got: "
      )
    )

    assertTrue(
      exception.getMessage.contains(
        "EXPLAIN RETURN 1 AS one; RETURN 2 AS two; RETURN 3 AS three;"
      )
    )
  }

  @Test
  def testReadIndexedScriptShouldNotContainAnInvalidQuery(): Unit = {
    val readOpts = Map(
      Neo4jOptions.URL -> SparkConnectorScalaSuiteIT.server.getBoltUrl,
      "query" -> "MATCH (f) RETURN f",
      "script.1" -> "RETURN 1 AS one",
      "script.2" -> "RETUR 2 AS two"
    )

    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Validations.validate(ValidateRead(neo4j, new Neo4jOptions(readOpts), "1"))
      }
    )

    assertTrue(
      exception.getMessage.contains(
        "The following script queries are not valid,"
      )
    )

    assertTrue(
      exception.getMessage.contains(
        "EXPLAIN RETUR 2 AS two"
      )
    )
  }

  @Test
  def testWriteScriptShouldNotContainAnInvalidQuery(): Unit = {
    val writeOpts = Map(
      Neo4jOptions.URL -> SparkConnectorScalaSuiteIT.server.getBoltUrl,
      Neo4jOptions.ACCESS_MODE -> AccessMode.WRITE.toString,
      "query" -> "CREATE (n:Person);",
      "script" -> "RETUR 1 AS one;"
    )

    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Validations.validate(ValidateWrite(neo4j, new Neo4jOptions(writeOpts), "1", null))
      }
    )

    assertTrue(
      exception.getMessage.contains(
        "The following script query is not valid, please check the syntax: RETUR 1 AS one;"
      )
    )
  }

  @Test
  def testWriteScriptShouldNotContainMultipleQueries(): Unit = {
    val writeOpts = Map(
      Neo4jOptions.URL -> SparkConnectorScalaSuiteIT.server.getBoltUrl,
      Neo4jOptions.ACCESS_MODE -> AccessMode.WRITE.toString,
      "query" -> "CREATE (n:Person);",
      "script" -> "RETURN 1 AS one; RETURN 2 AS two; RETURN 3 AS three;"
    )

    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Validations.validate(ValidateWrite(neo4j, new Neo4jOptions(writeOpts), "1", null))
      }
    )

    assertTrue(
      exception.getMessage.contains(
        "The following script query is not valid, please check the syntax: RETURN 1 AS one; RETURN 2 AS two; RETURN 3 AS three;"
      )
    )
  }

  @Test
  def testWriteIndexedScriptShouldNotContainAnInvalidQuery(): Unit = {
    val writeOpts = Map(
      Neo4jOptions.URL -> SparkConnectorScalaSuiteIT.server.getBoltUrl,
      Neo4jOptions.ACCESS_MODE -> AccessMode.WRITE.toString,
      "query" -> "CREATE (n:Person);",
      "script.1" -> "RETURN 1 AS one",
      "script.2" -> "RETUR 2 AS two"
    )

    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Validations.validate(ValidateWrite(neo4j, new Neo4jOptions(writeOpts), "1", null))
      }
    )

    assertTrue(
      exception.getMessage.contains(
        "The following script query is not valid, please check the syntax: RETUR 2 AS two"
      )
    )
  }

  @Test
  def testWriteQueryShouldBeSyntacticallyInvalid(): Unit = {
    // given
    val query = "MERGE (f{) RETURN f"
    val writeOpts = Map(
      Neo4jOptions.URL -> SparkConnectorScalaSuiteIT.server.getBoltUrl,
      Neo4jOptions.ACCESS_MODE -> AccessMode.WRITE.toString,
      "query" -> query
    )

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
    val writeOpts = Map(
      Neo4jOptions.URL -> SparkConnectorScalaSuiteIT.server.getBoltUrl,
      Neo4jOptions.ACCESS_MODE -> AccessMode.WRITE.toString,
      "query" -> query
    )

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

  @Test
  def testVersionThrowsExceptionSparkVersionIsNotSupported(): Unit = {
    val sparkVersion = SparkConnectorScalaSuiteIT.ss.version
    SparkSession.setActiveSession(SparkConnectorScalaSuiteIT.ss)
    try {
      val exception = assertThrows(
        classOf[IllegalArgumentException],
        () => Validations.validate(ValidateSparkMinVersion("4.10000"))
      )
      assertEquals(
        s"""Your current Spark version $sparkVersion is not supported by the current connector.
           |Please visit https://neo4j.com/developer/spark/overview/#_spark_compatibility to know which connector version you need.
           |""".stripMargin,
        exception.getMessage
      )
    } finally {
      SparkSession.clearActiveSession()
    }
  }

  @Test
  def testVersionShouldBeValid(): Unit = {
    val fullVersion = SparkSession
      .getDefaultSession
      .map(_.version)
      .getOrElse("3.2")
    val baseVersion = fullVersion
      .split("\\.")
      .take(2)
      .mkString(".")
    Validations.validate(ValidateSparkMinVersion(s"$baseVersion.*"))
    Validations.validate(ValidateSparkMinVersion(fullVersion))
    Validations.validate(ValidateSparkMinVersion(s"$fullVersion-amzn-0"))
  }

  @Test
  def testVersionShouldValidateTheVersion(): Unit = {
    val version = ValidateSparkMinVersion("2.3.0")
    assertTrue(version.isSupported("2.3.0-amzn-1"))
    assertTrue(version.isSupported("2.3.1-amzn-1"))
    assertTrue(version.isSupported("3.3.0-amzn-1"))
    assertTrue(version.isSupported("3.3.0"))
    assertTrue(version.isSupported("3.1.0"))
    assertTrue(version.isSupported("3.2.0"))
    assertFalse(version.isSupported("2.2.10"))
  }

}
