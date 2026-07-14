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
package org.neo4j.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.array
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.params.Parameter
import org.neo4j.caniuse.Neo4j
import org.neo4j.driver.Driver
import org.neo4j.driver.Value
import org.neo4j.driver.internal.InternalIsoDuration
import org.neo4j.spark.testsupport.InjectNeo4jContainerParameter
import org.testcontainers.neo4j.Neo4jContainer

import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.Period
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.stream.Stream

import scala.jdk.CollectionConverters.SeqHasAsJava

@InjectNeo4jContainerParameter
@DisplayName("writing")
class WriteIT {

  @Parameter
  var neo4jContainer: Neo4jContainer = _

  @Test
  def throws_exception_if_no_valid_read_options_set(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
    assertThatExceptionOfType(classOf[IllegalArgumentException])
      .isThrownBy(() => spark.read.format("neo4j").option("url", neo4jContainer.getBoltUrl).load()).withMessage(
        "No valid option found. One of `GDS`, `LABELS`, `QUERY`, `RELATIONSHIP` is required"
      )
  }

  @Test
  def throws_exception_if_multiple_read_options_set(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
    assertThatExceptionOfType(classOf[IllegalArgumentException])
      .isThrownBy(() =>
        spark.read.format("neo4j").option(
          "url",
          neo4jContainer.getBoltUrl
        ).option("labels", "Person").option("relationship", "KNOWS").load()
      ).withMessage(
        "You need to specify just one of these options: 'gds', 'labels', 'query', 'relationship'"
      )
  }

  @TestFactory
  def should_write_jvm_to_neo4j(driver: Driver, spark: SparkSession, neo4j: Neo4j): Stream[DynamicTest] = {

    import spark.implicits._ // to import .toDF()
    val col = "node_property"

    case class JvmTestCase[ToType](
      name: String,
      df: DataFrame,
      expectedValue: ToType,
      expectedType: String,
      accessor: Value => ToType
    )

    val cases = Seq(
      JvmTestCase("String to String", Seq("test").toDF(col), "test", "STRING", _.asString()),
      JvmTestCase("Long to Int", Seq(1234567890L).toDF(col), 1234567890, "INTEGER", _.asInt()),
      JvmTestCase("Int to Int", Seq(1234567890).toDF(col), 1234567890, "INTEGER", _.asInt()),
      JvmTestCase("Short to Int", Seq(12345.toShort).toDF(col), 12345, "INTEGER", _.asInt()),
      JvmTestCase("Byte to Int", Seq(123.toByte).toDF(col), 123, "INTEGER", _.asInt()),
      JvmTestCase("Double to Float", Seq(123.45).toDF(col), 123.45, "FLOAT", _.asDouble()),
      JvmTestCase("Float to Float", Seq(123.5f).toDF(col), 123.5, "FLOAT", _.asDouble()),
      JvmTestCase(
        "Decimal to String",
        Seq(BigDecimal("5.42")).toDF(col),
        "5.420000000000000000",
        "STRING",
        _.asString()
      ),
      JvmTestCase("Boolean to Boolean", Seq(true).toDF(col), true, "BOOLEAN", _.asBoolean()),
      JvmTestCase(
        "Date to Date",
        Seq(LocalDate.of(2022, 1, 1)).toDF(col),
        LocalDate.of(2022, 1, 1),
        "DATE",
        _.asLocalDate()
      ),
      JvmTestCase(
        "Instant to ZonedDateTime",
        Seq(Instant.ofEpochSecond(1337)).toDF(col),
        ZonedDateTime.ofInstant(Instant.ofEpochSecond(1337), ZoneOffset.UTC),
        "ZONED DATETIME",
        _.asZonedDateTime()
      ),
      JvmTestCase(
        "LocalDateTime to LocalDateTime",
        Seq(LocalDateTime.of(2022, 1, 1, 12, 0)).toDF(col),
        LocalDateTime.of(2022, 1, 1, 12, 0),
        "LOCAL DATETIME",
        _.asLocalDateTime()
      ),
      JvmTestCase(
        "Duration to Duration",
        Seq(Duration.ofDays(42)).toDF(col),
        new InternalIsoDuration(0, 42, 0, 0),
        "DURATION",
        _.asIsoDuration()
      ),
      JvmTestCase(
        "Period to Duration",
        Seq(Period.ofMonths(5)).toDF(col),
        new InternalIsoDuration(5, 0, 0, 0),
        "DURATION",
        _.asIsoDuration()
      )
    )

    cases.map { testCase =>
      dynamicTest(
        testCase.name,
        () => {
          val label = testCase.name.replace(" ", "_")
          testCase.df.write.format(classOf[DataSource].getName).mode(SaveMode.Append)
            .option("url", neo4jContainer.getBoltUrl)
            .option("labels", label)
            .save()

          val fetchQuery = s"MATCH (n:`$label`) RETURN n.$col AS value, valueType(n.$col) AS type"
          val record = driver.session().run(fetchQuery).single()
          val actualValue = testCase.accessor(record.get("value"))
          val actualType = record.get("type").asString()

          assertThat(actualValue).isEqualTo(testCase.expectedValue)
          assertThat(actualType).isEqualTo(s"${testCase.expectedType} NOT NULL")
        }
      )
    }
      .asJava.stream()
  }

  @TestFactory
  def should_write_jvm_array_to_neo4j(driver: Driver, spark: SparkSession, neo4j: Neo4j): Stream[DynamicTest] = {

    import spark.implicits._ // to import .toDF()
    val col = "node_property"

    def arr[T: Encoder](value: T): DataFrame =
      Seq(value)
        .toDF("_value")
        .select(array($"_value").as(col))

    case class JvmArrayTestCase[ToType](
      name: String,
      df: DataFrame,
      expectedInnerValue: ToType,
      expectedInnerType: String,
      accessor: Value => ToType
    )

    val cases = Seq(
      JvmArrayTestCase("String[] to String[]", arr("test"), "test", "STRING", _.asString()),
      JvmArrayTestCase("Long[] to Int[]", arr(1234567890L), 1234567890, "INTEGER", _.asInt()),
      JvmArrayTestCase("Int[] to Int[]", arr(1234567890), 1234567890, "INTEGER", _.asInt()),
      JvmArrayTestCase("Short[] to Int[]", arr(12345.toShort), 12345, "INTEGER", _.asInt()),
      JvmArrayTestCase("Byte[] to Int[]", arr(123.toByte), 123, "INTEGER", _.asInt()),
      JvmArrayTestCase("Double[] to Float[]", arr(123.45), 123.45, "FLOAT", _.asDouble()),
      JvmArrayTestCase("Float[] to Float[]", arr(123.5f), 123.5, "FLOAT", _.asDouble()),
      JvmArrayTestCase(
        "Decimal[] to String[]",
        arr(BigDecimal("5.42")),
        "5.420000000000000000",
        "STRING",
        _.asString()
      ),
      JvmArrayTestCase("Boolean[] to Boolean[]", arr(true), true, "BOOLEAN", _.asBoolean()),
      JvmArrayTestCase(
        "Date[] to Date[]",
        arr(LocalDate.of(2022, 1, 1)),
        LocalDate.of(2022, 1, 1),
        "DATE",
        _.asLocalDate()
      ),
      JvmArrayTestCase(
        "Instant[] to ZonedDateTime[]",
        arr(Instant.ofEpochSecond(1337)),
        ZonedDateTime.ofInstant(Instant.ofEpochSecond(1337), ZoneOffset.UTC),
        "ZONED DATETIME",
        _.asZonedDateTime()
      ),
      JvmArrayTestCase(
        "LocalDateTime[] to LocalDateTime[]",
        arr(LocalDateTime.of(2022, 1, 1, 12, 0)),
        LocalDateTime.of(2022, 1, 1, 12, 0),
        "LOCAL DATETIME",
        _.asLocalDateTime()
      ),
      JvmArrayTestCase(
        "Duration[] to Duration[]",
        arr(Duration.ofDays(42)),
        new InternalIsoDuration(0, 42, 0, 0),
        "DURATION",
        _.asIsoDuration()
      ),
      JvmArrayTestCase(
        "Period[] to Duration[]",
        arr(Period.ofMonths(5)),
        new InternalIsoDuration(5, 0, 0, 0),
        "DURATION",
        _.asIsoDuration()
      )
    )

    cases.map { testCase =>
      dynamicTest(
        testCase.name,
        () => {
          val label = testCase.name.replace(" ", "_")
          testCase.df.write.format(classOf[DataSource].getName).mode(SaveMode.Append)
            .option("url", neo4jContainer.getBoltUrl)
            .option("labels", label)
            .save()

          val fetchQuery = s"MATCH (n:`$label`) RETURN n.$col AS value, valueType(n.$col) AS type"
          val record = driver.session().run(fetchQuery).single()
          val actualValue = testCase.accessor(record.get("value").get(0))
          val actualType = record.get("type").asString()

          assertThat(actualValue).isEqualTo(testCase.expectedInnerValue)
          assertThat(actualType).isEqualTo(s"LIST<${testCase.expectedInnerType} NOT NULL> NOT NULL")
        }
      )
    }
      .asJava.stream()
  }

  @TestFactory
  def should_write_sql_to_neo4j(driver: Driver, spark: SparkSession, neo4j: Neo4j): Stream[DynamicTest] = {
    val col = "node_property"

    case class SqlTestCase[ToType](
      name: String,
      df: DataFrame,
      expectedValue: ToType,
      expectedType: String,
      accessor: Value => ToType
    )

    val cases = Seq(
      SqlTestCase("STRING to String", spark.sql(s"SELECT 'sql' AS $col"), "sql", "STRING", _.asString()),
      SqlTestCase(
        "VARCHAR to String",
        spark.sql(s"SELECT CAST('sql' AS VARCHAR(3)) AS $col"),
        "sql",
        "STRING",
        _.asString()
      ),
      SqlTestCase("CHAR to String", spark.sql(s"SELECT CAST('sql' AS CHAR(3)) AS $col"), "sql", "STRING", _.asString()),
      SqlTestCase("Long to Int", spark.sql(s"SELECT 1234567890L AS $col"), 1234567890, "INTEGER", _.asInt()),
      SqlTestCase("BIGINT to Int", spark.sql(s"SELECT CAST (99 AS BIGINT) AS $col"), 99, "INTEGER", _.asInt()),
      SqlTestCase("LONG to Int", spark.sql(s"SELECT CAST (42 AS LONG) AS $col"), 42, "INTEGER", _.asInt()),
      SqlTestCase("Int to Int", spark.sql(s"SELECT 123456789 AS $col"), 123456789, "INTEGER", _.asInt()),
      SqlTestCase("INTEGER to Int", spark.sql(s"SELECT CAST (55 AS INTEGER) AS $col"), 55, "INTEGER", _.asInt()),
      SqlTestCase("INT to Int", spark.sql(s"SELECT CAST (3 AS INT) AS $col"), 3, "INTEGER", _.asInt()),
      SqlTestCase("SMALLINT to Int", spark.sql(s"SELECT CAST (2345 AS SMALLINT) AS $col"), 2345, "INTEGER", _.asInt()),
      SqlTestCase("SHORT to Int", spark.sql(s"SELECT CAST (12345 AS SHORT) AS $col"), 12345, "INTEGER", _.asInt()),
      SqlTestCase("TINYINT to Int", spark.sql(s"SELECT CAST (123 AS TINYINT) AS $col"), 123, "INTEGER", _.asInt()),
      SqlTestCase("BYTE to Int", spark.sql(s"SELECT CAST (25 AS BYTE) AS $col"), 25, "INTEGER", _.asInt())
    )

    cases.map { testCase =>
      dynamicTest(
        testCase.name,
        () => {
          val label = testCase.name.replace(" ", "_")
          testCase.df.write.format(classOf[DataSource].getName).mode(SaveMode.Append)
            .option("url", neo4jContainer.getBoltUrl)
            .option("labels", label)
            .save()

          val fetchQuery: String = s"MATCH (n:`$label`) RETURN n.$col AS value, valueType(n.$col) AS type"
          val record = driver.session().run(fetchQuery).single()
          val actualValue = testCase.accessor(record.get("value"))
          val actualType = record.get("type").asString()
          assertThat(actualValue).isEqualTo(testCase.expectedValue)
          assertThat(actualType).isEqualTo(s"${testCase.expectedType} NOT NULL")
        }
      )
    }
      .asJava.stream()
  }
}
