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

case class DfTestCase[ToType](
  name: String,
  df: DataFrame,
  expectedValue: ToType,
  expectedType: String,
  accessor: Value => ToType
)

case class SqlTestCase[ToType](
  name: String,
  sql: String,
  expectedValue: ToType,
  expectedType: String,
  accessor: Value => ToType
)

@InjectNeo4jContainerParameter
@DisplayName("writing")
class WriteIT {

  private val col = "node_property"

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

    val cases = Seq(
      DfTestCase("String to String", Seq("test").toDF(col), "test", "STRING", _.asString()),
      DfTestCase("Long to Int", Seq(1234567890L).toDF(col), 1234567890L, "INTEGER", _.asLong()),
      DfTestCase("Int to Int", Seq(1234567890).toDF(col), 1234567890, "INTEGER", _.asInt()),
      DfTestCase("Short to Int", Seq(12345.toShort).toDF(col), 12345, "INTEGER", _.asInt()),
      DfTestCase("Byte to Int", Seq(123.toByte).toDF(col), 123, "INTEGER", _.asInt()),
      DfTestCase("Double to Float", Seq(123.45).toDF(col), 123.45, "FLOAT", _.asDouble()),
      DfTestCase("Float to Float", Seq(123.5f).toDF(col), 123.5, "FLOAT", _.asDouble()),
      DfTestCase(
        "Decimal to String",
        Seq(BigDecimal("5.42")).toDF(col),
        "5.420000000000000000",
        "STRING",
        _.asString()
      ),
      DfTestCase("Boolean to Boolean", Seq(true).toDF(col), true, "BOOLEAN", _.asBoolean()),
      DfTestCase(
        "Date to Date",
        Seq(LocalDate.of(2022, 1, 1)).toDF(col),
        LocalDate.of(2022, 1, 1),
        "DATE",
        _.asLocalDate()
      ),
      DfTestCase(
        "Instant to ZonedDateTime",
        Seq(Instant.ofEpochSecond(1337)).toDF(col),
        ZonedDateTime.ofInstant(Instant.ofEpochSecond(1337), ZoneOffset.UTC),
        "ZONED DATETIME",
        _.asZonedDateTime()
      ),
      DfTestCase(
        "LocalDateTime to LocalDateTime",
        Seq(LocalDateTime.of(2022, 1, 1, 12, 0)).toDF(col),
        LocalDateTime.of(2022, 1, 1, 12, 0),
        "LOCAL DATETIME",
        _.asLocalDateTime()
      ),
      DfTestCase(
        "Duration to Duration",
        Seq(Duration.ofDays(42)).toDF(col),
        new InternalIsoDuration(0, 42, 0, 0),
        "DURATION",
        _.asIsoDuration()
      ),
      DfTestCase(
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
  def should_write_array_to_neo4j(driver: Driver, spark: SparkSession, neo4j: Neo4j): Stream[DynamicTest] = {

    import spark.implicits._ // to import .toDF()

    def arr[T: Encoder](value: T): DataFrame =
      Seq(value)
        .toDF("_value")
        .select(array($"_value").as(col))

    val cases = Seq(
      DfTestCase("String[] to String[]", arr("test"), "test", "STRING", _.asString()),
      DfTestCase("Long[] to Int[]", arr(1234567890L), 1234567890L, "INTEGER", _.asLong()),
      DfTestCase("Int[] to Int[]", arr(1234567890), 1234567890, "INTEGER", _.asInt()),
      DfTestCase("Short[] to Int[]", arr(12345.toShort), 12345, "INTEGER", _.asInt()),
      DfTestCase("Byte[] to Int[]", arr(123.toByte), 123, "INTEGER", _.asInt()),
      DfTestCase("Double[] to Float[]", arr(123.45), 123.45, "FLOAT", _.asDouble()),
      DfTestCase("Float[] to Float[]", arr(123.5f), 123.5, "FLOAT", _.asDouble()),
      DfTestCase("Decimal[] to String[]", arr(BigDecimal("5.42")), "5.420000000000000000", "STRING", _.asString()),
      DfTestCase("Boolean[] to Boolean[]", arr(true), true, "BOOLEAN", _.asBoolean()),
      DfTestCase("Date[] to Date[]", arr(LocalDate.of(2022, 1, 1)), LocalDate.of(2022, 1, 1), "DATE", _.asLocalDate()),
      DfTestCase(
        "Instant[] to ZonedDateTime[]",
        arr(Instant.ofEpochSecond(1337)),
        ZonedDateTime.ofInstant(Instant.ofEpochSecond(1337), ZoneOffset.UTC),
        "ZONED DATETIME",
        _.asZonedDateTime()
      ),
      DfTestCase(
        "LocalDateTime[] to LocalDateTime[]",
        arr(LocalDateTime.of(2022, 1, 1, 12, 0)),
        LocalDateTime.of(2022, 1, 1, 12, 0),
        "LOCAL DATETIME",
        _.asLocalDateTime()
      ),
      DfTestCase(
        "Duration[] to Duration[]",
        arr(Duration.ofDays(42)),
        new InternalIsoDuration(0, 42, 0, 0),
        "DURATION",
        _.asIsoDuration()
      ),
      DfTestCase(
        "Period[] to Duration[]",
        arr(Period.ofMonths(5)),
        new InternalIsoDuration(5, 0, 0, 0),
        "DURATION",
        _.asIsoDuration()
      ),
      DfTestCase(
        "REAL[] to Float[]",
        spark.sql("SELECT ARRAY(CAST(5.5 AS REAL)) AS node_property"),
        5.5f,
        "FLOAT",
        _.asFloat()
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

          assertThat(actualType).isEqualTo(s"LIST<${testCase.expectedType} NOT NULL> NOT NULL")
          assertThat(actualValue).isEqualTo(testCase.expectedValue)
        }
      )
    }
      .asJava.stream()
  }

  val sqlCases = Seq(
    SqlTestCase(
      "STRING/VARCHAR/CHAR to String",
      s"""
         |SELECT $col
         |FROM VALUES
         |  ('sql'),
         |  (CAST('sql' AS VARCHAR(3))),
         |  (CAST('sql' AS CHAR(3)))
         |AS t($col)""".stripMargin,
      "sql",
      "STRING",
      _.asString()
    ),
    SqlTestCase(
      "VALUE/LONG/BIGINT to Integer",
      s"""
         |SELECT $col
         |FROM VALUES
         |  (1234567890L),
         |  (CAST(1234567890L AS BIGINT)),
         |  (CAST(1234567890L AS LONG))
         |AS t($col)""".stripMargin,
      1234567890L,
      "INTEGER",
      _.asLong()
    ),
    SqlTestCase(
      "VALUE/INTEGER/INT to Integer",
      s"""
         |SELECT $col
         |FROM VALUES
         |  (123456789),
         |  (CAST(123456789 AS INTEGER)),
         |  (CAST(123456789 AS INT))
         |AS t($col)""".stripMargin,
      123456789,
      "INTEGER",
      _.asInt()
    ),
    SqlTestCase(
      "SHORT/SMALLINT to Integer",
      s"""
         |SELECT $col
         |FROM VALUES
         |  (CAST(2345 AS SHORT)),
         |  (CAST(2345 AS SMALLINT))
         |AS t($col)""".stripMargin,
      2345,
      "INTEGER",
      _.asInt()
    ),
    SqlTestCase(
      "BYTE/TINYINT to Integer",
      s"""
         |SELECT $col
         |FROM VALUES
         |  (CAST(123 AS BYTE)),
         |  (CAST(123 AS TINYINT))
         |AS t($col)""".stripMargin,
      123,
      "INTEGER",
      _.asInt()
    ),
    SqlTestCase(
      "FLOAT/REAL to Float",
      s"""
         |SELECT $col
         |FROM VALUES
         |  (CAST(123.5 AS FLOAT)),
         |  (CAST(123.5 AS REAL))
         |AS t($col)""".stripMargin,
      123.5f,
      "FLOAT",
      _.asFloat()
    ),
    SqlTestCase(
      "DECIMAL/DEC/NUMERIC to String",
      s"""
         |SELECT $col
         |FROM VALUES
         |  (CAST (66.66667 AS DECIMAL(10, 2))),
         |  (CAST (66.66667 AS DEC(10, 2))),
         |  (CAST (66.66667 AS NUMERIC(10, 2)))
         |AS t($col)""".stripMargin,
      "66.67",
      "STRING",
      _.asString()
    ),
    SqlTestCase("BOOLEAN to Boolean", s"SELECT TRUE as $col", true, "BOOLEAN", _.asBoolean()),
    SqlTestCase(
      "DATE to Date",
      s"SELECT DATE '2011-11-11' AS $col",
      LocalDate.of(2011, 11, 11),
      "DATE",
      _.asLocalDate()
    ),
    SqlTestCase(
      "TIMESTAMP/TIMESTAMP_LTZ to ZonedDateTime",
      s"""
         |SELECT $col
         |FROM VALUES
         |  (CAST('1988-10-04 13:33:00.000+04:30' AS TIMESTAMP)),
         |  (CAST('1988-10-04 13:33:00.000+04:30' AS TIMESTAMP_LTZ))
         |AS t($col)""".stripMargin,
      ZonedDateTime.of(1988, 10, 4, 9, 3, 0, 0, ZoneOffset.UTC), // expect to normalize as UTC!
      "ZONED DATETIME",
      _.asZonedDateTime()
    ),
    SqlTestCase(
      "TIMESTAMP_NTZ to LocalDateTime",
      s"SELECT CAST('2022-01-01 12:00:00' AS TIMESTAMP_NTZ) AS $col",
      LocalDateTime.of(2022, 1, 1, 12, 0),
      "LOCAL DATETIME",
      _.asLocalDateTime()
    ),
    SqlTestCase(
      "INTERVAL DAY/TIME to Duration",
      s"""
         |SELECT $col
         |FROM VALUES
         |  (INTERVAL '1' DAY),
         |  (INTERVAL '24' HOUR),
         |  (INTERVAL '1440' MINUTE),
         |  (INTERVAL '86400' SECOND)
         |AS t($col)""".stripMargin,
      new InternalIsoDuration(0, 1L, 0, 0),
      "DURATION",
      _.asIsoDuration()
    ),
    SqlTestCase(
      "INTERVAL DAY to Duration",
      s"""
         |SELECT $col
         |FROM VALUES
         |  (INTERVAL '10 05' DAY TO HOUR),
         |  (INTERVAL '10 05:00' DAY TO MINUTE),
         |  (INTERVAL '10 05:00:00' DAY TO SECOND)
         |AS t($col)""".stripMargin,
      new InternalIsoDuration(0, 10L, 5L * 3600, 0),
      "DURATION",
      _.asIsoDuration()
    ),
    SqlTestCase(
      "INTERVAL HOUR to Duration",
      s"""
         |SELECT $col
         |FROM VALUES
         |  (INTERVAL '3' HOUR),
         |  (INTERVAL '3:00' HOUR TO MINUTE),
         |  (INTERVAL '3:00:00' HOUR TO SECOND)
         |AS t($col)""".stripMargin,
      new InternalIsoDuration(0, 0, 3L * 3600, 0),
      "DURATION",
      _.asIsoDuration()
    ),
    SqlTestCase(
      "INTERVAL MINUTE to Duration",
      s"SELECT INTERVAL '13:37' MINUTE TO SECOND AS $col",
      new InternalIsoDuration(0, 0, 13L * 60L + 37L, 0),
      "DURATION",
      _.asIsoDuration()
    ),
    SqlTestCase(
      "INTERVAL YEAR to Duration",
      s"SELECT INTERVAL '3' YEAR AS $col",
      new InternalIsoDuration(3L * 12L, 0, 0, 0),
      "DURATION",
      _.asIsoDuration()
    ),
    SqlTestCase(
      "INTERVAL MONTH to Duration",
      s"SELECT INTERVAL '7' MONTH AS $col",
      new InternalIsoDuration(7L, 0, 0, 0),
      "DURATION",
      _.asIsoDuration()
    ),
    SqlTestCase(
      "INTERVAL YEAR TO MONTH to Duration",
      s"SELECT INTERVAL '4-5' YEAR TO MONTH AS $col",
      new InternalIsoDuration(4L * 12L + 5L, 0, 0, 0),
      "DURATION",
      _.asIsoDuration()
    )
  )

  @TestFactory
  def should_write_sql_to_neo4j(driver: Driver, spark: SparkSession, neo4j: Neo4j): Stream[DynamicTest] = {
    sqlCases.map { testCase =>
      dynamicTest(
        testCase.name,
        () => {
          val label = testCase.name.replace(" ", "_")
          spark.sql(testCase.sql).write.format(classOf[DataSource].getName).mode(SaveMode.Append)
            .option("url", neo4jContainer.getBoltUrl)
            .option("labels", label)
            .save()

          val fetchQuery: String = s"MATCH (n:`$label`) RETURN n.$col AS value, valueType(n.$col) AS type"
          val records = driver.session().run(fetchQuery).list()

          records.forEach(record => {
            val actualValue = testCase.accessor(record.get("value"))
            val actualType = record.get("type").asString()

            assertThat(actualType).isEqualTo(s"${testCase.expectedType} NOT NULL")
            assertThat(actualValue).isEqualTo(testCase.expectedValue)
          })
        }
      )
    }
      .asJava.stream()
  }

  @TestFactory
  def should_write_sql_array_to_neo4j(driver: Driver, spark: SparkSession, neo4j: Neo4j): Stream[DynamicTest] = {
    def arrayTransform(df: DataFrame): DataFrame = {
      import org.apache.spark.sql.functions.collect_list
      df.select(collect_list(col).as(col))
    }

    sqlCases.map { testCase =>
      dynamicTest(
        testCase.name,
        () => {
          val label = testCase.name.replace(" ", "_")
          arrayTransform(spark.sql(testCase.sql)).write.format(classOf[DataSource].getName).mode(SaveMode.Append)
            .option("url", neo4jContainer.getBoltUrl)
            .option("labels", label)
            .save()

          val fetchQuery: String = s"MATCH (n:`$label`) RETURN n.$col AS value, valueType(n.$col) AS type"
          val record = driver.session().run(fetchQuery).single()
          val actualValue = testCase.accessor(record.get("value").get(0))
          val actualType = record.get("type").asString()

          assertThat(actualType).isEqualTo(s"LIST<${testCase.expectedType} NOT NULL> NOT NULL")
          assertThat(actualValue).isEqualTo(testCase.expectedValue)
        }
      )
    }
      .asJava.stream()
  }
}
