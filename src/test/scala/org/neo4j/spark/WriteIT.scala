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

import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.junit.jupiter.params.Parameter
import org.neo4j.caniuse.Neo4j
import org.neo4j.driver.Driver
import org.neo4j.driver.Value
import org.neo4j.driver.internal.InternalIsoDuration
import org.neo4j.driver.internal.InternalPoint2D
import org.neo4j.driver.internal.InternalPoint3D
import org.neo4j.driver.types.IsoDuration
import org.neo4j.driver.types.Point
import org.neo4j.spark.testsupport.InjectNeo4jContainerParameter
import org.testcontainers.neo4j.Neo4jContainer

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetTime
import java.time.Period
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.stream.Stream

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

case class ExceptionTestCase(
  name: String,
  expectedException: Class[_ <: Throwable] = classOf[Exception],
  expectedMessage: String,
  block: ThrowingCallable
)

case class DfTestCase[ToType](
  name: String,
  df: DataFrame,
  expectedValue: ToType,
  expectedType: ExpectedNeo4jType[ToType]
)

case class SqlTestCase[ToType](
  name: String,
  sql: String,
  expectedValue: ToType,
  expectedType: ExpectedNeo4jType[ToType]
)

case class ExpectedNeo4jType[T](
  name: String,
  accessor: Value => T
)

@InjectNeo4jContainerParameter
@DisplayName("writing")
class WriteIT {

  import WriteIT._

  @Parameter
  var neo4jContainer: Neo4jContainer = _

  @TestFactory
  def should_handle_error_states(driver: Driver, spark: SparkSession, neo4j: Neo4j): Stream[DynamicTest] = {
    import spark.implicits._

    val exampleDf = Seq(
      (12, "John Bonham", "Drums"),
      (19, "John Mayer", "Guitar"),
      (32, "John Scofield", "Guitar"),
      (15, "John Butler", "Guitar")
    ).toDF("experience", "name", "instrument")

    val cases = Seq(
      ExceptionTestCase(
        "SaveMode.Overwrite require node.keys",
        classOf[IllegalArgumentException],
        "node.keys is required when Save Mode is Overwrite",
        () => {
          spark.sql("SELECT 42 AS element").write.format("neo4j")
            .mode(SaveMode.Overwrite)
            .option("labels", "A")
            .save()
        }
      ),
      ExceptionTestCase(
        "custom query should not be read-only",
        classOf[IllegalArgumentException],
        "Invalid query `MATCH (o:Object) RETURN o` because the accepted types are [WRITE_ONLY, READ_WRITE], but the actual type is READ_ONLY",
        () => {
          spark.sql("SELECT 42 AS element").write.format("neo4j")
            .mode(SaveMode.Append)
            .option("query", "MATCH (o:Object) RETURN o")
            .save()
        }
      ),
      ExceptionTestCase(
        "propagate validation error when 'ErrorIfExists' is used in rel save modes",
        classOf[IllegalArgumentException],
        "This connector does not support save mode 'ErrorIfExists'. Use save mode 'Append' instead.",
        () => {
          exampleDf.repartition(1).write
            .format(classOf[DataSource].getName)
            .mode(SaveMode.Overwrite)
            .option("relationship", "PLAYS")
            .option("relationship.source.save.mode", "ErrorIfExists")
            .option("relationship.target.save.mode", "Overwrite")
            .option("relationship.source.labels", ":Musician")
            .option("relationship.source.node.keys", "name:name")
            .option("relationship.target.labels", ":Instrument")
            .option("relationship.target.node.keys", "instrument:name")
            .save()
        }
      ),
      ExceptionTestCase(
        "should propagate client exceptions via spark exceptions",
        classOf[SparkException],
        "org.neo4j.driver.exceptions.ClientException",
        () => {
          driver.session().run("CREATE CONSTRAINT id FOR (o:Object) REQUIRE o.id IS UNIQUE").consume()
          driver.session().run("CREATE (o:Object {id: 42, description: 'ball'})").consume()

          Seq((42, "cube")).toDF("id", "description").write.format("neo4j")
            .mode(SaveMode.Append)
            .option("labels", "Object")
            .save()
        }
      ),
      ExceptionTestCase(
        "must have valid native schema in native mode",
        classOf[SparkException],
        "NATIVE write strategy requires a schema like: rel.[props], source.[props], target.[props]. " +
          "All of these columns are empty in the current schema.",
        () => {
          exampleDf.write.format("neo4j")
            .mode(SaveMode.Append)
            .option("relationship", "PLAYS")
            .option("relationship.save.strategy", "native")
            .option("relationship.source.labels", ":Person")
            .option("relationship.source.save.mode", "Overwrite")
            .option("relationship.target.labels", ":Instrument")
            .option("relationship.target.save.mode", "Overwrite")
            .save()
        }
      )
    )

    cases.map { testCase =>
      dynamicTest(
        testCase.name,
        () => {
          SparkSession.setActiveSession(spark)
          val exceptionAssertion = assertThatThrownBy(testCase.block)
          exceptionAssertion.isInstanceOf(testCase.expectedException)
          exceptionAssertion.hasMessageContaining(testCase.expectedMessage)
        }
      )
    }
      .asJava.stream()
  }

  @Nested
  @DisplayName("happy path tests")
  @Execution(ExecutionMode.SAME_THREAD)
  class WriteHappyPathIT {

    @Test
    def should_skip_null_properties(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {

      import spark.implicits._

      SparkSession.setActiveSession(spark)
      val df = Seq(("Banana", null)).toDF("fruit", "color")
      df.write.format("neo4j")
        .mode(SaveMode.Append)
        .option("labels", "Fruit")
        .save()

      val fetchQuery = "MATCH (n:Fruit) RETURN n"
      val got = driver.session().run(fetchQuery).single().asMap()

      assertThat(got).doesNotContainKey("color")
    }

    @Test
    def should_handle_write_with_partitions(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      SparkSession.setActiveSession(spark)
      val df = spark.createDataFrame(
        (0 to 99).map(i => (i, 99 - i))
      ).toDF("a", "b").repartition(10)

      df.write.format("neo4j")
        .mode(SaveMode.Append)
        .option("labels", "Node")
        .option("batch.size", "11")
        .save()

      val record = driver.session().run("MATCH (n:Node) RETURN count(n) AS count, keys(n) AS keys").single()
      val count = record.get("count").asInt()
      val keys = record.get("keys").asList[String](_.asString()).asScala.toList

      assertThat(count).isEqualTo(df.count())
      assertThat(keys).isEqualTo(List("a", "b"))
    }

    @Test
    def should_insert_data_with_custom_query(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      SparkSession.setActiveSession(spark)
      val df = spark.createDataFrame(
        (0 to 99).map(i => (i, 99 - i))
      ).toDF("a", "b")

      df.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Append)
        .option("query", "CREATE (q:Query {first: event.a, second: event.b})")
        .option("batch.size", "11")
        .save()

      val got = driver.session().run("MATCH (q:Query) RETURN count(q) AS count").single().get("count").asLong()
      assertThat(got).isEqualTo(df.count())
    }

    @Test
    def should_properly_overwrite_when_already_existing(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      import spark.implicits._

      SparkSession.setActiveSession(spark)
      driver.session().run("CREATE CONSTRAINT person_surname FOR (p:Person) REQUIRE p.surname IS UNIQUE").consume()
      driver.session().run("CREATE (p:Person {name: 'Alice', surname: 'Doe'})").consume()

      Seq(("John", "Doe")).toDF("name", "surname").write.format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("labels", "Person")
        .option("node.keys", "surname")
        .save()

      val got = driver.session().run("MATCH (p:Person) RETURN p.name").list().asScala.map(_.get(0).asString()).toList

      assertThat(got.size).isEqualTo(1)
      assertThat(got).isEqualTo(List("John"))
    }

    @Test
    def should_handle_unusual_column_names(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      val nameColumn = 4
      val specialColumn = 5
      val instrumentColumn = 8

      import spark.implicits._

      SparkSession.setActiveSession(spark)
      val df = Seq(
        (12, "John Bonham", "Drums", "f``````oo"),
        (19, "John Mayer", "Guitar", "bar"),
        (32, "John Scofield", "Guitar", "ba` z"),
        (15, "John Butler", "Guitar", "qu   ux")
      ).toDF("experience", "name", "instrument", "fi``(╯°□°)╯︵ ┻━┻eld")

      df.write.format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("relationship", "PLAYS")
        .option("relationship.source.labels", ":Musician")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.node.keys", "name")
        .option("relationship.source.node.properties", "fi``(╯°□°)╯︵ ┻━┻eld:field")
        .option("relationship.target.labels", ":Instrument")
        .option("relationship.target.node.keys", "instrument:name")
        .option("relationship.target.save.mode", "Overwrite")
        .save()

      val got = spark.read.format("neo4j")
        .option("relationship", "PLAYS")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Musician")
        .option("relationship.target.labels", ":Instrument")
        .load()
        .orderBy("`source.name`")
        .collectAsList()

      assertThat(got).hasSize(4)
      assertThat(got.get(0).getString(nameColumn)).isEqualTo("John Bonham")
      assertThat(got.get(0).getString(specialColumn)).isEqualTo("f``````oo")
      assertThat(got.get(0).getString(instrumentColumn)).isEqualTo("Drums")

      assertThat(got.get(1).getString(nameColumn)).isEqualTo("John Butler")
      assertThat(got.get(1).getString(specialColumn)).isEqualTo("qu   ux")
      assertThat(got.get(1).getString(instrumentColumn)).isEqualTo("Guitar")

      assertThat(got.get(2).getString(nameColumn)).isEqualTo("John Mayer")
      assertThat(got.get(2).getString(specialColumn)).isEqualTo("bar")
      assertThat(got.get(2).getString(instrumentColumn)).isEqualTo("Guitar")

      assertThat(got.get(3).getString(nameColumn)).isEqualTo("John Scofield")
      assertThat(got.get(3).getString(specialColumn)).isEqualTo("ba` z")
      assertThat(got.get(3).getString(instrumentColumn)).isEqualTo("Guitar")
    }

    @Test
    def should_write_relationship_when_keys_mode(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      val nameColumn = 4
      val instrumentColumn = 7

      import spark.implicits._

      SparkSession.setActiveSession(spark)
      val df = Seq(
        (12, "John Bonham", "Drums"),
        (19, "John Mayer", "Guitar"),
        (32, "John Scofield", "Guitar"),
        (15, "John Butler", "Guitar")
      ).toDF("experience", "name", "instrument")

      df.repartition(1).write.format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("relationship", "PLAYS")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.source.labels", ":Musician")
        .option("relationship.source.node.keys", "name:name")
        .option("relationship.target.labels", ":Instrument")
        .option("relationship.target.node.keys", "instrument:name")
        .save()

      val got = spark.read.format("neo4j")
        .option("relationship", "PLAYS")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Musician")
        .option("relationship.target.labels", ":Instrument")
        .load()
        .orderBy("`source.name`")
        .collectAsList()

      assertThat(got).hasSize(4)
      assertThat(got.get(0).getString(nameColumn)).isEqualTo("John Bonham")
      assertThat(got.get(0).getString(instrumentColumn)).isEqualTo("Drums")

      assertThat(got.get(1).getString(nameColumn)).isEqualTo("John Butler")
      assertThat(got.get(1).getString(instrumentColumn)).isEqualTo("Guitar")

      assertThat(got.get(2).getString(nameColumn)).isEqualTo("John Mayer")
      assertThat(got.get(2).getString(instrumentColumn)).isEqualTo("Guitar")

      assertThat(got.get(3).getString(nameColumn)).isEqualTo("John Scofield")
      assertThat(got.get(3).getString(instrumentColumn)).isEqualTo("Guitar")
    }

    // TODO: insert the relationship tests here
  }

  @Nested
  @DisplayName("auto type mapping")
  @Execution(ExecutionMode.CONCURRENT)
  class WriteTypeMappingIT {

    @TestFactory
    def should_write_jvm_to_neo4j(driver: Driver, spark: SparkSession, neo4j: Neo4j): Stream[DynamicTest] = {
      TypeTestCases.dfCases(spark).map { testCase =>
        val label = testCase.name
        dynamicTest(
          label,
          () => {
            SparkSession.setActiveSession(testCase.df.sparkSession)

            testCase.df.write.format("neo4j")
              .mode(SaveMode.Append)
              .option("labels", label)
              .save()

            val fetchQuery = s"MATCH (n:`$label`) RETURN n.$col AS value, valueType(n.$col) AS type"
            val record = driver.session().run(fetchQuery).single()
            val actualType = record.get("type").asString()
            val actualValue = testCase.expectedType.accessor(record.get("value"))

            assertThat(actualType).isEqualTo(s"${testCase.expectedType.name} NOT NULL")
            assertThat(actualValue).isEqualTo(testCase.expectedValue)
          }
        )
      }
        .asJava.stream()
    }

    @TestFactory
    def should_write_jvm_array_to_neo4j(driver: Driver, spark: SparkSession, neo4j: Neo4j): Stream[DynamicTest] = {
      val cases = TypeTestCases.dfCases(spark)
        .filter(!_.name.toLowerCase.contains("byte")) // byte array special because it's binary type

      cases.map { testCase =>
        val label = "[" + testCase.name + "]"
        dynamicTest(
          label,
          () => {
            val df = testCase.df.select(collect_list(col).as(col)) // wrap in array length 1
            SparkSession.setActiveSession(df.sparkSession)

            df.write.format("neo4j")
              .mode(SaveMode.Append)
              .option("labels", label)
              .save()

            val fetchQuery = s"MATCH (n:`$label`) RETURN n.$col AS array, valueType(n.$col) AS type"
            val record = driver.session().run(fetchQuery).single()
            val actualType = record.get("type").asString()
            val actualValue = testCase.expectedType.accessor(record.get("array").get(0))

            assertThat(actualType).isEqualTo(s"LIST<${testCase.expectedType.name} NOT NULL> NOT NULL")
            assertThat(actualValue).isEqualTo(testCase.expectedValue)
          }
        )
      }
        .asJava.stream()
    }

    @TestFactory
    def should_write_sql_to_neo4j(driver: Driver, spark: SparkSession, neo4j: Neo4j): Stream[DynamicTest] = {
      TypeTestCases.sqlCases.map { testCase =>
        val label = testCase.name
        dynamicTest(
          label,
          () => {
            val df = spark.sql(testCase.sql)
            SparkSession.setActiveSession(df.sparkSession)

            df.write.format("neo4j")
              .mode(SaveMode.Append)
              .option("labels", label)
              .save()

            val fetchQuery: String = s"MATCH (n:`$label`) RETURN n.$col AS value, valueType(n.$col) AS type"
            val records = driver.session().run(fetchQuery).list()

            records.forEach(record => {
              val actualType = record.get("type").asString()
              val actualValue = testCase.expectedType.accessor(record.get("value"))

              assertThat(actualType).isEqualTo(s"${testCase.expectedType.name} NOT NULL")
              assertThat(actualValue).isEqualTo(testCase.expectedValue)
            })
          }
        )
      }
        .asJava.stream()
    }

    @TestFactory
    def should_write_sql_array_to_neo4j(driver: Driver, spark: SparkSession, neo4j: Neo4j): Stream[DynamicTest] = {
      val cases = TypeTestCases.sqlCases
        .filter(!_.name.toLowerCase.contains("byte")) // byte array special because it's binary type

      cases.map { testCase =>
        val label = "[" + testCase.name + "]"
        dynamicTest(
          label,
          () => {
            val df = spark.sql(testCase.sql).select(collect_list(col).as(col)) // wrap in array length 1
            SparkSession.setActiveSession(df.sparkSession)

            df.write.format("neo4j")
              .mode(SaveMode.Append)
              .option("labels", label)
              .save()

            val fetchQuery: String = s"MATCH (n:`$label`) RETURN n.$col AS array, valueType(n.$col) AS type"
            val record = driver.session().run(fetchQuery).single()
            val actualType = record.get("type").asString()
            val actualValue = testCase.expectedType.accessor(record.get("array").get(0))

            assertThat(actualType).isEqualTo(s"LIST<${testCase.expectedType.name} NOT NULL> NOT NULL")
            assertThat(actualValue).isEqualTo(testCase.expectedValue)
          }
        )
      }
        .asJava.stream()
    }

    @Test
    def should_write_jvm_binary_to_neo4j(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      SparkSession.setActiveSession(spark)
      val binary = "message".getBytes(StandardCharsets.UTF_8)
      val df = spark.createDataset(Seq(binary))(Encoders.BINARY).toDF(col)
      df.write.format("neo4j").mode(SaveMode.Append).option("labels", "BinaryJvm").save()

      val fetchQuery: String = s"MATCH (n:BinaryJvm) RETURN n.$col AS binary, valueType(n.$col) AS type"
      val record = driver.session().run(fetchQuery).single()
      val actualType = record.get("type").asString()
      val actualValue = record.get("binary").asByteArray()

      assertThat(actualType).isEqualTo("LIST<INTEGER NOT NULL> NOT NULL")
      assertThat(actualValue).isEqualTo(binary)
    }

    @Test
    def should_write_sql_binary_to_neo4j(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      SparkSession.setActiveSession(spark)
      val expectedBinary = "message".getBytes(StandardCharsets.UTF_8)

      spark.sql(s"SELECT CAST('message' AS BINARY) AS $col").write.format("neo4j")
        .mode(SaveMode.Append)
        .option("labels", "BinarySql")
        .save()

      val fetchQuery: String = s"MATCH (n:BinarySql) RETURN n.$col AS binary, valueType(n.$col) AS type"
      val record = driver.session().run(fetchQuery).single()
      val actualType = record.get("type").asString()
      val actualValue = record.get("binary").asByteArray()

      assertThat(actualType).isEqualTo("LIST<INTEGER NOT NULL> NOT NULL")
      assertThat(actualValue).isEqualTo(expectedBinary)
    }

    @TestFactory
    def should_write_duration_struct_as_duration(
      driver: Driver,
      spark: SparkSession,
      neo4j: Neo4j
    ): Stream[DynamicTest] = {
      TypeTestCases.structDurationCases(spark).map { testCase =>
        val label = testCase.expectedValue.toString
        dynamicTest(
          label,
          () => {
            SparkSession.setActiveSession(testCase.df.sparkSession)
            testCase.df.write.format("neo4j")
              .mode(SaveMode.Append)
              .option("labels", label)
              .save()

            val fetchQuery = s"MATCH (n:`$label`) RETURN n.$col AS duration, valueType(n.$col) AS type"
            val record = driver.session().run(fetchQuery).single()
            val actualType = record.get("type").asString()
            val actualValue = testCase.expectedType.accessor(record.get("duration"))

            assertThat(actualType).isEqualTo(s"${testCase.expectedType.name} NOT NULL")
            assertThat(actualValue).isEqualTo(testCase.expectedValue)
          }
        )
      }
        .asJava.stream()
    }

    @TestFactory
    def should_write_point_struct_as_point(
      driver: Driver,
      spark: SparkSession,
      neo4j: Neo4j
    ): Stream[DynamicTest] = {
      TypeTestCases.structPointCases(spark).map { testCase =>
        val label = testCase.name
        dynamicTest(
          label,
          () => {
            SparkSession.setActiveSession(testCase.df.sparkSession)
            testCase.df.write.format("neo4j")
              .mode(SaveMode.Append)
              .option("labels", label)
              .save()

            val fetchQuery = s"MATCH (n:`$label`) RETURN n.$col AS point, valueType(n.$col) AS type"
            val record = driver.session().run(fetchQuery).single()
            val actualType = record.get("type").asString()
            val actualValue = testCase.expectedType.accessor(record.get("point"))

            assertThat(actualType).isEqualTo(s"${testCase.expectedType.name} NOT NULL")
            assertThat(actualValue).isEqualTo(testCase.expectedValue)

          }
        )
      }
        .asJava.stream()
    }

    @TestFactory
    def should_write_local_time_struct_as_local_time(
      driver: Driver,
      spark: SparkSession,
      neo4j: Neo4j
    ): Stream[DynamicTest] = {
      TypeTestCases.structLocalTimeCases(spark).map { testCase =>
        val label = testCase.name
        dynamicTest(
          label,
          () => {
            SparkSession.setActiveSession(testCase.df.sparkSession)
            testCase.df.write.format("neo4j")
              .mode(SaveMode.Append)
              .option("labels", label)
              .save()

            val fetchQuery = s"MATCH (n: `$label`) RETURN n.$col AS time, valueType(n.$col) AS type"
            val record = driver.session().run(fetchQuery).single()
            val actualType = record.get("type").asString()
            val actualValue = testCase.expectedType.accessor(record.get("time"))

            assertThat(actualType).isEqualTo(s"${testCase.expectedType.name} NOT NULL")
            assertThat(actualValue).isEqualTo(testCase.expectedValue)
          }
        )
      }
        .asJava.stream()
    }

    @TestFactory
    def should_write_offset_time_struct_as_offset_time(
      driver: Driver,
      spark: SparkSession,
      neo4j: Neo4j
    ): Stream[DynamicTest] = {
      TypeTestCases.structOffsetTimeCases(spark).map { testCase =>
        val label = testCase.name
        dynamicTest(
          label,
          () => {
            SparkSession.setActiveSession(testCase.df.sparkSession)
            testCase.df.write.format("neo4j")
              .mode(SaveMode.Append)
              .option("labels", label)
              .save()

            val fetchQuery = s"MATCH (n: `$label`) RETURN n.$col AS time, valueType(n.$col) AS type"
            val record = driver.session().run(fetchQuery).single()
            val actualType = record.get("type").asString()
            val actualValue = testCase.expectedType.accessor(record.get("time"))

            assertThat(actualType).isEqualTo(s"${testCase.expectedType.name} NOT NULL")
            assertThat(actualValue).isEqualTo(testCase.expectedValue)
          }
        )
      }
        .asJava.stream()
    }

    @TestFactory
    def should_write_duration_struct_array_as_duration_array(
      driver: Driver,
      spark: SparkSession,
      neo4j: Neo4j
    ): Stream[DynamicTest] = {
      TypeTestCases.structDurationCases(spark).map { testCase =>
        val label = "[" + testCase.expectedValue.toString + "]"
        dynamicTest(
          label,
          () => {
            val df = testCase.df.select(collect_list(col).as(col))
            SparkSession.setActiveSession(df.sparkSession)
            df.write.format("neo4j")
              .mode(SaveMode.Append)
              .option("labels", label)
              .save()

            val fetchQuery = s"MATCH (n:`$label`) RETURN n.$col AS durationArray, valueType(n.$col) AS type"
            val record = driver.session().run(fetchQuery).single()
            val actualType = record.get("type").asString()
            val actualValue = testCase.expectedType.accessor(record.get("durationArray").get(0))

            assertThat(actualType).isEqualTo(s"LIST<${testCase.expectedType.name} NOT NULL> NOT NULL")
            assertThat(actualValue).isEqualTo(testCase.expectedValue)
          }
        )
      }
        .asJava.stream()
    }

    @TestFactory
    def should_write_point_struct_array_as_point_array(
      driver: Driver,
      spark: SparkSession,
      neo4j: Neo4j
    ): Stream[DynamicTest] = {
      TypeTestCases.structPointCases(spark).map { testCase =>
        val label = "[" + testCase.name + "]"
        dynamicTest(
          label,
          () => {
            val df = testCase.df.select(collect_list(col).as(col))
            SparkSession.setActiveSession(df.sparkSession)
            df.write.format("neo4j")
              .mode(SaveMode.Append)
              .option("labels", label)
              .save()

            val fetchQuery = s"MATCH (n:`$label`) RETURN n.$col AS pointArray, valueType(n.$col) AS type"
            val record = driver.session().run(fetchQuery).single()
            val actualType = record.get("type").asString()
            val actualValue = testCase.expectedType.accessor(record.get("pointArray").get(0))

            assertThat(actualType).isEqualTo(s"LIST<${testCase.expectedType.name} NOT NULL> NOT NULL")
            assertThat(actualValue).isEqualTo(testCase.expectedValue)

          }
        )
      }
        .asJava.stream()
    }

    @TestFactory
    def should_write_local_time_struct_array_as_local_time_array(
      driver: Driver,
      spark: SparkSession,
      neo4j: Neo4j
    ): Stream[DynamicTest] = {
      TypeTestCases.structLocalTimeCases(spark).map { testCase =>
        val label = "[" + testCase.name + "]"
        dynamicTest(
          label,
          () => {
            val df = testCase.df.select(collect_list(col).as(col))
            SparkSession.setActiveSession(df.sparkSession)
            df.write.format("neo4j")
              .mode(SaveMode.Append)
              .option("labels", label)
              .save()

            val fetchQuery = s"MATCH (n: `$label`) RETURN n.$col AS timeArray, valueType(n.$col) AS type"
            val record = driver.session().run(fetchQuery).single()
            val actualType = record.get("type").asString()
            val actualValue = testCase.expectedType.accessor(record.get("timeArray").get(0))

            assertThat(actualType).isEqualTo(s"LIST<${testCase.expectedType.name} NOT NULL> NOT NULL")
            assertThat(actualValue).isEqualTo(testCase.expectedValue)
          }
        )
      }
        .asJava.stream()
    }

    @TestFactory
    def should_write_offset_time_struct_array_as_offset_time_array(
      driver: Driver,
      spark: SparkSession,
      neo4j: Neo4j
    ): Stream[DynamicTest] = {
      TypeTestCases.structOffsetTimeCases(spark).map { testCase =>
        val label = "[" + testCase.name + "]"
        dynamicTest(
          label,
          () => {
            val df = testCase.df.select(collect_list(col).as(col))
            SparkSession.setActiveSession(df.sparkSession)
            df.write.format("neo4j")
              .mode(SaveMode.Append)
              .option("labels", label)
              .save()

            val fetchQuery = s"MATCH (n: `$label`) RETURN n.$col AS timeArray, valueType(n.$col) AS type"
            val record = driver.session().run(fetchQuery).single()
            val actualType = record.get("type").asString()
            val actualValue = testCase.expectedType.accessor(record.get("timeArray").get(0))

            assertThat(actualType).isEqualTo(s"LIST<${testCase.expectedType.name} NOT NULL> NOT NULL")
            assertThat(actualValue).isEqualTo(testCase.expectedValue)
          }
        )
      }
        .asJava.stream()
    }
  }

  object WriteIT {
    val col = "prop"
  }

  /**
   * This companion object contains re-usable test cases. Test procedures are in the actual test class above.
   * Re-using test cases allows us to test the same test construct outside an array and then also inside an array.
   */
  object TypeTestCases {

    case class DurationStruct(months: Long, days: Long, seconds: Long, nanoseconds: Int) {
      val `type` = "duration"

      val asSparkSchema = StructType(Array(StructField(
        "duration",
        StructType(Array(
          StructField("type", DataTypes.StringType, nullable = false),
          StructField("months", DataTypes.LongType, nullable = false),
          StructField("days", DataTypes.LongType, nullable = false),
          StructField("seconds", DataTypes.LongType, nullable = false),
          StructField("nanoseconds", DataTypes.IntegerType, nullable = false)
        )),
        nullable = false
      )))
    }

    case class PointStruct(`type`: String, srid: Int, x: Double, y: Double, z: Option[Double] = None) {

      val asSparkSchema = StructType(Array(StructField(
        "point",
        StructType(Array(
          StructField("type", DataTypes.StringType, nullable = false),
          StructField("srid", DataTypes.IntegerType, nullable = false),
          StructField("x", DataTypes.DoubleType, nullable = false),
          StructField("y", DataTypes.DoubleType, nullable = false),
          StructField("z", DataTypes.DoubleType, nullable = true)
        )),
        nullable = false
      )))
    }

    case class TimeStruct(`type`: String, value: String) {

      val asSparkSchema = StructType(Array(StructField(
        "time",
        StructType(Array(
          StructField("type", DataTypes.StringType, nullable = false),
          StructField("value", DataTypes.StringType, nullable = false)
        )),
        nullable = false
      )))
    }

    def dfCases(spark: SparkSession): Seq[DfTestCase[_]] = {
      import spark.implicits._ // to import .toDF()

      Seq(
        DfTestCase("String to String", Seq("test").toDF(col), "test", Neo4jString),
        DfTestCase("Long to Int", Seq(1234567890L).toDF(col), 1234567890L, Neo4jLong),
        DfTestCase("Int to Int", Seq(1234567890).toDF(col), 1234567890, Neo4jInteger),
        DfTestCase("Short to Int", Seq(12345.toShort).toDF(col), 12345, Neo4jInteger),
        DfTestCase("Byte to Int", Seq(123.toByte).toDF(col), 123, Neo4jInteger),
        DfTestCase("Double to Float", Seq(123.45).toDF(col), 123.45, Neo4jFloatAsDouble),
        DfTestCase("Float to Float", Seq(123.5f).toDF(col), 123.5f, Neo4jFloat),
        DfTestCase("Decimal to String", Seq(BigDecimal("5.42")).toDF(col), "5.420000000000000000", Neo4jString),
        DfTestCase("Boolean to Boolean", Seq(true).toDF(col), true, Neo4jBoolean),
        DfTestCase("Date to Date", Seq(LocalDate.of(2022, 1, 1)).toDF(col), LocalDate.of(2022, 1, 1), Neo4jDate),
        DfTestCase(
          "Instant to ZonedDateTime",
          Seq(Instant.ofEpochSecond(1337)).toDF(col),
          ZonedDateTime.ofInstant(Instant.ofEpochSecond(1337), ZoneOffset.UTC),
          Neo4jZonedDateTime
        ),
        DfTestCase(
          "LocalDateTime to LocalDateTime",
          Seq(LocalDateTime.of(2022, 1, 1, 12, 0)).toDF(col),
          LocalDateTime.of(2022, 1, 1, 12, 0),
          Neo4jLocalDateTime
        ),
        DfTestCase(
          "Duration to Duration",
          Seq(Duration.ofDays(42)).toDF(col),
          new InternalIsoDuration(0, 42, 0, 0),
          Neo4jDuration
        ),
        DfTestCase(
          "Period to Duration",
          Seq(Period.ofMonths(5)).toDF(col),
          new InternalIsoDuration(5, 0, 0, 0),
          Neo4jDuration
        )
      )
    }

    val sqlCases: Seq[SqlTestCase[_]] = Seq(
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
        Neo4jString
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
        Neo4jLong
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
        Neo4jInteger
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
        Neo4jInteger
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
        Neo4jInteger
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
        Neo4jFloat
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
        Neo4jString
      ),
      SqlTestCase("BOOLEAN to Boolean", s"SELECT TRUE as $col", true, Neo4jBoolean),
      SqlTestCase("DATE to Date", s"SELECT DATE '2011-11-11' AS $col", LocalDate.of(2011, 11, 11), Neo4jDate),
      SqlTestCase(
        "TIMESTAMP/TIMESTAMP_LTZ to ZonedDateTime",
        s"""
           |SELECT $col
           |FROM VALUES
           |  (CAST('1988-10-04 13:33:00.000+04:30' AS TIMESTAMP)),
           |  (CAST('1988-10-04 13:33:00.000+04:30' AS TIMESTAMP_LTZ))
           |AS t($col)""".stripMargin,
        ZonedDateTime.of(1988, 10, 4, 9, 3, 0, 0, ZoneOffset.UTC), // expect to normalize as UTC!
        Neo4jZonedDateTime
      ),
      SqlTestCase(
        "TIMESTAMP_NTZ to LocalDateTime",
        s"SELECT CAST('2022-01-01 12:00:00' AS TIMESTAMP_NTZ) AS $col",
        LocalDateTime.of(2022, 1, 1, 12, 0),
        Neo4jLocalDateTime
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
        Neo4jDuration
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
        Neo4jDuration
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
        Neo4jDuration
      ),
      SqlTestCase(
        "INTERVAL MINUTE to Duration",
        s"SELECT INTERVAL '13:37' MINUTE TO SECOND AS $col",
        new InternalIsoDuration(0, 0, 13L * 60L + 37L, 0),
        Neo4jDuration
      ),
      SqlTestCase(
        "INTERVAL YEAR to Duration",
        s"SELECT INTERVAL '3' YEAR AS $col",
        new InternalIsoDuration(3L * 12L, 0, 0, 0),
        Neo4jDuration
      ),
      SqlTestCase(
        "INTERVAL MONTH to Duration",
        s"SELECT INTERVAL '7' MONTH AS $col",
        new InternalIsoDuration(7L, 0, 0, 0),
        Neo4jDuration
      ),
      SqlTestCase(
        "INTERVAL YEAR TO MONTH to Duration",
        s"SELECT INTERVAL '4-5' YEAR TO MONTH AS $col",
        new InternalIsoDuration(4L * 12L + 5L, 0, 0, 0),
        Neo4jDuration
      )
    )

    def structDurationCases(spark: SparkSession): Seq[DfTestCase[IsoDuration]] = {
      val structsToExpectedIso = Map[DurationStruct, InternalIsoDuration](
        DurationStruct(months = 1, days = 0, seconds = 8, nanoseconds = 0) -> new InternalIsoDuration(1L, 0, 8L, 0),
        DurationStruct(months = 0, days = 55, seconds = 0, nanoseconds = 0) -> new InternalIsoDuration(0L, 55L, 0L, 0),
        DurationStruct(
          months = 1,
          days = 55,
          seconds = 23,
          nanoseconds = 666000
        ) -> new InternalIsoDuration(1L, 55L, 23L, 666000),
        DurationStruct(months = 2, days = 2, seconds = 3600, nanoseconds = 87870000) -> new InternalIsoDuration(
          2L,
          2L,
          3600L,
          87870000
        )
      )

      structsToExpectedIso.map { case (givenStruct, expectedIso) =>
        val row: Row = Row(Row(
          givenStruct.`type`,
          givenStruct.months,
          givenStruct.days,
          givenStruct.seconds,
          givenStruct.nanoseconds
        ))
        DfTestCase(
          expectedIso.toString,
          spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(row)),
            givenStruct.asSparkSchema
          ).toDF(col),
          expectedIso,
          Neo4jDuration
        )
      }.toSeq
    }

    def structPointCases(spark: SparkSession): Seq[DfTestCase[Point]] = {
      val structsToExpectedPoints = Map[PointStruct, Point](
        PointStruct("point-2d", 4326, 1, 3) -> new InternalPoint2D(4326, 1, 3),
        PointStruct("point-2d", 4326, 2, 4) -> new InternalPoint2D(4326, 2, 4),
        PointStruct("point-3d", 4979, 1, 4, Some(7d)) -> new InternalPoint3D(4979, 1, 4, 7),
        PointStruct("point-3d", 4979, 2, 5, Some(8d)) -> new InternalPoint3D(4979, 2, 5, 8),
        PointStruct("point-3d", 4979, 3, 6, Some(9d)) -> new InternalPoint3D(4979, 3, 6, 9)
      )

      structsToExpectedPoints.map { case (givenStruct, expectedPoint) =>
        val row = Row(Row(givenStruct.`type`, givenStruct.srid, givenStruct.x, givenStruct.y, givenStruct.z.orNull))
        DfTestCase(
          "Point (" + givenStruct.`type` + "), x = " + givenStruct.x.toString,
          spark.createDataFrame(spark.sparkContext.parallelize(Seq(row)), givenStruct.asSparkSchema).toDF(col),
          expectedPoint,
          Neo4jPoint
        )
      }.toSeq
    }

    def structLocalTimeCases(spark: SparkSession): Seq[DfTestCase[LocalTime]] = {
      val structsToExpectedLocalTime = Map[TimeStruct, LocalTime](
        TimeStruct("local-time", "12:34:56") -> LocalTime.of(12, 34, 56),
        TimeStruct("local-time", "23:59:59") -> LocalTime.of(23, 59, 59),
        TimeStruct("local-time", "12:50:33.556000000") -> LocalTime.parse("12:50:33.556000000"),
        TimeStruct("local-time", "15:47:26.000") -> LocalTime.parse("15:47:26.000")
      )

      structsToExpectedLocalTime.map { case (givenStruct, expectedLocalTime) =>
        val row = Row(Row(givenStruct.`type`, givenStruct.value))
        DfTestCase(
          "LocalTime_" + givenStruct.value.replace(":", "_"), // prevent ":" label separation
          spark.createDataFrame(spark.sparkContext.parallelize(Seq(row)), givenStruct.asSparkSchema).toDF(col),
          expectedLocalTime,
          Neo4jLocalTime
        )
      }.toSeq
    }

    def structOffsetTimeCases(spark: SparkSession): Seq[DfTestCase[OffsetTime]] = {
      val structsToExpectedLocalTime = Map[TimeStruct, OffsetTime](
        TimeStruct("offset-time", "12:34:56+01:00") -> OffsetTime.of(LocalTime.of(12, 34, 56), ZoneOffset.of("+01:00")),
        TimeStruct("offset-time", "12:34:56Z") -> OffsetTime.of(LocalTime.of(12, 34, 56), ZoneOffset.UTC),
        TimeStruct("offset-time", "23:59:59+02:00") -> OffsetTime.of(LocalTime.of(23, 59, 59), ZoneOffset.of("+02:00")),
        TimeStruct("offset-time", "12:50:35.556000000+01:00") -> OffsetTime.parse("12:50:35.556000000+01:00"),
        TimeStruct("offset-time", "15:47:26.000+02:00") -> OffsetTime.parse("15:47:26.000+02:00"),
        TimeStruct("offset-time", "15:47:26.000Z") -> OffsetTime.parse("15:47:26.000Z")
      )

      structsToExpectedLocalTime.map { case (givenStruct, expectedLocalTime) =>
        val row = Row(Row(givenStruct.`type`, givenStruct.value))
        DfTestCase(
          "OffsetTime_" + givenStruct.value.replace(":", "_"), // prevent ":" label separation
          spark.createDataFrame(spark.sparkContext.parallelize(Seq(row)), givenStruct.asSparkSchema).toDF(col),
          expectedLocalTime,
          Neo4jOffsetTime
        )
      }.toSeq
    }

    lazy val Neo4jString = ExpectedNeo4jType("STRING", _.asString())
    lazy val Neo4jLong = ExpectedNeo4jType("INTEGER", _.asLong())
    lazy val Neo4jInteger = ExpectedNeo4jType("INTEGER", _.asInt())
    lazy val Neo4jFloatAsDouble = ExpectedNeo4jType("FLOAT", _.asDouble())
    lazy val Neo4jFloat = ExpectedNeo4jType("FLOAT", _.asFloat())
    lazy val Neo4jBoolean = ExpectedNeo4jType("BOOLEAN", _.asBoolean())
    lazy val Neo4jDate = ExpectedNeo4jType("DATE", _.asLocalDate())
    lazy val Neo4jZonedDateTime = ExpectedNeo4jType("ZONED DATETIME", _.asZonedDateTime())
    lazy val Neo4jLocalDateTime = ExpectedNeo4jType("LOCAL DATETIME", _.asLocalDateTime())
    lazy val Neo4jDuration = ExpectedNeo4jType("DURATION", _.asIsoDuration())
    lazy val Neo4jPoint = ExpectedNeo4jType("POINT", _.asPoint())
    lazy val Neo4jLocalTime = ExpectedNeo4jType("LOCAL TIME", _.asLocalTime())
    lazy val Neo4jOffsetTime = ExpectedNeo4jType("ZONED TIME", _.asOffsetTime())
  }
}
