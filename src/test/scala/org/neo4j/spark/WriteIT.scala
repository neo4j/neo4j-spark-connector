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

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.SparkException
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.DayTimeIntervalType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.YearMonthIntervalType
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.junit.jupiter.params.Parameter
import org.neo4j.caniuse.CanIUse
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Schema
import org.neo4j.driver.Driver
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.TransactionContext
import org.neo4j.driver.Value
import org.neo4j.driver.internal.InternalIsoDuration
import org.neo4j.driver.internal.InternalPoint2D
import org.neo4j.driver.internal.InternalPoint3D
import org.neo4j.driver.types.IsoDuration
import org.neo4j.driver.types.Point
import org.neo4j.spark.testsupport.Assert
import org.neo4j.spark.testsupport.Closeables.use
import org.neo4j.spark.testsupport.InjectNeo4jContainerParameter
import org.neo4j.spark.testsupport.Neo4jExtensions.DriverExtensions
import org.neo4j.spark.testsupport.RowUtil.getByName
import org.neo4j.spark.testsupport.TestUtil
import org.neo4j.spark.testsupport.Versions
import org.neo4j.spark.util.ConstraintsOptimizationType
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.SchemaConstraintsOptimizationType
import org.neo4j.spark.writer.DataWriterMetrics
import org.testcontainers.neo4j.Neo4jContainer

import java.nio.charset.StandardCharsets
import java.sql.Date
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetTime
import java.time.Period
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.TimeZone
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Stream

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

@InjectNeo4jContainerParameter
@DisplayName("writing")
class WriteIT {

  import WriteIT._

  @Parameter
  var neo4jContainer: Neo4jContainer = _

  @TestFactory
  def should_handle_error_states(driver: Driver, spark: SparkSession, neo4j: Neo4j): Stream[DynamicTest] = {
    import spark.implicits._

    case class ExceptionTestCase(
      name: String,
      expectedException: Class[_ <: Throwable] = classOf[Exception],
      expectedMessage: String,
      block: ThrowingCallable
    )

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
          val thrown = catchThrowable(testCase.block)
          assertThat(thrown).isInstanceOf(testCase.expectedException)
          assertThat(thrown).hasMessageContaining(testCase.expectedMessage)
        }
      )
    }
      .asJava.stream()
  }

  @Nested
  @DisplayName("nodes")
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
        .format("neo4j")
        .mode(SaveMode.Append)
        .option("query", "CREATE (q:Query {first: event.a, second: event.b})")
        .option("batch.size", "11")
        .save()

      val got = driver.session().run("MATCH (q:Query) RETURN count(q) AS count").single().get("count").asLong()
      assertThat(got).isEqualTo(df.count())
    }

    @Test
    def should_insert_data_with_custom_query_with_skip_limit(
      driver: Driver,
      spark: SparkSession,
      neo4j: Neo4j
    ): Unit = {
      SparkSession.setActiveSession(spark)
      val df = spark.createDataFrame(
        (0 to 99).map(i => (i, 99 - i))
      ).toDF("a", "b")

      df.write
        .format("neo4j")
        .mode(SaveMode.Append)
        .option(
          "query",
          "UNWIND [1,2] AS x WITH x, event SKIP 1 LIMIT 1 CREATE (q:Query {first: event.a, second: event.b})"
        )
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
    def should_unpack_maps(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      import spark.implicits._

      SparkSession.setActiveSession(spark)
      Seq(
        ("Foo", 1, Map("key" -> "value1"))
      ).toDF("id", "time", "table").write.format("neo4j")
        .mode(SaveMode.Append)
        .option("labels", ":Map")
        .save()

      val fetchQuery = "MATCH (n:Map) RETURN n.`table.key` AS value"
      val got = driver.session().run(fetchQuery).single().get("value").asString()
      assertThat(got).isEqualTo("value1")
    }

    @Test
    def should_unpack_nested_maps_when_no_collisions(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      import spark.implicits._

      SparkSession.setActiveSession(spark)
      Seq(
        ("Foo", 1, Map("inner" -> Map("key" -> "innerValue1"))),
        ("Bar", 1, Map("inner" -> Map("key" -> "innerValue2")))
      )
        .toDF("id", "time", "table")
        .write.format("neo4j")
        .mode(SaveMode.Append)
        .option("labels", ":NestedMap")
        .save()

      val fetchQuery =
        """
          |MATCH (n:NestedMap)
          |WHERE (
          |  properties(n) = {id: 'Foo', time: 1, `table.inner.key`: 'innerValue1'}
          |  OR properties(n) = {id: 'Bar', time: 1, `table.inner.key`: 'innerValue2'}
          |)
          |RETURN count(n) AS count
          |""".stripMargin

      val gotCount = driver.session().run(fetchQuery).single().get("count").asInt()
      assertThat(gotCount).isEqualTo(2)
    }

    @Test
    def should_unpack_nested_maps_with_collisions(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      import spark.implicits._

      SparkSession.setActiveSession(spark)
      Seq(
        ("Foo", 1, ListMap("key.inner" -> Map("key" -> "innerValue1"), "key" -> Map("inner.key" -> "value1"))),
        ("Bar", 1, ListMap("key.inner" -> Map("key" -> "innerValue2"), "key" -> Map("inner.key" -> "value2")))
      ).toDF("id", "time", "table")
        .write.format("neo4j")
        .mode(SaveMode.Append)
        .option("labels", ":CollisionMap")
        .save()

      val fetchQuery =
        """
          |MATCH (n:CollisionMap)
          |WHERE (
          | properties(n) = {id: 'Foo', time: 1, `table.key.inner.key`: 'value1'}
          | OR properties(n) = {id: 'Foo', time: 1, `table.key.inner.key`: 'innerValue1'}
          | OR properties(n) = {id: 'Bar', time: 1, `table.key.inner.key`: 'value2'}
          | OR properties(n) = {id: 'Bar', time: 1, `table.key.inner.key`: 'innerValue2'}
          |)
          |RETURN count(n) AS count
          |""".stripMargin

      val gotCount = driver.session().run(fetchQuery).single().get("count").asInt()
      assertThat(gotCount).isEqualTo(2)
    }

    @Test
    def should_unpack_nested_maps_with_collisions_and_aggregations(
      driver: Driver,
      spark: SparkSession,
      neo4j: Neo4j
    ): Unit = {
      import spark.implicits._

      SparkSession.setActiveSession(spark)
      Seq(
        ("Foo", 0, ListMap("key.inner" -> Map("key" -> "innerValue0"), "key" -> Map("inner.key" -> "value0"))),
        ("Bar", 0, ListMap("key.inner" -> Map("key" -> "innerValue1"), "key" -> Map("inner.key" -> "value1")))
      ).toDF("id", "time", "table")
        .write.format("neo4j")
        .mode(SaveMode.Append)
        .option("labels", ":AggregatedMap")
        .option("schema.map.group.duplicate.keys", true)
        .save()

      val fetchQuery =
        """
          |MATCH (n:AggregatedMap)
          |WHERE (
          | properties(n) = {id: 'Foo', time: 0, `table.key.inner.key`: ['innerValue0', 'value0']}
          | OR properties(n) = {id: 'Bar', time: 0, `table.key.inner.key`: ['innerValue1', 'value1']}
          |)
          |RETURN count(n) AS count
          |""".stripMargin

      val gotCount = driver.session().run(fetchQuery).single().get("count").asInt()
      assertThat(gotCount).isEqualTo(2)
    }
  }

  @Nested
  @DisplayName("relationships")
  @Execution(ExecutionMode.SAME_THREAD)
  class RelationshipWriteIT {

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

      df.repartition(1).write.format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("relationship", "PLÄYS")
        .option("relationship.source.labels", ":Müsician")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.node.keys", "name")
        .option("relationship.source.node.properties", "fi``(╯°□°)╯︵ ┻━┻eld:field")
        .option("relationship.target.labels", ":Iñstrument")
        .option("relationship.target.node.keys", "instrument:name")
        .option("relationship.target.save.mode", "Overwrite")
        .save()

      val got = spark.read.format("neo4j")
        .option("relationship", "PLÄYS")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Müsician")
        .option("relationship.target.labels", ":Iñstrument")
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

    @TestFactory
    def should_handle_relationship_options_properly(
      driver: Driver,
      spark: SparkSession,
      neo4j: Neo4j
    ): Stream[DynamicTest] = {

      import spark.implicits._

      case class RelationshipPropertyTestCase(
        name: String,
        relationshipProperties: Option[String],
        expectedInclusions: List[Map[String, Any]],
        expectedExclusions: List[String]
      ) {
        val df: DataFrame = Seq(
          (12L, "John Bonham", "Drums", 2L, true),
          (19L, "John Mayer", "Guitar", 1L, false)
        ).toDF("experience", "name", "instrument", "rating", "hasDiploma")

        val relType: String = "PROP_TEST_" + name.toUpperCase().replaceAll(" ", "_")

        private val internalOptions = Map(
          "relationship" -> relType,
          "relationship.source.save.mode" -> "Overwrite",
          "relationship.target.save.mode" -> "Overwrite",
          "relationship.source.labels" -> ":M",
          "relationship.source.node.keys" -> "name",
          "relationship.target.labels" -> ":I",
          "relationship.target.node.keys" -> "instrument:name"
        )

        val options: Map[String, String] = if (relationshipProperties.isDefined) {
          internalOptions ++ Map("relationship.properties" -> relationshipProperties.get)
        } else {
          internalOptions
        }
      }

      val cases = Seq(
        RelationshipPropertyTestCase(
          "should read all properties when not passing any properties",
          None,
          List(
            Map("experience" -> 12L, "rating" -> 2L, "hasDiploma" -> true),
            Map("experience" -> 19L, "rating" -> 1L, "hasDiploma" -> false)
          ),
          List("name", "instrument")
        ),
        RelationshipPropertyTestCase(
          "should do remapping of relationship properties",
          Some("experience:exp, rating:skill, instrument:on"),
          List(
            Map("exp" -> 12L, "skill" -> 2L, "on" -> "Drums"),
            Map("exp" -> 19L, "skill" -> 1L, "on" -> "Guitar")
          ),
          List("name", "instrument", "experience", "rating")
        ),
        RelationshipPropertyTestCase(
          "should do remapping and selection of relationship properties",
          Some("experience:exp, rating:skill, instrument"),
          List(
            Map("exp" -> 12L, "skill" -> 2L, "instrument" -> "Drums"),
            Map("exp" -> 19L, "skill" -> 1L, "instrument" -> "Guitar")
          ),
          List("name", "experience", "rating", "hasDiploma")
        ),
        RelationshipPropertyTestCase(
          "should pick nothing but source and target when passing empty options",
          Some(""),
          List(Map[String, AnyRef]()),
          List("name", "experience", "instrument", "rating", "hasDiploma")
        )
      )

      cases.map { testCase =>
        dynamicTest(
          testCase.name,
          () => {
            val session = testCase.df.sparkSession
            SparkSession.setActiveSession(session)
            testCase.df.repartition(1).write.format("neo4j")
              .mode(SaveMode.Overwrite)
              .options(testCase.options)
              .save()

            val res = session.read.format("neo4j")
              .option("relationship", testCase.relType)
              .option("relationship.nodes.map", "false")
              .option("relationship.source.labels", ":M")
              .option("relationship.target.labels", ":I")
              .load()
              .orderBy("`source.name`")
              .collectAsList()

            assertThat(res.size).isEqualTo(2)

            testCase.expectedInclusions.zipWithIndex.foreach {
              case (expectedProperties, index) =>
                expectedProperties.foreach {
                  case (property, value) =>
                    val relProp = "rel." + property
                    assertThat(getByName[Any](res.get(index), relProp)).isEqualTo(value)
                }
            }

            testCase.expectedExclusions.foreach { property =>
              val expectMissingProp = "rel." + property
              val assertion = assertThat(catchThrowable(() => res.get(0).fieldIndex(expectMissingProp)))
              assertion.`as`(s"Expecting column $expectMissingProp to be excluded")
              assertion.isInstanceOf(classOf[IllegalArgumentException])
            }

            // source.name and target.name always exist
            assertThat(res.get(0).fieldIndex("source.name")).isNotNull
            assertThat(res.get(0).fieldIndex("target.name")).isNotNull
          }
        )
      }
        .asJava.stream()
    }

    @Test
    def should_overwrite_related_graph_entities_when_node_overwrite_mode(
      driver: Driver,
      spark: SparkSession,
      neo4j: Neo4j
    ): Unit = {
      import spark.implicits._

      SparkSession.setActiveSession(spark)
      driver.session().run(
        """CREATE (a:Person {id: 0, name:'Alice'})
          |CREATE (b:Person {id: 1, name:'Bob'})
          |CREATE (a)-[:KNOWS {nickname: "Bob"}]->(b)""".stripMargin
      ).consume()

      Seq((0, 1, "AliceNewName", "Bobby"))
        .toDF("sourceId", "targetId", "name", "nickname")
        .write.format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("relationship.nodes.map", "false")
        .option("relationship", "KNOWS")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.properties", "nickname")
        .option("relationship.source.labels", ":Person")
        .option("relationship.source.node.keys", "sourceId:id")
        .option("relationship.source.node.properties", "name")
        .option("relationship.target.labels", ":Person")
        .option("relationship.target.node.keys", "targetId:id")
        .save()

      val fetchQuery = "MATCH (a)-[r:KNOWS]->(b) RETURN a.name AS source, r.nickname AS nick, b.name AS target"
      val record = driver.session().run(fetchQuery).single()

      assertThat(record.get("source").asString()).isEqualTo("AliceNewName")
      assertThat(record.get("nick").asString()).isEqualTo("Bobby")
      assertThat(record.get("target").asString()).isEqualTo("Bob")
    }

    @Test
    def should_pass_script_data_to_the_executor(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      import spark.implicits._

      Seq(("Andrea", "Santurbano"), ("Davide", "Fantuzzi")).toDF("name", "surname")
        .write.format("neo4j")
        .mode(SaveMode.Append)
        .option(
          "query",
          "CREATE (n:Person {fullName: event.name + ' ' + event.surname, age: scriptResult[0].age[event.name]})"
        )
        .option("script.1", "CREATE INDEX person_surname FOR (p:Person) ON (p.surname);")
        .option("script.2", "CREATE CONSTRAINT product_name_sku FOR (p:Product) REQUIRE (p.name, p.sku) IS NODE KEY;")
        .option("script.3", "RETURN {Andrea: 36, Davide: 32} AS age;")
        .save()

      val gotSum = driver.session().run("MATCH (p:Person) RETURN sum(p.age) AS ageSum").single()
      assertThat(gotSum.get("ageSum").asInt()).isEqualTo(36 + 32)

      val fetchSchemaQuery =
        s"""SHOW INDEXES YIELD labelsOrTypes, properties, owningConstraint
           |WHERE (labelsOrTypes = ['Person'] AND properties = ['surname'] AND owningConstraint IS NULL)
           |OR (labelsOrTypes = ['Product'] AND properties = ['name', 'sku'] AND owningConstraint IS NOT NULL)
           |RETURN count(*) AS count
           |""".stripMargin
      val gotCount = driver.session().run(fetchSchemaQuery).single().get("count").asInt()
      assertThat(gotCount).isEqualTo(2)
    }

    @Test
    def `should_handle_relationship_match_source_even_when_odd_characters_present`(
      driver: Driver,
      spark: SparkSession,
      neo4j: Neo4j
    ): Unit = {
      import spark.implicits._

      SparkSession.setActiveSession(spark)
      driver.session().run(
        """CREATE (:Tool {name: 'hammer'})
          |CREATE (:Tool {name: 'driver'})""".stripMargin
      ).consume()

      Seq(
        ("hammer", "nail", true),
        ("driver", "screw", true),
        ("hammer", "screw", false),
        ("driver", "nail", false)
      ).toDF("töõl", "part:specification", "is_appropriate‽")
        .repartition()
        .write.format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("relationship", "INTERACTS_WITH")
        .option("relationship.source.save.mode", "match")
        .option("relationship.source.labels", ":Tool")
        .option("relationship.source.node.keys", "töõl:name")
        .option("relationship.target.save.mode", "overwrite")
        .option("relationship.target.labels", ":Part")
        .option("relationship.target.node.keys", "`part:specification`")
        .option("relationship.properties", "is_appropriate‽:suitable")
        .save()

      val fetchQuery = "MATCH (:Tool)-[r:INTERACTS_WITH]->(:Part) RETURN count(r) AS count"
      val gotCount = driver.session().run(fetchQuery).single().get("count").asInt()

      assertThat(gotCount).isEqualTo(4)
    }

    @Test
    def should_not_write_node_properties_to_relationships(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      import spark.implicits._

      Seq(
        ("john", "The Matrix", "today"),
        ("jane", "Oppenheimer", "yesterday"),
        ("şaban", "Hababam Sınıfı", "two days ago")
      ).toDF("username", "movie_title", "watch_time")
        .write.format("neo4j")
        .mode(SaveMode.Append)
        .option("relationship", "WATCHED")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":User")
        .option("relationship.source.node.keys", "username:name")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Movie")
        .option("relationship.target.node.keys", "movie_title:title")
        .save()

      val fetchQuery =
        """
          |MATCH (:User)-[r:WATCHED]->(:Movie)
          |WITH r
          |ORDER BY r.watch_time ASC
          |RETURN collect(r{.*})
          |""".stripMargin

      val rows = driver.session().run(fetchQuery)
        .single()
        .get(0)
        .asList((value: Value) => value.asMap().asScala)
        .asScala

      assertThat(rows).isEqualTo(List(
        Map("watch_time" -> "today"),
        Map("watch_time" -> "two days ago"),
        Map("watch_time" -> "yesterday")
      ))
    }
  }

  @Nested
  @DisplayName("skip null keys")
  @Execution(ExecutionMode.SAME_THREAD)
  class SkipNullKeysWriteIT {

    @BeforeEach
    def drop_constraints(driver: Driver): Unit = {
      use(driver.session()) { session =>
        session.run("SHOW CONSTRAINTS YIELD name").list().asScala
          .map(_.get("name").asString())
          .foreach(name => session.run(s"DROP CONSTRAINT `$name`").consume())
      }
    }

    @Test
    def should_fail_to_write_nodes_when_key_properties_are_null(spark: SparkSession): Unit = {
      import spark.implicits._
      val cities = Seq(
        (Some(1), "Cherbourg en Cotentin"),
        (Some(2), "London"),
        (Some(3), "Malmö"),
        (None, "Moon")
      ).toDF("id", "city")

      val thrown = catchThrowable(() =>
        cities.write
          .format("neo4j")
          .mode(SaveMode.Overwrite)
          .option("labels", ":City")
          .option("node.keys", "id")
          .option("schema.optimization.node.keys", "KEY")
          .save()
      )

      assertThat(thrown).isInstanceOf(classOf[SparkException])
      assertThat(thrown).hasMessageContaining("Cannot merge the following node because of null property value")
    }

    @Test
    def should_fails_to_write_relationships_when_source_node_key_properties_contain_null_values(spark: SparkSession)
      : Unit = {
      import spark.implicits._
      val thrown = catchThrowable(() => {
        val cities = Seq(
          (Some(1), Some(2), "British Airways"),
          (Some(2), Some(3), "Turkish Airlines"),
          (None, Some(5), "Another Airline")
        ).toDF("from", "to", "airline")

        cities.write
          .format("neo4j")
          .mode(SaveMode.Overwrite)
          .option("relationship", "FLIES_TO")
          .option("relationship.source.save.mode", "Overwrite")
          .option("relationship.source.labels", ":City")
          .option("relationship.source.node.keys", "from:id")
          .option("relationship.target.save.mode", "Overwrite")
          .option("relationship.target.labels", ":City")
          .option("relationship.target.node.keys", "to:id")
          .option("relationship.properties", "airline")
          .option("schema.optimization.node.keys", "KEY")
          .save()
      })
      assertThat(thrown).isInstanceOf(classOf[SparkException])
      assertThat(thrown).hasMessageContaining("Cannot merge the following node because of null property value")
    }

    @Test
    def should_fails_to_write_relationships_when_target_node_key_properties_contain_null_values(spark: SparkSession)
      : Unit = {
      import spark.implicits._
      val caught = catchThrowable(() => {
        val cities = Seq(
          (Some(1), Some(2), "British Airways"),
          (Some(2), Some(3), "Turkish Airlines"),
          (Some(3), None, "Another Airline")
        ).toDF("from", "to", "airline")

        cities.write
          .format("neo4j")
          .mode(SaveMode.Overwrite)
          .option("relationship", "FLIES_TO")
          .option("relationship.source.save.mode", "Overwrite")
          .option("relationship.source.labels", ":City")
          .option("relationship.source.node.keys", "from:id")
          .option("relationship.target.save.mode", "Overwrite")
          .option("relationship.target.labels", ":City")
          .option("relationship.target.node.keys", "to:id")
          .option("relationship.properties", "airline")
          .option("schema.optimization.node.keys", "KEY")
          .save()
      })
      assertThat(caught).isInstanceOf(classOf[SparkException])
      assertThat(caught).hasMessageContaining("Cannot merge the following node because of null property value")
    }

    @Test
    def should_fails_to_write_relationships_when_relationship_key_properties_contain_null_values(
      spark: SparkSession,
      neo4j: Neo4j
    ): Unit = {
      assumeTrue(
        CanIUse.INSTANCE.canIUse(Schema.INSTANCE.relationshipKeyConstraints()).withNeo4j(neo4j)
      )
      import spark.implicits._

      val caught = catchThrowable(() => {
        val cities = Seq(
          (Some(1), Some(2), Some("BA721"), "British Airways"),
          (Some(2), Some(3), Some("TK211"), "Turkish Airlines"),
          (Some(3), Some(4), None, "Another Airline")
        ).toDF("from", "to", "flight", "airline")

        cities.write
          .format("neo4j")
          .mode(SaveMode.Overwrite)
          .option("relationship", "FLIES_TO")
          .option("relationship.source.save.mode", "Overwrite")
          .option("relationship.source.labels", ":City")
          .option("relationship.source.node.keys", "from:id")
          .option("relationship.target.save.mode", "Overwrite")
          .option("relationship.target.labels", ":City")
          .option("relationship.target.node.keys", "to:id")
          .option("relationship.keys", "flight")
          .option("relationship.properties", "airline")
          .option("schema.optimization.node.keys", "KEY")
          .option("schema.optimization.relationship.keys", "KEY")
          .save()
      })
      assertThat(caught).isInstanceOf(classOf[SparkException])
      assertThat(caught).hasMessageContaining("Cannot merge the following relationship because of null property value")
    }

    @Test
    def should_skips_nodes_when_key_properties_contain_null_values_with_APPEND_mode(
      spark: SparkSession,
      driver: Driver
    ): Unit = {
      import spark.implicits._

      val cities = Seq(
        (Some(1), "Cherbourg en Cotentin"),
        (Some(2), "London"),
        (Some(3), "Malmö"),
        (None, "Moon")
      ).toDF("id", "city")

      cities.write
        .format("neo4j")
        .mode(SaveMode.Append)
        .option("labels", ":City")
        .option("node.keys", "id")
        .option("node.keys.skip.nulls", "true")
        .save()

      use(driver.session()) { session =>
        val result = session.run("MATCH (n:City) RETURN count(n) as count")
          .single()
          .get("count")
          .asLong()
        assert(result == 3)
      }
    }

    @Test
    def should_skips_nodes_when_key_properties_contain_null_values_with_OVERWRITE_mode(
      spark: SparkSession,
      driver: Driver
    ): Unit = {
      import spark.implicits._
      val cities = Seq(
        (Some(1), "Cherbourg en Cotentin"),
        (Some(2), "London"),
        (Some(3), "Malmö"),
        (None, "Moon")
      ).toDF("id", "city")

      cities.write
        .format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("labels", ":City")
        .option("node.keys", "id")
        .option("node.keys.skip.nulls", "true")
        .option("schema.optimization.node.keys", "KEY")
        .save()

      use(driver.session()) { session =>
        val result = session.run("MATCH (n:City) RETURN count(n) as count")
          .single()
          .get("count")
          .asLong()
        assert(result == 3)
      }
    }

    @Test
    def should_skips_relationships_when_source_or_target_node_key_properties_contain_null_values(
      spark: SparkSession,
      driver: Driver
    ): Unit = {
      import spark.implicits._
      val cities = Seq(
        (Some(1), Some(2), "British Airways"),
        (Some(2), Some(3), "Turkish Airlines"),
        (None, Some(5), "Another Airline"),
        (Some(5), None, "Another Airline")
      ).toDF("from", "to", "airline")

      cities.write
        .format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("relationship", "FLIES_TO")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":City")
        .option("relationship.source.node.keys", "from:id")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":City")
        .option("relationship.target.node.keys", "to:id")
        .option("relationship.properties", "airline")
        .option("schema.optimization.node.keys", "KEY")
        .option("relationship.source.node.keys.skip.nulls", "true")
        .option("relationship.target.node.keys.skip.nulls", "true")
        .save()

      use(driver.session()) { session =>
        val cities = session.run("MATCH (n:City) RETURN count(n) as count")
          .single()
          .get("count")
          .asLong()
        assert(cities == 3)

        val citiesWithId5 = session.run("MATCH (n:City {id: 5}) RETURN count(n) as count")
          .single()
          .get("count")
          .asLong()
        assert(citiesWithId5 == 0)

        val flies = session.run("MATCH ()-[r:FLIES_TO]->() RETURN count(r) as count")
          .single()
          .get("count")
          .asLong()
        assert(flies == 2)
      }
    }

    @Test
    def should_skips_relationships_when_source_or_target_node_key_properties_contain_null_values_when_nodes_are_matched(
      spark: SparkSession,
      driver: Driver
    )
      : Unit = {
      import spark.implicits._
      use(driver.session()) { session =>
        session.run("UNWIND [1,2,3,5] AS id CREATE (:City {id: id})").consume()
      }

      val cities = Seq(
        (Some(1), Some(2), "British Airways"),
        (Some(2), Some(3), "Turkish Airlines"),
        (None, Some(5), "Another Airline"),
        (Some(5), None, "Another Airline")
      ).toDF("from", "to", "airline")

      cities.write
        .format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("relationship", "FLIES_TO")
        .option("relationship.source.save.mode", "Match")
        .option("relationship.source.labels", ":City")
        .option("relationship.source.node.keys", "from:id")
        .option("relationship.target.save.mode", "Match")
        .option("relationship.target.labels", ":City")
        .option("relationship.target.node.keys", "to:id")
        .option("relationship.properties", "airline")
        .option("relationship.source.node.keys.skip.nulls", "true")
        .option("relationship.target.node.keys.skip.nulls", "true")
        .save()

      use(driver.session()) { session =>
        val cities = session.run("MATCH (n:City) RETURN count(n) as count")
          .single()
          .get("count")
          .asLong()
        assert(cities == 4)

        val flies = session.run("MATCH ()-[r:FLIES_TO]->() RETURN count(r) as count")
          .single()
          .get("count")
          .asLong()
        assert(flies == 2)
      }
    }

    @Test
    def should_skips_relationships_when_source_or_target_node_key_properties_contain_null_values_when_nodes_are_appended(
      spark: SparkSession,
      driver: Driver
    )
      : Unit = {
      import spark.implicits._
      val cities = Seq(
        (Some(1), Some(2), "British Airways"),
        (Some(3), Some(4), "Turkish Airlines"),
        (None, Some(5), "Another Airline"),
        (Some(5), None, "Another Airline")
      ).toDF("from", "to", "airline")

      cities.write
        .format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("relationship", "FLIES_TO")
        .option("relationship.source.save.mode", "Append")
        .option("relationship.source.labels", ":City")
        .option("relationship.source.node.keys", "from:id")
        .option("relationship.target.save.mode", "Append")
        .option("relationship.target.labels", ":City")
        .option("relationship.target.node.keys", "to:id")
        .option("relationship.properties", "airline")
        .option("relationship.source.node.keys.skip.nulls", "true")
        .option("relationship.target.node.keys.skip.nulls", "true")
        .save()

      use(driver.session()) { session =>
        val cities = session.run("MATCH (n:City) RETURN count(n) as count")
          .single()
          .get("count")
          .asLong()
        assert(cities == 4)

        val citiesWithId5 = session.run("MATCH (n:City {id: 5}) RETURN count(n) as count")
          .single()
          .get("count")
          .asLong()
        assert(citiesWithId5 == 0)

        val flies = session.run("MATCH ()-[r:FLIES_TO]->() RETURN count(r) as count")
          .single()
          .get("count")
          .asLong()
        assert(flies == 2)
      }
    }

    @Test
    def should_skips_relationships_when_source_or_target_node_key_properties_contain_null_values_with_append_mode(
      spark: SparkSession,
      driver: Driver
    ): Unit = {
      import spark.implicits._
      val cities = Seq(
        (Some(1), Some(2), "British Airways"),
        (Some(3), Some(4), "Turkish Airlines"),
        (None, Some(5), "Another Airline"),
        (Some(5), None, "Another Airline")
      ).toDF("from", "to", "airline")

      cities.write
        .format("neo4j")
        .mode(SaveMode.Append)
        .option("relationship", "FLIES_TO")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":City")
        .option("relationship.source.node.keys", "from:id")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":City")
        .option("relationship.target.node.keys", "to:id")
        .option("relationship.properties", "airline")
        .option("schema.optimization.node.keys", "KEY")
        .option("relationship.source.node.keys.skip.nulls", "true")
        .option("relationship.target.node.keys.skip.nulls", "true")
        .save()

      use(driver.session()) { session =>
        val cities = session.run("MATCH (n:City) RETURN count(n) as count")
          .single()
          .get("count")
          .asLong()
        assert(cities == 4)

        val citiesWithId5 = session.run("MATCH (n:City {id: 5}) RETURN count(n) as count")
          .single()
          .get("count")
          .asLong()
        assert(citiesWithId5 == 0)

        val flies = session.run("MATCH ()-[r:FLIES_TO]->() RETURN count(r) as count")
          .single()
          .get("count")
          .asLong()
        assert(flies == 2)
      }
    }

    @Test
    def should_skips_relationships_when_source_or_target_node_key_properties_contain_null_values_when_nodes_are_matched_with_append_mode(
      spark: SparkSession,
      driver: Driver
    )
      : Unit = {
      import spark.implicits._
      use(driver.session()) { session =>
        session.run("UNWIND [1,2,3,5] AS id CREATE (:City {id: id})").consume()
      }

      val cities = Seq(
        (Some(1), Some(2), "British Airways"),
        (Some(2), Some(3), "Turkish Airlines"),
        (None, Some(5), "Another Airline"),
        (Some(5), None, "Another Airline")
      ).toDF("from", "to", "airline")

      cities.write
        .format("neo4j")
        .mode(SaveMode.Append)
        .option("relationship", "FLIES_TO")
        .option("relationship.source.save.mode", "Match")
        .option("relationship.source.labels", ":City")
        .option("relationship.source.node.keys", "from:id")
        .option("relationship.target.save.mode", "Match")
        .option("relationship.target.labels", ":City")
        .option("relationship.target.node.keys", "to:id")
        .option("relationship.properties", "airline")
        .option("relationship.source.node.keys.skip.nulls", "true")
        .option("relationship.target.node.keys.skip.nulls", "true")
        .save()

      use(driver.session()) { session =>
        val cities = session.run("MATCH (n:City) RETURN count(n) as count")
          .single()
          .get("count")
          .asLong()
        assert(cities == 4)

        val flies = session.run("MATCH ()-[r:FLIES_TO]->() RETURN count(r) as count")
          .single()
          .get("count")
          .asLong()
        assert(flies == 2)
      }
    }

    @Test
    def should_skips_relationships_when_source_or_target_node_key_properties_contain_null_values_when_nodes_are_appended_with_append_mode(
      spark: SparkSession,
      driver: Driver
    )
      : Unit = {
      import spark.implicits._
      val cities = Seq(
        (Some(1), Some(2), "British Airways"),
        (Some(3), Some(4), "Turkish Airlines"),
        (None, Some(5), "Another Airline"),
        (Some(5), None, "Another Airline")
      ).toDF("from", "to", "airline")

      cities.write
        .format("neo4j")
        .mode(SaveMode.Append)
        .option("relationship", "FLIES_TO")
        .option("relationship.source.save.mode", "Append")
        .option("relationship.source.labels", ":City")
        .option("relationship.source.node.keys", "from:id")
        .option("relationship.target.save.mode", "Append")
        .option("relationship.target.labels", ":City")
        .option("relationship.target.node.keys", "to:id")
        .option("relationship.properties", "airline")
        .option("relationship.source.node.keys.skip.nulls", "true")
        .option("relationship.target.node.keys.skip.nulls", "true")
        .save()

      use(driver.session()) { session =>
        val cities = session.run("MATCH (n:City) RETURN count(n) as count")
          .single()
          .get("count")
          .asLong()
        assert(cities == 4)

        val citiesWithId5 = session.run("MATCH (n:City {id: 5}) RETURN count(n) as count")
          .single()
          .get("count")
          .asLong()
        assert(citiesWithId5 == 0)

        val flies = session.run("MATCH ()-[r:FLIES_TO]->() RETURN count(r) as count")
          .single()
          .get("count")
          .asLong()
        assert(flies == 2)
      }
    }

    @Test
    def should_skips_relationships_when_relationship_key_properties_contain_null_values(
      spark: SparkSession,
      neo4j: Neo4j,
      driver: Driver
    ): Unit = {
      assumeTrue(
        CanIUse.INSTANCE.canIUse(Schema.INSTANCE.relationshipKeyConstraints()).withNeo4j(neo4j)
      )

      import spark.implicits._

      val cities = Seq(
        (Some(1), Some(2), Some("BA721"), "British Airways"),
        (Some(2), Some(3), Some("TK211"), "Turkish Airlines"),
        (Some(3), Some(5), None, "Another Airline")
      ).toDF("from", "to", "flight", "airline")

      cities.write
        .format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("relationship", "FLIES_TO")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":City")
        .option("relationship.source.node.keys", "from:id")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":City")
        .option("relationship.target.node.keys", "to:id")
        .option("relationship.keys", "flight")
        .option("relationship.properties", "airline")
        .option("schema.optimization.node.keys", "KEY")
        .option("schema.optimization.relationship.keys", "KEY")
        .option("relationship.keys.skip.nulls", "true")
        .save()

      use(driver.session()) { session =>
        val cities = session.run("MATCH (n:City) RETURN count(n) as count")
          .single()
          .get("count")
          .asLong()
        assert(cities == 3)

        val citiesWithId5 = session.run("MATCH (n:City {id: 5}) RETURN count(n) as count")
          .single()
          .get("count")
          .asLong()
        assert(citiesWithId5 == 0)

        val flies = session.run("MATCH ()-[r:FLIES_TO]->() RETURN count(r) as count")
          .single()
          .get("count")
          .asLong()
        assert(flies == 2)
      }
    }
  }

  @Nested
  @DisplayName("relationships across databases")
  @Execution(ExecutionMode.SAME_THREAD)
  class RelationshipDatabasesWriteIT {

    @BeforeEach
    def create_databases(driver: Driver): Unit = {
      driver.createOrReplaceDatabase("db1")
      driver.createOrReplaceDatabase("db2")
    }

    @AfterEach
    def drop_databases(driver: Driver): Unit = {
      driver.dropDatabase("db1")
      driver.dropDatabase("db2")
    }

    @Test
    def should_read_and_write_relations_with_append_mode(driver: Driver, spark: SparkSession): Unit = {
      val total = 100
      val fixtureQuery: String =
        s"""UNWIND range(1, $total) as id
           |CREATE (pr:Product {id: id * $total, name: 'Product ' + id})
           |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
           |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
           |RETURN *
      """.stripMargin

      use(driver.session(SessionConfig.forDatabase("db1"))) { session =>
        session.run(fixtureQuery).consume()
      }

      use(driver.session(SessionConfig.forDatabase("db2"))) { session =>
        session.executeWrite(tx => {
          tx.run("CREATE CONSTRAINT person_id FOR (p:Person) REQUIRE p.id IS UNIQUE").consume()
          tx.run("CREATE CONSTRAINT product_id FOR (p:Product) REQUIRE p.id IS UNIQUE").consume()
        })
      }

      val dfOriginal: DataFrame = spark.read.format("neo4j")
        .option("database", "db1")
        .option("relationship", "BOUGHT")
        .option("relationship.save.strategy", "native")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Person")
        .option("relationship.target.labels", ":Product")
        .load()

      dfOriginal.write
        .format("neo4j")
        .mode(SaveMode.Append)
        .option("database", "db2")
        .option("relationship", "SOLD")
        .option("relationship.save.strategy", "native")
        .option("relationship.source.labels", ":Person")
        .option("relationship.source.save.mode", "Append")
        .option("relationship.target.labels", ":Product")
        .option("relationship.target.save.mode", "Append")
        .option("batch.size", "11")
        .save()

      // let's write again to prove that 2 relationship are being added
      dfOriginal.write
        .format("neo4j")
        .mode(SaveMode.Append)
        .option("database", "db2")
        .option("relationship", "SOLD")
        .option("relationship.save.strategy", "native")
        .option("relationship.source.labels", ":Person")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.node.keys", "source.id:id")
        .option("relationship.target.labels", ":Product")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.node.keys", "target.id:id")
        .option("batch.size", "11")
        .save()

      val dfCopy = spark.read.format("neo4j")
        .option("database", "db2")
        .option("relationship", "SOLD")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Person")
        .option("relationship.target.labels", ":Product")
        .load()

      assertThat(dfCopy.count()).isEqualTo(dfOriginal.count() * 2)

      val resSourceOrig = dfOriginal.select("`source.id`").orderBy("`source.id`").collectAsList()
      val resSourceCopy = dfCopy.select("`source.id`").orderBy("`source.id`").collectAsList()
      val resTargetOrig = dfOriginal.select("`target.id`").orderBy("`target.id`").collectAsList()
      val resTargetCopy = dfCopy.select("`target.id`").orderBy("`target.id`").collectAsList()

      assertThat(resSourceCopy.get(0).getLong(0)).isEqualTo(resSourceOrig.get(0).getLong(0))
      assertThat(resTargetCopy.get(0).getLong(0)).isEqualTo(resTargetOrig.get(0).getLong(0))

      assertThat(dfCopy.where("`source.id` = 1").count()).isEqualTo(2)
    }

    @Test
    def should_read_and_write_relations_with_overwrite_mode(driver: Driver, spark: SparkSession): Unit = {
      val total = 100
      val fixtureQuery: String =
        s"""UNWIND range(1, $total) as id
           |CREATE (pr:Product {id: id * $total, name: 'Product ' + id})
           |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
           |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
           |RETURN *
    """.stripMargin

      use(driver.session(SessionConfig.forDatabase("db1"))) { session =>
        session.run(fixtureQuery).consume()
      }

      use(driver.session(SessionConfig.forDatabase("db2"))) { session =>
        session
          .executeWrite(tx => {
            tx.run("CREATE CONSTRAINT person_id FOR (p:Person) REQUIRE p.id IS UNIQUE").consume()
            tx.run("CREATE CONSTRAINT product_id FOR (p:Product) REQUIRE p.id IS UNIQUE").consume()
          })
      }

      val dfOriginal: DataFrame = spark.read.format("neo4j")
        .option("database", "db1")
        .option("relationship", "BOUGHT")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Person")
        .option("relationship.target.labels", ":Product")
        .load()
        .orderBy("`source.id`", "`target.id`")

      dfOriginal.write
        .format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("database", "db2")
        .option("relationship", "SOLD")
        .option("relationship.save.strategy", "native")
        .option("relationship.source.labels", ":Person")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Product")
        .option("relationship.target.save.mode", "Overwrite")
        .option("batch.size", "11")
        .save()

      // let's write the same thing again to prove there will be just one relation
      dfOriginal.write
        .format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("database", "db2")
        .option("relationship", "SOLD")
        .option("relationship.save.strategy", "native")
        .option("relationship.source.labels", ":Person")
        .option("relationship.source.node.keys", "source.id:id")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Product")
        .option("relationship.target.node.keys", "target.id:id")
        .option("relationship.target.save.mode", "Overwrite")
        .option("batch.size", "11")
        .save()

      val dfCopy = spark.read.format("neo4j")
        .option("database", "db2")
        .option("relationship", "SOLD")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Person")
        .option("relationship.target.labels", ":Product")
        .load()
        .orderBy("`source.id`", "`target.id`")

      val dfOriginalCount = dfOriginal.count()
      assertEquals(dfOriginalCount, dfCopy.count())

      for (i <- 0 until 1) {
        assertEquals(
          dfOriginal.select("`source.id`").collectAsList().get(i).getLong(0),
          dfCopy.select("`source.id`").collectAsList().get(i).getLong(0)
        )
        assertEquals(
          dfOriginal.select("`target.id`").collectAsList().get(i).getLong(0),
          dfCopy.select("`target.id`").collectAsList().get(i).getLong(0)
        )
      }

      assertEquals(
        1,
        dfCopy.where("`source.id` = 1").count()
      )
    }

    @Test
    def should_read_and_write_relations_with_MATCH_and_node_keys(driver: Driver, spark: SparkSession): Unit = {
      val total = 100
      val fixtureQuery: String =
        s"""UNWIND range(1, $total) as id
           |CREATE (pr:Product {id: id * $total, name: 'Product ' + id})
           |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
           |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
           |RETURN *
    """.stripMargin

      use(driver.session(SessionConfig.forDatabase("db1"))) { session =>
        session.run(fixtureQuery).consume()
      }

      use(driver.session(SessionConfig.forDatabase("db2"))) { session =>
        session.run(fixtureQuery).consume()
      }

      val dfOriginal: DataFrame = spark.read.format("neo4j")
        .option("database", "db1")
        .option("relationship", "BOUGHT")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Person")
        .option("relationship.target.labels", ":Product")
        .load()
        .orderBy("`source.id`", "`target.id`")

      dfOriginal.write
        .format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("database", "db2")
        .option("relationship", "SOLD")
        .option("relationship.save.strategy", "native")
        .option("relationship.source.labels", ":Person")
        .option("relationship.target.labels", ":Product")
        .option("relationship.source.node.keys", "source.id:id")
        .option("relationship.target.node.keys", "target.id:id")
        .option("relationship.source.save.mode", "Match")
        .option("relationship.target.save.mode", "Match")
        .option("batch.size", "11")
        .save()

      val dfCopy = spark.read.format("neo4j")
        .option("database", "db2")
        .option("relationship", "SOLD")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Person")
        .option("relationship.target.labels", ":Product")
        .load()
        .orderBy("`source.id`", "`target.id`")

      for (i <- 0 until 1) {
        assertEquals(
          dfOriginal.select("`source.id`").collectAsList().get(i).getLong(0),
          dfCopy.select("`source.id`").collectAsList().get(i).getLong(0)
        )
        assertEquals(
          dfOriginal.select("`target.id`").collectAsList().get(i).getLong(0),
          dfCopy.select("`target.id`").collectAsList().get(i).getLong(0)
        )
      }
    }

    @Test
    def should_read_and_write_relations_with_MERGE_and_node_keys(driver: Driver, spark: SparkSession): Unit = {
      val total = 100
      val fixtureQuery: String =
        s"""UNWIND range(1, $total) as id
           |CREATE (pr:Product {id: id * $total, name: 'Product ' + id})
           |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
           |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
           |RETURN *
    """.stripMargin

      use(driver.session(SessionConfig.forDatabase("db1"))) { session =>
        session.run(fixtureQuery).consume()
      }

      val dfOriginal: DataFrame = spark.read.format("neo4j")
        .option("database", "db1")
        .option("relationship", "BOUGHT")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Person")
        .option("relationship.target.labels", ":Product")
        .load()
        .orderBy("`source.id`", "`target.id`")

      dfOriginal.write
        .format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("database", "db2")
        .option("relationship", "SOLD")
        .option("relationship.save.strategy", "native")
        .option("relationship.source.labels", ":Person")
        .option("relationship.target.labels", ":Product")
        .option("relationship.source.node.keys", "source.id:id")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.target.node.keys", "target.id:id")
        .option("relationship.target.save.mode", "Overwrite")
        .option("batch.size", "11")
        .save()

      val dfCopy = spark.read.format("neo4j")
        .option("database", "db2")
        .option("relationship", "SOLD")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Person")
        .option("relationship.target.labels", ":Product")
        .load()
        .orderBy("`source.id`", "`target.id`")

      for (i <- 0 until 1) {
        assertEquals(
          dfOriginal.select("`source.id`").collectAsList().get(i).getLong(0),
          dfCopy.select("`source.id`").collectAsList().get(i).getLong(0)
        )
        assertEquals(
          dfOriginal.select("`target.id`").collectAsList().get(i).getLong(0),
          dfCopy.select("`target.id`").collectAsList().get(i).getLong(0)
        )
      }
    }

    @Test
    def should_read_relations_and_write_relation_with_match_mode(driver: Driver, spark: SparkSession): Unit = {
      import spark.implicits._
      val fixtureQuery: String =
        s"""CREATE (m:Musician {name: "John Bonham", age: 32})
           |CREATE (i:Instrument {name: "Drums"})
           |RETURN *
    """.stripMargin

      use(driver.session(SessionConfig.forDatabase("db1"))) { session =>
        session.executeWrite((tx: TransactionContext) => tx.run(fixtureQuery).consume())
      }

      val musicDf = Seq(
        (12, 32, "John Bonham", "Drums")
      ).toDF("experience", "age", "name", "instrument")

      musicDf.write
        .format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("database", "db1")
        .option("relationship.nodes.map", "false")
        .option("relationship", "PLAYS")
        .option("relationship.source.save.mode", "Match")
        .option("relationship.target.save.mode", "Match")
        .option("relationship.source.labels", ":Musician")
        .option("relationship.source.node.keys", "name,age")
        .option("relationship.target.labels", ":Instrument")
        .option("relationship.target.node.keys", "instrument:name")
        .save()

      val df2 = spark.read.format("neo4j")
        .option("database", "db1")
        .option("relationship.nodes.map", "false")
        .option("relationship", "PLAYS")
        .option("relationship.source.labels", ":Musician")
        .option("relationship.target.labels", ":Instrument")
        .load()

      val experience = df2.select("`source.age`").where("`source.name` = 'John Bonham'")
        .collectAsList().get(0).getLong(0)

      assertEquals(32, experience)
    }

    @Test
    def should_give_a_more_clear_error_if_properties_or_keys_are_inverted(spark: SparkSession): Unit = {
      import spark.implicits._
      val musicDf = Seq(
        (1, 12, "John Henry Bonham", "Drums"),
        (2, 19, "John Mayer", "Guitar"),
        (3, 32, "John Scofield", "Guitar"),
        (4, 15, "John Butler", "Guitar")
      ).toDF("id", "experience", "name", "instrument")

      try {
        musicDf.write
          .format("neo4j")
          .mode(SaveMode.Overwrite)
          .option("database", "db1")
          .option("relationship", "PLAYS")
          .option("relationship.source.save.mode", "Overwrite")
          .option("relationship.target.save.mode", "Overwrite")
          .option("relationship.source.labels", ":Musician")
          .option("relationship.source.node.keys", "musician_name:name")
          .option("relationship.target.labels", ":Instrument")
          .option("relationship.target.node.keys", "instrument:name")
          .save()
      } catch {
        case exception: IllegalArgumentException => {
          val clientException = ExceptionUtils.getRootCause(exception)
          assertTrue(clientException.getMessage.equals(
            """Write failed due to the following errors:
              | - Schema is missing musician_name from option `relationship.source.node.keys`
              |
              |The option key and value might be inverted.""".stripMargin
          ))
        }
        case generic: Throwable =>
          fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}, got ${generic.getClass} instead")
      }
    }

    @Test
    def should_give_a_more_clear_error_if_properties_or_keys_are_inverted_on_different_options(spark: SparkSession)
      : Unit = {
      import spark.implicits._
      val musicDf = Seq(
        (1, 12, "John Henry Bonham", "Drums"),
        (2, 19, "John Mayer", "Guitar"),
        (3, 32, "John Scofield", "Guitar"),
        (4, 15, "John Butler", "Guitar")
      ).toDF("id", "experience", "name", "instrument")

      try {
        musicDf.write
          .format("neo4j")
          .mode(SaveMode.Overwrite)
          .option("database", "db1")
          .option("relationship", "PLAYS")
          .option("relationship.source.save.mode", "Overwrite")
          .option("relationship.target.save.mode", "Overwrite")
          .option("relationship.source.labels", ":Musician")
          .option("relationship.source.node.keys", "musician_name:name,another_name:name")
          .option("relationship.target.labels", ":Instrument")
          .option("relationship.target.node.keys", "instrument_name:name")
          .save()
      } catch {
        case exception: IllegalArgumentException => {
          val clientException = ExceptionUtils.getRootCause(exception)
          assertTrue(clientException.getMessage.equals(
            """Write failed due to the following errors:
              | - Schema is missing instrument_name from option `relationship.target.node.keys`
              | - Schema is missing musician_name, another_name from option `relationship.source.node.keys`
              |
              |The option key and value might be inverted.""".stripMargin
          ))
        }
        case generic: Throwable =>
          fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}, got ${generic.getClass} instead")
      }
    }

    @Test
    def should_give_a_more_clear_error_if_node_properties_or_keys_are_inverted(spark: SparkSession): Unit = {
      import spark.implicits._
      val musicDf = Seq(
        (1, 12, "John Henry Bonham", "Drums"),
        (2, 19, "John Mayer", "Guitar"),
        (3, 32, "John Scofield", "Guitar"),
        (4, 15, "John Butler", "Guitar")
      ).toDF("id", "experience", "name", "instrument")

      try {
        musicDf.write
          .format("neo4j")
          .mode(SaveMode.Append)
          .option("database", "db1")
          .option("labels", "Person")
          .option("relationship.save.strategy", "native")
          .option("node.properties", "musician_name:name,another_name:name")
          .save()
      } catch {
        case exception: IllegalArgumentException => {
          val clientException = ExceptionUtils.getRootCause(exception)
          assertTrue(clientException.getMessage.equals(
            """Write failed due to the following errors:
              | - Schema is missing instrument_name from option `node.properties`
              |
              |The option key and value might be inverted.""".stripMargin
          ))
        }
        case generic: Throwable => fail(
            s"should be thrown a ${classOf[IllegalArgumentException].getName}, got ${generic.getClass} instead: ${generic.getMessage}"
          )
      }
    }

    @Test
    def should_export_write_metrics(driver: Driver, spark: SparkSession): Unit = {
      import spark.implicits._
      val input = List("Ali", "Andrea", "Eugene", "Florent")
      val query = "CREATE (:Name {name: event.name})-[:STARTS_WITH]->(:Letter {value: left(event.name, 1)})"
      val expectedMetrics = Map(
        DataWriterMetrics.RECORDS_WRITTEN_DESCRIPTION -> 4,
        DataWriterMetrics.RELATIONSHIPS_CREATED_DESCRIPTION -> 4,
        DataWriterMetrics.NODES_CREATED_DESCRIPTION -> 8,
        DataWriterMetrics.PROPERTIES_SET_DESCRIPTION -> 8
      )

      val metrics = new AtomicReference[Map[String, Any]]()
      val listener = new MetricsListener(expectedMetrics.keySet, metrics)
      spark.sparkContext.addSparkListener(listener)

      try {
        input.toDF("name")
          .repartition(1)
          .write
          .format("neo4j")
          .mode(SaveMode.Append)
          .option("database", "db1")
          .option("batch.size", 1) // TODO: remove this when https://issues.apache.org/jira/browse/SPARK-45759 is fixed
          .option("query", query)
          .save()

        use(driver.session(SessionConfig.forDatabase("db1"))) { db1Session =>
          Assert.assertEventually(
            4L,
            () => {
              db1Session.run(
                "MATCH (:Name)-[r:STARTS_WITH]->(:Letter) RETURN count(r) as cnt"
              ).single().get("cnt").asLong()
            },
            30L,
            TimeUnit.SECONDS
          )
        }
        assertNotNull(metrics.get())
        assertEquals(metrics.get(), expectedMetrics)

      } finally {
        spark.sparkContext.removeSparkListener(listener)
      }
    }

    @Test
    def should_not_create_constraint_if_schema_validation_fails(driver: Driver, spark: SparkSession): Unit = {
      import spark.implicits._
      val cities = Seq(
        (1, "Cherbourg en Cotentin"),
        (2, "London"),
        (3, "Malmö")
      ).toDF("id", "city")

      try {
        cities.write
          .format("neo4j")
          .mode(SaveMode.Overwrite)
          .option("labels", ":News")
          .option("node.keys", "newsId")
          .option("schema.optimization.node.keys", "UNIQUE")
          .save()
      } catch {
        case _: Exception => {}
      }

      use(driver.session()) { session =>
        val result =
          session.run("SHOW CONSTRAINTS YIELD labelsOrTypes WHERE labelsOrTypes[0] = 'News' RETURN count(*) AS count")
            .single()
            .get("count")
            .asLong()
        assertEquals(0, result)
      }
    }

    class MetricsListener(names: Set[String], captureMetrics: AtomicReference[Map[String, Any]])
        extends SparkListener {

      override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
        stageSubmitted.stageInfo.accumulables.clear()
      }

      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        captureMetrics.set(stageCompleted
          .stageInfo
          .accumulables
          .values
          .filter(metric => metric.name.exists(names.contains))
          .map(metric => (metric.name.get, metric.value.get))
          .toMap)
      }
    }
  }

  @Nested
  @DisplayName("schema optimization")
  @Execution(ExecutionMode.SAME_THREAD)
  class SchemaWriteIT {

    import scala.math.Ordering.Implicits.infixOrderingOps

    private var originalDefaultTimeZone: TimeZone = _
    private var originalSessionTimeZone: String = _

    @BeforeEach
    def prepare(driver: Driver, spark: SparkSession): Unit = {
      use(driver.session()) { session =>
        assumeTrue(TestUtil.neo4jVersion(session) >= Versions.NEO4J_5_13)
        session.run("SHOW CONSTRAINTS YIELD name").list().asScala
          .map(_.get("name").asString())
          .foreach(name => session.run(s"DROP CONSTRAINT `$name`").consume())
      }
      originalDefaultTimeZone = TimeZone.getDefault
      originalSessionTimeZone = spark.conf.get("spark.sql.session.timeZone")
      spark.conf.set("spark.sql.session.timeZone", timeZoneLock) // to make TIMESTAMP_NTZ tests deterministic
    }

    @AfterEach
    def restore_time_zones(spark: SparkSession): Unit = {
      if (originalDefaultTimeZone != null) TimeZone.setDefault(originalDefaultTimeZone)
      if (originalSessionTimeZone != null) spark.conf.set("spark.sql.session.timeZone", originalSessionTimeZone)
    }

    private val timeZoneLock = "UTC" // to make TIMESTAMP_NTZ tests deterministic

    private val SHOW_CONSTRAINTS_QUERY =
      """|SHOW CONSTRAINTS
         |YIELD name, type, entityType, labelsOrTypes, properties, ownedIndex, propertyType""".stripMargin

    private val NODE_UNIQUENESS_SHOW_CONSTRAINTS_QUERY =
      """|SHOW CONSTRAINTS
         |YIELD name, type AS ptype, entityType, labelsOrTypes, properties, ownedIndex, propertyType
         |RETURN name, entityType, labelsOrTypes, properties, ownedIndex,
         |CASE ptype
         |  WHEN "UNIQUENESS" THEN "NODE_PROPERTY_UNIQUENESS"
         |  ELSE ptype
         |END AS type, propertyType
         |ORDER BY type ASC""".stripMargin

    private val RELATIONSHIP_UNIQUENESS_SHOW_CONSTRAINTS_QUERY =
      """|SHOW CONSTRAINTS
         |YIELD name, type AS ptype, entityType, labelsOrTypes, properties, ownedIndex, propertyType
         |RETURN name, entityType, labelsOrTypes, properties, ownedIndex,
         |CASE ptype
         |  WHEN "RELATIONSHIP_UNIQUENESS" THEN "RELATIONSHIP_PROPERTY_UNIQUENESS"
         |  ELSE ptype
         |END AS type, propertyType
         |ORDER BY type ASC""".stripMargin

    private val ALL_TYPES_AS_COL_NAMES = Array(
      "string",
      "int",
      "boolean",
      "float",
      "date",
      "localDateTime",
      "zonedDateTime",
      "stringArray",
      "intArray",
      "booleanArray",
      "floatArray",
      "dateArray",
      "localDateTimeArray",
      "zonedDateTimeArray"
    )

    private def mapData(data: Any): Any = data match {
      case null                 => null
      case a: Array[_]          => a.toSeq.map(mapData)
      case l: java.util.List[_] => l.asScala.toSeq.map(mapData)
      case d: LocalDate         => Date.valueOf(d)
      case zdt: ZonedDateTime   => Timestamp.from(zdt.toInstant)
      case any: Any             => any
    }

    private val schemaOptimization = SchemaConstraintsOptimizationType.values
      .filterNot(_ == SchemaConstraintsOptimizationType.NONE)
      .mkString(",")

    private val nodeWithSchema = "NodeWithSchema"

    @Test
    def should_apply_schema_for_nodes(driver: Driver, spark: SparkSession): Unit = {
      val (expectedNode: Map[_root_.java.lang.String, Any], df: DataFrame) =
        createNodesDataFrameWithNotNullColumns(spark)

      df
        .write
        .mode(SaveMode.Append)
        .format("neo4j")
        .option("labels", s":$nodeWithSchema")
        .option(Neo4jOptions.SCHEMA_OPTIMIZATION, schemaOptimization)
        .save()

      val count: Long = driver.session().run(
        s"""
           |MATCH (n:$nodeWithSchema)
           |RETURN count(n)
           |""".stripMargin
      )
        .single()
        .get(0)
        .asLong()

      assertEquals(1L, count)

      val expectedSchema = Seq(
        constraintNodeNotNull(nodeWithSchema, "boolean"),
        constraintNodeNotNull(nodeWithSchema, "float"),
        constraintNodeNotNull(nodeWithSchema, "int"),
        constraintNodeNotNull(nodeWithSchema, "string"),
        constraintNodeType(nodeWithSchema, "boolean", "BOOLEAN"),
        constraintNodeType(nodeWithSchema, "booleanArray", "LIST<BOOLEAN NOT NULL>"),
        constraintNodeType(nodeWithSchema, "date", "DATE"),
        constraintNodeType(nodeWithSchema, "dateArray", "LIST<DATE NOT NULL>"),
        constraintNodeType(nodeWithSchema, "float", "FLOAT"),
        constraintNodeType(nodeWithSchema, "floatArray", "LIST<FLOAT NOT NULL>"),
        constraintNodeType(nodeWithSchema, "int", "INTEGER"),
        constraintNodeType(nodeWithSchema, "intArray", "LIST<INTEGER NOT NULL>"),
        constraintNodeType(nodeWithSchema, "localDateTime", "LOCAL DATETIME"),
        constraintNodeType(nodeWithSchema, "localDateTimeArray", "LIST<LOCAL DATETIME NOT NULL>"),
        constraintNodeType(nodeWithSchema, "string", "STRING"),
        constraintNodeType(nodeWithSchema, "stringArray", "LIST<STRING NOT NULL>"),
        constraintNodeType(nodeWithSchema, "zonedDateTime", "ZONED DATETIME"),
        constraintNodeType(nodeWithSchema, "zonedDateTimeArray", "LIST<ZONED DATETIME NOT NULL>")
      )

      val actualSchema = driver.session()
        .run(SHOW_CONSTRAINTS_QUERY)
        .list()
        .asScala
        .map(_.asMap(v => v.asObject()).asScala.view.mapValues(mapData).toMap)
        .toSeq

      assertEquals(expectedSchema, actualSchema)

      val actualNode: Map[String, Any] = driver.session()
        .executeRead(tx =>
          tx.run(s"MATCH (n:$nodeWithSchema) RETURN n")
            .list()
            .asScala
            .map(_.get("n").asNode())
            .map(_.asMap())
        )
        .head
        .asScala
        .view
        .mapValues(mapData)
        .toMap

      assertEquals(expectedNode, actualNode)
    }

    @Test
    def should_apply_schema_and_node_keys_for_nodes(driver: Driver, spark: SparkSession): Unit = {
      val (expectedNode: Map[_root_.java.lang.String, Any], df: DataFrame) =
        createNodesDataFrameWithNotNullColumns(spark)

      df.write
        .mode(SaveMode.Overwrite)
        .format("neo4j")
        .option("labels", s":$nodeWithSchema")
        .option(Neo4jOptions.SCHEMA_OPTIMIZATION, schemaOptimization)
        .option(Neo4jOptions.SCHEMA_OPTIMIZATION_NODE_KEY, ConstraintsOptimizationType.KEY.toString)
        .option("node.keys", "int,string")
        .save()

      val count: Long = driver.session().run(
        s"""
           |MATCH (n:$nodeWithSchema)
           |RETURN count(n)
           |""".stripMargin
      )
        .single()
        .get(0)
        .asLong()

      assertEquals(1L, count)

      val expectedSchema = Seq(
        constraintNodeNotNull(nodeWithSchema, "boolean"),
        constraintNodeNotNull(nodeWithSchema, "float"),
        constraintNodeNotNull(nodeWithSchema, "int"),
        constraintNodeNotNull(nodeWithSchema, "string"),
        constraintNodeType(nodeWithSchema, "boolean", "BOOLEAN"),
        constraintNodeType(nodeWithSchema, "booleanArray", "LIST<BOOLEAN NOT NULL>"),
        constraintNodeType(nodeWithSchema, "date", "DATE"),
        constraintNodeType(nodeWithSchema, "dateArray", "LIST<DATE NOT NULL>"),
        constraintNodeType(nodeWithSchema, "float", "FLOAT"),
        constraintNodeType(nodeWithSchema, "floatArray", "LIST<FLOAT NOT NULL>"),
        constraintNodeType(nodeWithSchema, "int", "INTEGER"),
        constraintNodeType(nodeWithSchema, "intArray", "LIST<INTEGER NOT NULL>"),
        constraintNodeType(nodeWithSchema, "localDateTime", "LOCAL DATETIME"),
        constraintNodeType(nodeWithSchema, "localDateTimeArray", "LIST<LOCAL DATETIME NOT NULL>"),
        constraintNodeType(nodeWithSchema, "string", "STRING"),
        constraintNodeType(nodeWithSchema, "stringArray", "LIST<STRING NOT NULL>"),
        constraintNodeType(nodeWithSchema, "zonedDateTime", "ZONED DATETIME"),
        constraintNodeType(nodeWithSchema, "zonedDateTimeArray", "LIST<ZONED DATETIME NOT NULL>"),
        constraintNodeKey(nodeWithSchema, Seq("int", "string"))
      )

      val actualSchema = driver.session()
        .run(SHOW_CONSTRAINTS_QUERY)
        .list()
        .asScala
        .map(_.asMap(v => v.asObject()).asScala.view.mapValues(mapData).toMap)
        .toSeq

      assertEquals(expectedSchema, actualSchema)

      val actualNode: Map[String, Any] = driver.session()
        .executeRead(tx =>
          tx.run(s"MATCH (n:$nodeWithSchema) RETURN n")
            .list()
            .asScala
            .map(_.get("n").asNode())
            .map(_.asMap())
        )
        .head
        .asScala
        .view
        .mapValues(mapData)
        .toMap

      assertEquals(expectedNode, actualNode)
    }

    @Test
    def should_apply_schema_and_node_keys_for_nodes_when_remapped(driver: Driver, spark: SparkSession): Unit = {
      val (node: Map[_root_.java.lang.String, Any], df: DataFrame) = createNodesDataFrameWithNotNullColumns(spark)

      df.write
        .mode(SaveMode.Overwrite)
        .format("neo4j")
        .option("labels", s":$nodeWithSchema")
        .option(Neo4jOptions.SCHEMA_OPTIMIZATION, schemaOptimization)
        .option(Neo4jOptions.SCHEMA_OPTIMIZATION_NODE_KEY, ConstraintsOptimizationType.KEY.toString)
        .option("node.keys", "int:int_prop,string:string_prop")
        .save()

      val count: Long = driver.session().run(
        s"""
           |MATCH (n:$nodeWithSchema)
           |RETURN count(n)
           |""".stripMargin
      )
        .single()
        .get(0)
        .asLong()

      assertEquals(1L, count)

      val expectedSchema = Seq(
        constraintNodeNotNull(nodeWithSchema, "boolean"),
        constraintNodeNotNull(nodeWithSchema, "float"),
        constraintNodeNotNull(nodeWithSchema, "int_prop"),
        constraintNodeNotNull(nodeWithSchema, "string_prop"),
        constraintNodeType(nodeWithSchema, "boolean", "BOOLEAN"),
        constraintNodeType(nodeWithSchema, "booleanArray", "LIST<BOOLEAN NOT NULL>"),
        constraintNodeType(nodeWithSchema, "date", "DATE"),
        constraintNodeType(nodeWithSchema, "dateArray", "LIST<DATE NOT NULL>"),
        constraintNodeType(nodeWithSchema, "float", "FLOAT"),
        constraintNodeType(nodeWithSchema, "floatArray", "LIST<FLOAT NOT NULL>"),
        constraintNodeType(nodeWithSchema, "intArray", "LIST<INTEGER NOT NULL>"),
        constraintNodeType(nodeWithSchema, "int_prop", "INTEGER"),
        constraintNodeType(nodeWithSchema, "localDateTime", "LOCAL DATETIME"),
        constraintNodeType(nodeWithSchema, "localDateTimeArray", "LIST<LOCAL DATETIME NOT NULL>"),
        constraintNodeType(nodeWithSchema, "stringArray", "LIST<STRING NOT NULL>"),
        constraintNodeType(nodeWithSchema, "string_prop", "STRING"),
        constraintNodeType(nodeWithSchema, "zonedDateTime", "ZONED DATETIME"),
        constraintNodeType(nodeWithSchema, "zonedDateTimeArray", "LIST<ZONED DATETIME NOT NULL>"),
        constraintNodeKey(nodeWithSchema, Seq("int_prop", "string_prop"))
      )

      val actualSchema = driver.session()
        .run(SHOW_CONSTRAINTS_QUERY)
        .list()
        .asScala
        .map(_.asMap(v => v.asObject()).asScala.view.mapValues(mapData).toMap)
        .toSeq

      assertEquals(expectedSchema, actualSchema)

      val expectedNode =
        node.map {
          case (k, v) =>
            if (k == "string" || k == "int") (k + "_prop", v)
            else (k, v)
        }

      val actualNode = driver.session()
        .executeRead(tx =>
          tx.run(s"MATCH (n:$nodeWithSchema) RETURN n")
            .list()
            .asScala
            .map(_.get("n").asNode())
            .map(_.asMap())
        )
        .head
        .asScala
        .view
        .mapValues(mapData)
        .toMap

      assertEquals(expectedNode, actualNode)
    }

    private def constraintNodeNotNull(node: String, prop: String): Map[String, Any] = Map(
      "name" -> s"spark_NODE-NOT_NULL-CONSTRAINT-$node-$prop",
      "type" -> "NODE_PROPERTY_EXISTENCE",
      "entityType" -> "NODE",
      "labelsOrTypes" -> Seq(node),
      "properties" -> Seq(prop),
      "ownedIndex" -> null,
      "propertyType" -> null
    )

    private def constraintNodeType(node: String, prop: String, expectedType: String): Map[String, Any] = Map(
      "name" -> s"spark_NODE-TYPE-CONSTRAINT-$node-$prop",
      "type" -> "NODE_PROPERTY_TYPE",
      "entityType" -> "NODE",
      "labelsOrTypes" -> Seq(node),
      "properties" -> Seq(prop),
      "ownedIndex" -> null,
      "propertyType" -> expectedType
    )

    private def constraintNodeKey(node: String, props: Seq[String]): Map[String, Any] = Map(
      "name" -> s"spark_NODE_KEY-CONSTRAINT_${node}_${props.mkString("-")}",
      "type" -> "NODE_KEY",
      "entityType" -> "NODE",
      "labelsOrTypes" -> Seq(node),
      "properties" -> props,
      "ownedIndex" -> s"spark_NODE_KEY-CONSTRAINT_${node}_${props.mkString("-")}",
      "propertyType" -> null
    )

    private def createNodesDataFrameWithNotNullColumns(spark: SparkSession): (Map[String, Any], DataFrame) = {
      import spark.implicits._

      TimeZone.setDefault(TimeZone.getTimeZone(timeZoneLock))

      val row = (
        "Foo",
        1,
        false,
        1.1,
        Date.valueOf("2023-11-22"),
        LocalDateTime.of(2023, 11, 22, 12, 12, 12),
        Timestamp.valueOf(s"2020-11-22 11:11:11.11"),
        Seq("Foo1", "Foo2"),
        Seq(1, 2),
        Seq(true, false),
        Seq(1.1, 2.2),
        Seq(Date.valueOf("2023-11-22"), Date.valueOf("2023-11-23")),
        Seq(LocalDateTime.of(2023, 11, 22, 11, 11, 11), LocalDateTime.of(2023, 11, 23, 12, 12, 12)),
        Seq(Timestamp.valueOf("2023-11-22 11:11:11.11"), Timestamp.valueOf("2023-11-23 12:12:12.12"))
      )

      val data = Seq(row).toDF(ALL_TYPES_AS_COL_NAMES.toIndexedSeq: _*)
      val expectedNode = ALL_TYPES_AS_COL_NAMES.zip(row.productIterator.toSeq).toMap

      val schema = StructType(data.schema.map { sf =>
        sf.name match {
          case "localDateTimeArray" =>
            StructField(sf.name, DataTypes.createArrayType(DataTypes.TimestampNTZType, false), sf.nullable)
          case "zonedDateTimeArray" =>
            StructField(sf.name, DataTypes.createArrayType(DataTypes.TimestampType, false), sf.nullable)
          case "stringArray" =>
            StructField(sf.name, DataTypes.createArrayType(DataTypes.StringType, false), sf.nullable)
          case "dateArray" => StructField(sf.name, DataTypes.createArrayType(DataTypes.DateType, false), sf.nullable)
          case "string"    => StructField(sf.name, DataTypes.StringType, false)
          case _           => sf
        }
      })

      val df = spark.createDataFrame(data.rdd, schema)
      (expectedNode, df)
    }

    @Test
    def should_apply_schema_for_relationships_and_nodes(driver: Driver, spark: SparkSession): Unit = {
      val expectedMap = createDatasetForRelationships(
        spark,
        driver,
        Map(
          Neo4jOptions.SCHEMA_OPTIMIZATION -> schemaOptimization
        )
      )

      val count: Long = driver.session().run(
        """
          |MATCH p = (:NodeA)-[:MY_REL]->(:NodeB)
          |RETURN count(p)
          |""".stripMargin
      )
        .single()
        .get(0)
        .asLong()

      assertEquals(1L, count)

      val expected = Seq(
        constraintNodeNotNull("NodeA", "id"),
        constraintNodeNotNull("NodeB", "id"),
        constraintNodeType("NodeA", "id", "STRING"),
        constraintNodeType("NodeB", "id", "STRING"),
        constraintRelNotNull("boolean"),
        constraintRelNotNull("float"),
        constraintRelNotNull("int"),
        constraintRelType("boolean", "BOOLEAN"),
        constraintRelType("booleanArray", "LIST<BOOLEAN NOT NULL>"),
        constraintRelType("date", "DATE"),
        constraintRelType("dateArray", "LIST<DATE NOT NULL>"),
        constraintRelType("float", "FLOAT"),
        constraintRelType("floatArray", "LIST<FLOAT NOT NULL>"),
        constraintRelType("int", "INTEGER"),
        constraintRelType("intArray", "LIST<INTEGER NOT NULL>"),
        constraintRelType("localDateTime", "LOCAL DATETIME"),
        constraintRelType("localDateTimeArray", "LIST<LOCAL DATETIME NOT NULL>"),
        constraintRelType("string", "STRING"),
        constraintRelType("stringArray", "LIST<STRING NOT NULL>"),
        constraintRelType("zonedDateTime", "ZONED DATETIME"),
        constraintRelType("zonedDateTimeArray", "LIST<ZONED DATETIME NOT NULL>")
      )

      val actual = driver.session()
        .run(SHOW_CONSTRAINTS_QUERY)
        .list()
        .asScala
        .map(_.asMap(v => v.asObject()).asScala.view.mapValues(mapData).toMap)
        .toSeq

      assertEquals(expected, actual)

      val actualMap = driver.session().run(
        """
          |MATCH (s:NodeA)-[r:MY_REL]->(t:NodeB)
          |RETURN s.id AS idSource, t.id AS idTarget, r
          |""".stripMargin
      )
        .list()
        .asScala
        .map(r =>
          Map("idSource" -> r.get("idSource").asString(), "idTarget" -> r.get("idTarget").asString()) ++ r.get(
            "r"
          ).asRelationship().asMap().asScala
        )
        .head
        .view
        .mapValues(mapData)
        .toMap

      assertEquals(expectedMap, actualMap)
    }

    private def createDatasetForRelationships(
      spark: SparkSession,
      driver: Driver,
      options: Map[String, String]
    ): Map[String, Any] = {
      import spark.implicits._

      val shouldRemap = options.contains(Neo4jOptions.RELATIONSHIP_PROPERTIES)

      driver.session()
        .run("CREATE (:NodeA{id: 'a'}), (:NodeB{id: 'b'})")
        .consume()

      val colNames = Array(
        "idSource",
        "idTarget"
      ) ++ ALL_TYPES_AS_COL_NAMES

      val row = (
        "a",
        "b",
        "Foo",
        1,
        false,
        1.1,
        Date.valueOf("2023-11-22"),
        LocalDateTime.of(2023, 11, 22, 12, 12, 12),
        Timestamp.valueOf(s"2020-11-22 11:11:11.11"),
        Seq("Foo1", "Foo2"),
        Seq(1, 2),
        Seq(true, false),
        Seq(1.1, 2.2),
        Seq(Date.valueOf("2023-11-22"), Date.valueOf("2023-11-23")),
        Seq(LocalDateTime.of(2023, 11, 22, 11, 11, 11), LocalDateTime.of(2023, 11, 23, 12, 12, 12)),
        Seq(Timestamp.valueOf("2023-11-22 11:11:11.11"), Timestamp.valueOf("2023-11-23 12:12:12.12"))
      )

      val data = Seq(row).toDF(colNames.toIndexedSeq: _*)

      val schema = StructType(data.schema.map { sf =>
        sf.name match {
          case "localDateTimeArray" =>
            StructField(sf.name, DataTypes.createArrayType(DataTypes.TimestampNTZType, false), sf.nullable)
          case "zonedDateTimeArray" =>
            StructField(sf.name, DataTypes.createArrayType(DataTypes.TimestampType, false), sf.nullable)
          case "stringArray" =>
            StructField(sf.name, DataTypes.createArrayType(DataTypes.StringType, false), sf.nullable)
          case "dateArray" => StructField(sf.name, DataTypes.createArrayType(DataTypes.DateType, false), sf.nullable)
          case "idSource"  => StructField(sf.name, DataTypes.StringType, false)
          case "idTarget"  => StructField(sf.name, DataTypes.StringType, false)
          case _           => sf
        }
      })

      spark.createDataFrame(data.rdd, schema)
        .write
        .mode(SaveMode.Overwrite)
        .format("neo4j")
        .option("relationship", "MY_REL")
        .option("relationship.source.labels", ":NodeA")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.node.keys", "idSource:id")
        .option("relationship.target.labels", ":NodeB")
        .option("relationship.target.node.keys", "idTarget:id")
        .option("relationship.target.save.mode", "Overwrite")
        .options(options)
        .save()

      colNames.map(c =>
        if (shouldRemap && (c == "string" || c == "int")) c + "_prop"
        else c
      ).zip(row.productIterator.toSeq).toMap
    }

    private def constraintRelNotNull(prop: String): Map[String, Any] = Map(
      "name" -> s"spark_RELATIONSHIP-NOT_NULL-CONSTRAINT-MY_REL-$prop",
      "type" -> "RELATIONSHIP_PROPERTY_EXISTENCE",
      "entityType" -> "RELATIONSHIP",
      "labelsOrTypes" -> Seq("MY_REL"),
      "properties" -> Seq(prop),
      "ownedIndex" -> null,
      "propertyType" -> null
    )

    private def constraintRelType(prop: String, expectedType: String) = Map(
      "name" -> s"spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-$prop",
      "type" -> "RELATIONSHIP_PROPERTY_TYPE",
      "entityType" -> "RELATIONSHIP",
      "labelsOrTypes" -> Seq("MY_REL"),
      "properties" -> Seq(prop),
      "ownedIndex" -> null,
      "propertyType" -> expectedType
    )

    @Test
    def should_apply_schema_for_relationships_and_nodes_when_remapped(driver: Driver, spark: SparkSession): Unit = {
      val expectedMap = createDatasetForRelationships(
        spark,
        driver,
        Map(
          Neo4jOptions.SCHEMA_OPTIMIZATION -> schemaOptimization,
          Neo4jOptions.RELATIONSHIP_PROPERTIES -> ALL_TYPES_AS_COL_NAMES.map {
            case "string" => "string:string_prop"
            case "int"    => "int:int_prop"
            case c        => c
          }.mkString(",")
        )
      )

      val count: Long = driver.session().run(
        """
          |MATCH p = (:NodeA)-[:MY_REL]->(:NodeB)
          |RETURN count(p)
          |""".stripMargin
      )
        .single()
        .get(0)
        .asLong()

      assertEquals(1L, count)

      val expected = Seq(
        constraintNodeNotNull("NodeA", "id"),
        constraintNodeNotNull("NodeB", "id"),
        constraintNodeType("NodeA", "id", "STRING"),
        constraintNodeType("NodeB", "id", "STRING"),
        constraintRelNotNull("boolean"),
        constraintRelNotNull("float"),
        constraintRelNotNull("int_prop"),
        constraintRelType("boolean", "BOOLEAN"),
        constraintRelType("booleanArray", "LIST<BOOLEAN NOT NULL>"),
        constraintRelType("date", "DATE"),
        constraintRelType("dateArray", "LIST<DATE NOT NULL>"),
        constraintRelType("float", "FLOAT"),
        constraintRelType("floatArray", "LIST<FLOAT NOT NULL>"),
        constraintRelType("intArray", "LIST<INTEGER NOT NULL>"),
        constraintRelType("int_prop", "INTEGER"),
        constraintRelType("localDateTime", "LOCAL DATETIME"),
        constraintRelType("localDateTimeArray", "LIST<LOCAL DATETIME NOT NULL>"),
        constraintRelType("stringArray", "LIST<STRING NOT NULL>"),
        constraintRelType("string_prop", "STRING"),
        constraintRelType("zonedDateTime", "ZONED DATETIME"),
        constraintRelType("zonedDateTimeArray", "LIST<ZONED DATETIME NOT NULL>")
      )

      val actual = driver.session()
        .run(SHOW_CONSTRAINTS_QUERY)
        .list()
        .asScala
        .map(_.asMap(v => v.asObject()).asScala.view.mapValues(mapData).toMap)
        .toSeq

      assertEquals(expected, actual)

      val actualMap = driver.session().run(
        """
          |MATCH (s:NodeA)-[r:MY_REL]->(t:NodeB)
          |RETURN s.id AS idSource, t.id AS idTarget, r
          |""".stripMargin
      )
        .list()
        .asScala
        .map(r =>
          Map("idSource" -> r.get("idSource").asString(), "idTarget" -> r.get("idTarget").asString()) ++ r.get(
            "r"
          ).asRelationship().asMap().asScala
        )
        .head
        .view
        .mapValues(mapData)
        .toMap

      assertEquals(expectedMap, actualMap)
    }

    @Test
    def should_apply_unique_constraint_for_node(driver: Driver, spark: SparkSession): Unit = {
      import spark.implicits._
      val total = 10
      val ds = (1 to total)
        .map(i => i.toString)
        .toDF("surname")

      ds.write
        .format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("labels", ":Person:Customer")
        .option("node.keys", "surname")
        .option(Neo4jOptions.SCHEMA_OPTIMIZATION_NODE_KEY, ConstraintsOptimizationType.UNIQUE.toString)
        .save()

      val records = driver.session().run(
        """MATCH (p:Person:Customer)
          |RETURN p.surname AS surname
          |""".stripMargin
      ).list().asScala
        .map(r => r.asMap().asScala)
        .toSet
      val expected = ds.collect().map(row => Map("surname" -> row.getAs[String]("surname")))
        .toSet
      assertEquals(expected, records)

      val actualConstraint = driver.session().run(NODE_UNIQUENESS_SHOW_CONSTRAINTS_QUERY)
        .list()
        .asScala
        .map(_.asMap(v => v.asObject()).asScala.view.mapValues(mapData).toMap)
        .head
      val expectedConstraint = Map(
        "name" -> "spark_NODE_UNIQUE-CONSTRAINT_Person_surname",
        "type" -> "NODE_PROPERTY_UNIQUENESS",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("Person"),
        "properties" -> Seq("surname"),
        "ownedIndex" -> "spark_NODE_UNIQUE-CONSTRAINT_Person_surname",
        "propertyType" -> null
      )
      assertEquals(expectedConstraint, actualConstraint)

    }

    @Test
    def should_apply_node_key_constraint_for_node(driver: Driver, spark: SparkSession): Unit = {
      import spark.implicits._
      val total = 10
      val ds = (1 to total)
        .map(i => i.toString)
        .toDF("surname")

      ds.write
        .format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("labels", ":Person:Customer")
        .option("node.keys", "surname")
        .option(Neo4jOptions.SCHEMA_OPTIMIZATION_NODE_KEY, ConstraintsOptimizationType.KEY.toString)
        .save()

      val records = driver.session().run(
        """MATCH (p:Person:Customer)
          |RETURN p.surname AS surname
          |""".stripMargin
      ).list().asScala
        .map(r => r.asMap().asScala)
        .toSet
      val expected = ds.collect().map(row => Map("surname" -> row.getAs[String]("surname")))
        .toSet
      assertEquals(expected, records)

      val actualConstraint = driver.session().run(SHOW_CONSTRAINTS_QUERY)
        .list()
        .asScala
        .map(_.asMap(v => v.asObject()).asScala.view.mapValues(mapData).toMap)
        .head
      val expectedConstraint = Map(
        "name" -> "spark_NODE_KEY-CONSTRAINT_Person_surname",
        "type" -> "NODE_KEY",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("Person"),
        "properties" -> Seq("surname"),
        "ownedIndex" -> "spark_NODE_KEY-CONSTRAINT_Person_surname",
        "propertyType" -> null
      )
      assertEquals(expectedConstraint, actualConstraint)

    }

    @Test
    def should_apply_appropriate_constraints_even_when_remapped(driver: Driver, spark: SparkSession): Unit = {
      import spark.implicits._
      val total = 10
      val ds = (1 to total)
        .map(i => i.toString)
        .toDF("surname")

      ds.write
        .format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("labels", ":SurnameKey")
        .option("node.keys", "surname:surname_key")
        .option(Neo4jOptions.SCHEMA_OPTIMIZATION_NODE_KEY, ConstraintsOptimizationType.KEY.toString)
        .save()

      ds.write
        .format("neo4j")
        .mode(SaveMode.Overwrite)
        .option("labels", ":SurnameUnique")
        .option("node.keys", "surname:surname_unique")
        .option(Neo4jOptions.SCHEMA_OPTIMIZATION_NODE_KEY, ConstraintsOptimizationType.UNIQUE.toString)
        .save()

      val actualConstraint = driver.session().run(NODE_UNIQUENESS_SHOW_CONSTRAINTS_QUERY)
        .list()
        .asScala
        .map(_.asMap(v => v.asObject()).asScala.view.mapValues(mapData).toMap)
        .toSeq

      val expectedConstraint = Seq(
        Map(
          "name" -> "spark_NODE_KEY-CONSTRAINT_SurnameKey_surname_key",
          "type" -> "NODE_KEY",
          "entityType" -> "NODE",
          "labelsOrTypes" -> Seq("SurnameKey"),
          "properties" -> Seq("surname_key"),
          "ownedIndex" -> "spark_NODE_KEY-CONSTRAINT_SurnameKey_surname_key",
          "propertyType" -> null
        ),
        Map(
          "name" -> "spark_NODE_UNIQUE-CONSTRAINT_SurnameUnique_surname_unique",
          "type" -> "NODE_PROPERTY_UNIQUENESS",
          "entityType" -> "NODE",
          "labelsOrTypes" -> Seq("SurnameUnique"),
          "properties" -> Seq("surname_unique"),
          "ownedIndex" -> "spark_NODE_UNIQUE-CONSTRAINT_SurnameUnique_surname_unique",
          "propertyType" -> null
        )
      )

      assertEquals(expectedConstraint, actualConstraint)
    }

    @Test
    def should_apply_unique_constraint_for_relationship(driver: Driver, spark: SparkSession): Unit = {
      val expectedMap = createDatasetForRelationships(
        spark,
        driver,
        Map(
          Neo4jOptions.SCHEMA_OPTIMIZATION_RELATIONSHIP_KEY -> ConstraintsOptimizationType.UNIQUE.toString,
          "relationship.keys" -> "string,int"
        )
      )
      val actualMap = driver.session().run(
        """
          |MATCH (s:NodeA)-[r:MY_REL]->(t:NodeB)
          |RETURN s.id AS idSource, t.id AS idTarget, r
          |""".stripMargin
      )
        .list()
        .asScala
        .map(r =>
          Map("idSource" -> r.get("idSource").asString(), "idTarget" -> r.get("idTarget").asString()) ++ r.get(
            "r"
          ).asRelationship().asMap().asScala
        )
        .head
        .view
        .mapValues(mapData)
        .toMap

      assertEquals(expectedMap, actualMap)

      val actualConstraint = driver.session().run(RELATIONSHIP_UNIQUENESS_SHOW_CONSTRAINTS_QUERY)
        .list()
        .asScala
        .map(_.asMap(v => v.asObject()).asScala.view.mapValues(mapData).toMap)
        .head
      val expectedConstraint = Map(
        "name" -> "spark_RELATIONSHIP_UNIQUE-CONSTRAINT_MY_REL_string-int",
        "type" -> "RELATIONSHIP_PROPERTY_UNIQUENESS",
        "entityType" -> "RELATIONSHIP",
        "labelsOrTypes" -> Seq("MY_REL"),
        "properties" -> Seq("string", "int"),
        "ownedIndex" -> "spark_RELATIONSHIP_UNIQUE-CONSTRAINT_MY_REL_string-int",
        "propertyType" -> null
      )
      assertEquals(expectedConstraint, actualConstraint)
    }

    @Test
    def should_apply_rel_unique_constraint_for_relationship(driver: Driver, spark: SparkSession): Unit = {
      val expectedMap = createDatasetForRelationships(
        spark,
        driver,
        Map(
          Neo4jOptions.SCHEMA_OPTIMIZATION_RELATIONSHIP_KEY -> ConstraintsOptimizationType.KEY.toString,
          "relationship.keys" -> "string,int"
        )
      )

      val actualMap = driver.session().run(
        """
          |MATCH (s:NodeA)-[r:MY_REL]->(t:NodeB)
          |RETURN s.id AS idSource, t.id AS idTarget, r
          |""".stripMargin
      )
        .list()
        .asScala
        .map(r =>
          Map("idSource" -> r.get("idSource").asString(), "idTarget" -> r.get("idTarget").asString()) ++ r.get(
            "r"
          ).asRelationship().asMap().asScala
        )
        .head
        .view
        .mapValues(mapData)
        .toMap

      assertEquals(expectedMap, actualMap)

      val actualConstraint = driver.session().run(SHOW_CONSTRAINTS_QUERY)
        .list()
        .asScala
        .map(_.asMap(v => v.asObject()).asScala.view.mapValues(mapData).toMap)
        .head
      val expectedConstraint = Map(
        "name" -> "spark_RELATIONSHIP_KEY-CONSTRAINT_MY_REL_string-int",
        "type" -> "RELATIONSHIP_KEY",
        "entityType" -> "RELATIONSHIP",
        "labelsOrTypes" -> Seq("MY_REL"),
        "properties" -> Seq("string", "int"),
        "ownedIndex" -> "spark_RELATIONSHIP_KEY-CONSTRAINT_MY_REL_string-int",
        "propertyType" -> null
      )

      assertEquals(expectedConstraint, actualConstraint)
    }

    @Test
    def should_write_node_with_legacy_type_conversion_disabled_by_default(driver: Driver, spark: SparkSession): Unit = {
      val df = spark.sql(
        """
          |SELECT
          |  'legacy-type-conversion' AS id,
          |  timestamp('2025-01-01 11:11:11') AS timestamp,
          |  CAST('2025-01-01 11:11:11' AS TIMESTAMP_NTZ) AS timestampNtz,
          |  INTERVAL '4' DAY AS dayInterval,
          |  INTERVAL '10 05' DAY TO HOUR AS dayToHour,
          |  timestamp('2025-01-02 18:30:00.454') - timestamp('2024-01-01 00:00:00') AS arithmeticDuration,
          |  INTERVAL '3' YEAR AS yearInterval,
          |  INTERVAL '1-2' YEAR TO MONTH AS yearToMonth,
          |  CAST('erik' AS BINARY) AS binary,
          |  CAST(array(1, 2, 3) AS array<tinyint>) AS byteArray
          |""".stripMargin
      )
      assertTrue(df.schema("dayInterval").dataType.isInstanceOf[DayTimeIntervalType])
      assertTrue(df.schema("dayToHour").dataType.isInstanceOf[DayTimeIntervalType])
      assertTrue(df.schema("arithmeticDuration").dataType.isInstanceOf[DayTimeIntervalType])
      assertTrue(df.schema("yearInterval").dataType.isInstanceOf[YearMonthIntervalType])
      assertTrue(df.schema("yearToMonth").dataType.isInstanceOf[YearMonthIntervalType])

      df.write
        .mode(SaveMode.Overwrite)
        .format("neo4j")
        .option("labels", ":LegacyTypeConversionDisabled")
        .option("node.keys", "id")
        .save()

      val actual = driver.session().run(
        """
          |MATCH (n:LegacyTypeConversionDisabled {id: 'legacy-type-conversion'})
          |RETURN n
          |""".stripMargin
      )
        .single()
        .get("n")
        .asNode()
        .asMap()
        .asScala
        .toMap
      assertEquals(
        Set(
          "id",
          "timestamp",
          "timestampNtz",
          "dayInterval",
          "dayToHour",
          "arithmeticDuration",
          "yearInterval",
          "yearToMonth",
          "binary",
          "byteArray"
        ),
        actual.keySet
      )
      assertEquals("legacy-type-conversion", actual("id"))
      val expectedZoned = ZonedDateTime.of(2025, 1, 1, 11, 11, 11, 0, ZoneOffset.UTC)
      assertEquals(expectedZoned, actual("timestamp"))
      val expectedNtz = LocalDateTime.of(2025, 1, 1, 11, 11, 11)
      assertEquals(expectedNtz, actual("timestampNtz"))
      val dayInterval = actual("dayInterval").asInstanceOf[IsoDuration]
      assertEquals(0L, dayInterval.months())
      assertEquals(4L, dayInterval.days())
      assertEquals(0L, dayInterval.seconds())
      assertEquals(0, dayInterval.nanoseconds())
      val dayToHour = actual("dayToHour").asInstanceOf[IsoDuration]
      assertEquals(0L, dayToHour.months())
      assertEquals(10L, dayToHour.days())
      assertEquals(18000L, dayToHour.seconds())
      assertEquals(0, dayToHour.nanoseconds())
      val arithmetic = actual("arithmeticDuration").asInstanceOf[IsoDuration]
      assertEquals(0L, arithmetic.months())
      assertEquals(367L, arithmetic.days())
      assertEquals(66600L, arithmetic.seconds())
      assertEquals(454000000, arithmetic.nanoseconds())
      val yearInterval = actual("yearInterval").asInstanceOf[IsoDuration]
      assertEquals(36L, yearInterval.months())
      assertEquals(0L, yearInterval.days())
      assertEquals(0L, yearInterval.seconds())
      assertEquals(0, yearInterval.nanoseconds())
      val yearToMonth = actual("yearToMonth").asInstanceOf[IsoDuration]
      assertEquals(14L, yearToMonth.months())
      assertEquals(0L, yearToMonth.days())
      assertEquals(0L, yearToMonth.seconds())
      assertEquals(0, yearToMonth.nanoseconds())
      assertArrayEquals(Array[Byte](101, 114, 105, 107), actual("binary").asInstanceOf[Array[Byte]])
      assertArrayEquals(Array[Byte](1, 2, 3), actual("byteArray").asInstanceOf[Array[Byte]])
    }

    @Test
    def should_write_node_with_legacy_type_conversion_enabled(driver: Driver, spark: SparkSession): Unit = {
      val df = spark.sql(
        """
          |SELECT
          |  'legacy-type-conversion' AS id,
          |  timestamp('2025-01-01 11:11:11') AS timestamp,
          |  CAST('2025-01-01 11:11:11' AS TIMESTAMP_NTZ) AS timestampNtz,
          |  INTERVAL '4' DAY AS dayInterval,
          |  INTERVAL '10 05' DAY TO HOUR AS dayToHour,
          |  timestamp('2025-01-02 18:30:00.454') - timestamp('2024-01-01 00:00:00') AS arithmeticDuration,
          |  INTERVAL '3' YEAR AS yearInterval,
          |  INTERVAL '1-2' YEAR TO MONTH AS yearToMonth,
          |  CAST('erik' AS BINARY) AS binary,
          |  CAST(array(1, 2, 3) AS array<tinyint>) AS byteArray
          |""".stripMargin
      )
      assertTrue(df.schema("dayInterval").dataType.isInstanceOf[DayTimeIntervalType])
      assertTrue(df.schema("dayToHour").dataType.isInstanceOf[DayTimeIntervalType])
      assertTrue(df.schema("arithmeticDuration").dataType.isInstanceOf[DayTimeIntervalType])
      assertTrue(df.schema("yearInterval").dataType.isInstanceOf[YearMonthIntervalType])
      assertTrue(df.schema("yearToMonth").dataType.isInstanceOf[YearMonthIntervalType])

      df.write
        .mode(SaveMode.Overwrite)
        .format("neo4j")
        .option("labels", ":LegacyTypeConversionEnabled")
        .option("node.keys", "id")
        .option(Neo4jOptions.TYPE_CONVERSION, "legacy")
        .save()

      val actual = driver.session().run(
        """
          |MATCH (n:LegacyTypeConversionEnabled {id: 'legacy-type-conversion'})
          |RETURN n
          |""".stripMargin
      )
        .single()
        .get("n")
        .asNode()
        .asMap()
        .asScala
        .toMap
      assertEquals(
        Set(
          "id",
          "timestamp",
          "timestampNtz",
          "dayInterval",
          "dayToHour",
          "arithmeticDuration",
          "yearInterval",
          "yearToMonth",
          "binary",
          "byteArray"
        ),
        actual.keySet
      )
      assertEquals("legacy-type-conversion", actual("id"))
      val expectedTimestamp = ZonedDateTime.of(2025, 1, 1, 11, 11, 11, 0, ZoneOffset.UTC)
        .withZoneSameInstant(ZoneId.systemDefault())
        .toLocalDateTime
      assertEquals(expectedTimestamp, actual("timestamp"))
      assertEquals(
        DateTimeUtils.localDateTimeToMicros(LocalDateTime.of(2025, 1, 1, 11, 11, 11)),
        actual("timestampNtz").asInstanceOf[java.lang.Number].longValue()
      )
      assertEquals(4L * 24L * 3600L * 1000000L, actual("dayInterval").asInstanceOf[java.lang.Number].longValue())
      assertEquals((10L * 24L + 5L) * 3600L * 1000000L, actual("dayToHour").asInstanceOf[java.lang.Number].longValue())
      assertEquals(
        (367L * 24L * 3600L + 66600L) * 1000000L + 454000L,
        actual("arithmeticDuration").asInstanceOf[java.lang.Number].longValue()
      )
      assertEquals(36L, actual("yearInterval").asInstanceOf[java.lang.Number].longValue())
      assertEquals(14L, actual("yearToMonth").asInstanceOf[java.lang.Number].longValue())
      assertArrayEquals(Array[Byte](101, 114, 105, 107), actual("binary").asInstanceOf[Array[Byte]])
      val byteArray = actual("byteArray").asInstanceOf[java.util.List[_]].asScala
        .map(_.asInstanceOf[java.lang.Number].longValue())
      assertEquals(Seq(1L, 2L, 3L), byteArray)
    }
  }

  @Nested
  @DisplayName("type conversion")
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

    case class ExpectedNeo4jType[T](
      name: String,
      accessor: Value => T
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
