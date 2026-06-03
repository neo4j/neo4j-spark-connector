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

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.condition.DisabledIf
import org.junit.jupiter.params.Parameter
import org.junit.jupiter.params.ParameterizedClass
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ArgumentsSource
import org.junit.jupiter.params.provider.ValueSource
import org.neo4j.driver.Driver
import org.neo4j.driver.QueryConfig
import org.neo4j.driver.TransactionContext
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.spark.testsupport.Neo4jContainerProvider
import org.neo4j.spark.testsupport.Neo4jExtensions.DriverExtensions
import org.neo4j.spark.testsupport.Neo4jExtensions.Neo4jContainerExtensions
import org.neo4j.spark.testsupport.SparkConnectorScalaSuiteIT
import org.neo4j.spark.util.Neo4jOptions
import org.testcontainers.neo4j.Neo4jContainer

import java.sql.Date
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneOffset
import java.util.TimeZone

import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.Seq
import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.jdk.CollectionConverters.ListHasAsScala

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ParameterizedClass(name = "{argumentSetName}")
@ArgumentsSource(classOf[Neo4jContainerProvider])
@DisplayName("reading")
class ReadIT {

  @Parameter
  var neo4jContainer: Neo4jContainer = _

  var driver: Driver = _

  var spark: SparkSession = _

  @BeforeEach
  def prepare(): Unit = {
    if (!neo4jContainer.isRunning) {
      neo4jContainer.start()
    }
    driver = neo4jContainer.driver()
    driver.createOrReplaceDatabase("neo4j")
    spark = neo4jContainer.spark()
  }

  @AfterEach
  def cleanUp(): Unit = {
    if (spark != null) {
      spark.close()
    }
    if (driver != null) {
      driver.close()
    }
  }

  @Test
  def throws_exception_if_no_valid_read_option_is_set(): Unit = {
    assertThatExceptionOfType(classOf[IllegalArgumentException])
      .isThrownBy(() => {
        spark.read.format(classOf[DataSource].getName)
          .load()
          .show() // show is needed to trigger the exception because of changes in Spark 3
      })
      .withMessage("No valid option found. One of `GDS`, `LABELS`, `QUERY`, `RELATIONSHIP` is required")
  }

  @Test
  def throws_exception_if_two_valid_read_options_are_set(): Unit = {
    assertThatExceptionOfType(classOf[IllegalArgumentException])
      .isThrownBy(() => {
        spark.read.format(classOf[DataSource].getName)
          .option("labels", "Person")
          .option("relationship", "KNOWS")
          .load()
          .show() // show is needed to trigger the exception because of changes in Spark 3
      })
      .withMessage("You need to specify just one of these options: 'gds', 'labels', 'query', 'relationship'")
  }

  @Test
  def throws_exception_when_cypher_version_invalid(): Unit = {
    assertThatExceptionOfType(classOf[IllegalArgumentException])
      .isThrownBy(() => {
        spark.read.format(classOf[DataSource].getName)
          .option("labels", "Person")
          .option(Neo4jOptions.CYPHER_VERSION, "2.3")
          .load()
          .show()
      })
      .withMessage("The provided cypher version '2.3' is not valid.")
  }

  @Nested
  @DisplayName("by node labels")
  class ByNodeLabels {

    @Test
    def returns_element_id(): Unit = {
      driver.executableQuery(s"CREATE (:Person {name: 'John'})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      assertThat(df.select("<elementId>").collectAsList().get(0).getString(0))
        .isNotEmpty()
    }

    @Test
    def returns_labels(): Unit = {
      driver.executableQuery(s"CREATE (:Person:Customer {name: 'John'})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      assertThat(df.select("<labels>").collectAsList().asScala.map(_.getAs[mutable.Seq[String]](0)).asJava)
        .hasSize(1)
        .containsExactly(mutable.Seq("Person", "Customer"))
    }

    @Test
    def supports_unconventional_labels(): Unit = {
      driver.executableQuery(s"CREATE (:`Foo Bar`:Person:`(╯°□°）╯︵ ┻━┻`  {name: 'John'})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val result = df.select("<labels>").collectAsList().get(0).getAs[mutable.Seq[String]](0)
      assertThat(result.toSet[String]).isEqualTo(Set("Person", "Foo Bar", "(╯°□°）╯︵ ┻━┻"))
    }

    @Test
    def supports_joins_from_two_different_databases(): Unit = {
      driver.createOrReplaceDatabase("db1")
      driver.executableQuery(
        """
        CREATE (:Person:Customer {name: 'John Doe'}), (:Person:Customer {name: 'Mark Brown'}),
               (:Person:Customer {name: 'Cindy White'})
        """
      )
        .withConfig(QueryConfig.builder().withDatabase("db1").build())
        .execute()
      driver.createOrReplaceDatabase("db2")
      driver.executableQuery("CREATE (:Person:Employee {name: 'Jane Doe'}), (:Person:Employee {name: 'John Doe'})")
        .withConfig(QueryConfig.builder().withDatabase("db2").build())
        .execute()

      val df1 = spark.read.format(classOf[DataSource].getName)
        .option("database", "db1")
        .option("labels", "Person")
        .load()
      val df2 = spark.read.format(classOf[DataSource].getName)
        .option("database", "db2")
        .option("labels", "Person")
        .load()

      assertThat(df1.count()).isEqualTo(3)
      assertThat(df2.count()).isEqualTo(2)
      val joinedDf = df1.join(df2, df1("name") === df2("name"))
      assertThat(joinedDf.count()).isEqualTo(1)
    }

    @Test
    def returns_selected_string_column(): Unit = {
      driver.executableQuery(s"CREATE (:Person {name: 'John'})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      assertThat(df.select("name").collectAsList().get(0).getString(0)).isEqualTo("John")
    }

    @Test
    def returns_selected_long_column(): Unit = {
      driver.executableQuery(s"CREATE (:Person {age: 42})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      assertThat(df.select("age").collectAsList().get(0).getLong(0)).isEqualTo(42L)
    }

    @Test
    def returns_selected_double_column(): Unit = {
      driver.executableQuery(s"CREATE (:Person {score: 3.14})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      assertThat(df.select("score").collectAsList().get(0).getDouble(0)).isEqualTo(3.14)
    }

    @Test
    def returns_selected_localtime_column(): Unit = {
      driver.executableQuery(
        s"CREATE (:Person {aTime: localtime({hour:12, minute: 23, second: 0, millisecond: 294})})"
      ).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val result = df.select("aTime").collectAsList().get(0).getAs[GenericRowWithSchema](0)
      assertThat(result.get(0)).isEqualTo("local-time")
      assertThat(result.get(1)).isEqualTo("12:23:00.294")
    }

    @Test
    def returns_selected_time_column(): Unit = {
      val timezone = TimeZone.getDefault
      driver.executableQuery(
        s"CREATE (:Person {aTime: time({hour:12, minute: 23, second: 0, millisecond: 294})})"
      ).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val result = df.select("aTime").collectAsList().get(0).getAs[GenericRowWithSchema](0)
      val localTime = LocalTime.of(12, 23, 0, 294000000)
      val expectedTime = OffsetTime.of(localTime, timezone.toZoneId.getRules.getOffset(Instant.now()))
      assertThat(result.get(0)).isEqualTo("offset-time")
      assertThat(result.get(1)).isEqualTo(expectedTime.toString)
    }

    @Test
    def returns_selected_localdatetime_column(): Unit = {
      driver.executableQuery("CREATE (:Person {aTime: localdatetime('2007-12-03T10:15:30')})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val result = df.select("aTime").collectAsList().get(0).getAs[LocalDateTime](0)
      assertThat(result).isEqualTo(LocalDateTime.parse("2007-12-03T10:15:30"))
    }

    @Test
    def returns_selected_zoneddatetime_column(): Unit = {
      driver.executableQuery("CREATE (:Person {aTime: datetime('2015-06-24T12:50:35.556+01:00')})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val result = df.select("aTime").collectAsList().get(0).getTimestamp(0)
      assertThat(result).isEqualTo(Timestamp.from(OffsetDateTime.parse("2015-06-24T12:50:35.556+01:00").toInstant))
    }

    @Test
    def returns_selected_date_column(): Unit = {
      driver.executableQuery(s"CREATE (:Person {born: date('2009-10-10')})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val rows = df.select("born").collectAsList()
      val result = rows.get(0).getDate(0)
      assertThat(result).isEqualTo(Date.valueOf("2009-10-10"))
    }

    @Test
    def returns_selected_duration_column(): Unit = {
      driver.executableQuery(s"CREATE (:Person {range: duration({days: 14, hours:16, minutes: 12})})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val rows = df.select("range").collectAsList()
      val row = rows.get(0).getAs[GenericRowWithSchema](0)
      assertThat(row(0)).isEqualTo("duration")
      assertThat(row(1)).isEqualTo(0L)
      assertThat(row(2)).isEqualTo(14L)
      assertThat(row(3)).isEqualTo(58320L)
      assertThat(row(4)).isEqualTo(0)
      assertThat(row(5)).isEqualTo("P0M14DT58320S")
    }

    @Test
    def returns_selected_point_column(): Unit = {
      driver.executableQuery(s"CREATE (:Person {location: point({x: 12.12, y: 13.13})})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val row = df.select("location").collectAsList().get(0).getAs[GenericRowWithSchema](0)
      assertThat(row.get(0)).isEqualTo("point-2d")
      assertThat(row.get(1)).isEqualTo(7203)
      assertThat(row.get(2)).isEqualTo(12.12)
      assertThat(row.get(3)).isEqualTo(13.13)
    }

    @Test
    def returns_selected_geospatial_point_column(): Unit = {
      driver.executableQuery(s"CREATE (:Person {location: point({longitude: 12.12, latitude: 13.13})})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val row = df.select("location").collectAsList().get(0).getAs[GenericRowWithSchema](0)
      assertThat(row.get(0)).isEqualTo("point-2d")
      assertThat(row.get(1)).isEqualTo(4326)
      assertThat(row.get(2)).isEqualTo(12.12)
      assertThat(row.get(3)).isEqualTo(13.13)
    }

    @Test
    def returns_selected_3d_point_column(): Unit = {
      driver.executableQuery(s"CREATE (:Person {location: point({x: 12.12, y: 13.13, z: 1})})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val row = df.select("location").collectAsList().get(0).getAs[GenericRowWithSchema](0)
      assertThat(row.get(0)).isEqualTo("point-3d")
      assertThat(row.get(1)).isEqualTo(9157)
      assertThat(row.get(2)).isEqualTo(12.12)
      assertThat(row.get(3)).isEqualTo(13.13)
      assertThat(row.get(4)).isEqualTo(1.0)
    }

    @Test
    def returns_selected_string_array_column(): Unit = {
      driver.executableQuery(s"CREATE (:Person {names: ['John', 'Doe']})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val result = df.select("names").collectAsList().get(0).getAs[mutable.Seq[String]](0)
      assertThat(result.head).isEqualTo("John")
      assertThat(result(1)).isEqualTo("Doe")
    }

    @Test
    def returns_selected_long_array_column(): Unit = {
      driver.executableQuery(s"CREATE (:Person {ages: [22, 23]})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val result = df.select("ages").collectAsList().get(0).getAs[mutable.Seq[Long]](0)
      assertThat(result.head).isEqualTo(22)
      assertThat(result(1)).isEqualTo(23)
    }

    @Test
    def returns_selected_double_array_column(): Unit = {
      driver.executableQuery(s"CREATE (:Person {scores: [22.33, 44.55]})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val result = df.select("scores").collectAsList().get(0).getAs[mutable.Seq[Double]](0)
      assertThat(result.head).isEqualTo(22.33)
      assertThat(result(1)).isEqualTo(44.55)
    }

    @Test
    def returns_selected_datetime_array_column(): Unit = {
      driver.executableQuery(
        s"CREATE (p:Person {someTimes: [datetime('2010-10-10T11:13:37+01:00'), datetime('2011-11-11T10:13:37Z')]})"
      ).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val result = df.select("someTimes").collectAsList().get(0).getAs[mutable.Seq[Timestamp]](0)
      assertThat(result.head.toInstant.atZone(ZoneOffset.UTC).toString).isEqualTo("2010-10-10T10:13:37Z")
      assertThat(result(1).toInstant.atZone(ZoneOffset.UTC).toString).isEqualTo("2011-11-11T10:13:37Z")
    }

    @Test
    def returns_selected_localtime_array_column(): Unit = {
      driver.executableQuery(
        s"CREATE (:Person {someTimes: [localtime({hour:12}), localtime({hour:1, minute: 3})]})"
      ).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val row = df.select("someTimes").collectAsList().get(0).getAs[mutable.Seq[GenericRowWithSchema]](0)
      val value1 = row.head
      assertThat(value1.get(0)).isEqualTo("local-time")
      assertThat(value1.get(1)).isEqualTo("12:00:00")
      val value2 = row(1)
      assertThat(value2.get(0)).isEqualTo("local-time")
      assertThat(value2.get(1)).isEqualTo("01:03:00")
    }

    @Test
    def returns_selected_boolean_array_column(): Unit = {
      driver.executableQuery(s"CREATE (:Person {bools: [true, false]})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val result = df.select("bools").collectAsList().get(0).getAs[mutable.Seq[Boolean]](0)
      assertThat(result.head).isEqualTo(true)
      assertThat(result(1)).isEqualTo(false)
    }

    @Test
    def returns_selected_point_array_column(): Unit = {
      driver.executableQuery(
        s"CREATE (:Person {locations: [point({x: 11, y: 33.111}), point({x: 22, y: 44.222})]})"
      ).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val row = df.select("locations").collectAsList().get(0).getAs[mutable.Seq[GenericRowWithSchema]](0)
      val value1 = row.head
      assertThat(value1.get(0)).isEqualTo("point-2d")
      assertThat(value1.get(1)).isEqualTo(7203)
      assertThat(value1.get(2)).isEqualTo(11.0)
      assertThat(value1.get(3)).isEqualTo(33.111)
      val value2 = row(1)
      assertThat(value2.get(0)).isEqualTo("point-2d")
      assertThat(value2.get(1)).isEqualTo(7203)
      assertThat(value2.get(2)).isEqualTo(22.0)
      assertThat(value2.get(3)).isEqualTo(44.222)
    }

    @Test
    def returns_selected_geospatial_array_column(): Unit = {
      driver.executableQuery(
        s"CREATE (:Person {locations: [point({longitude: 11, latitude: 33.111}), point({longitude: 22, latitude: 44.222})]})"
      ).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val row = df.select("locations").collectAsList().get(0).getAs[mutable.Seq[GenericRowWithSchema]](0)
      val value1 = row.head
      assertThat(value1.get(0)).isEqualTo("point-2d")
      assertThat(value1.get(1)).isEqualTo(4326)
      assertThat(value1.get(2)).isEqualTo(11.0)
      assertThat(value1.get(3)).isEqualTo(33.111)
      val value2 = row(1)
      assertThat(value2.get(0)).isEqualTo("point-2d")
      assertThat(value2.get(1)).isEqualTo(4326)
      assertThat(value2.get(2)).isEqualTo(22.0)
      assertThat(value2.get(3)).isEqualTo(44.222)
    }

    @Test
    def returns_selected_3d_array_column(): Unit = {
      driver.executableQuery(
        s"CREATE (:Person {locations: [point({x: 11, y: 33.111, z: 12}), point({x: 22, y: 44.222, z: 99.1})]})"
      ).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val row = df.select("locations").collectAsList().get(0).getAs[mutable.Seq[GenericRowWithSchema]](0)
      val value1 = row.head
      assertThat(value1.get(0)).isEqualTo("point-3d")
      assertThat(value1.get(1)).isEqualTo(9157)
      assertThat(value1.get(2)).isEqualTo(11.0)
      assertThat(value1.get(3)).isEqualTo(33.111)
      assertThat(value1.get(4)).isEqualTo(12.0)
      val value2 = row(1)
      assertThat(value2.get(0)).isEqualTo("point-3d")
      assertThat(value2.get(1)).isEqualTo(9157)
      assertThat(value2.get(2)).isEqualTo(22.0)
      assertThat(value2.get(3)).isEqualTo(44.222)
      assertThat(value2.get(4)).isEqualTo(99.1)
    }

    @Test
    def returns_selected_date_array_column(): Unit = {
      driver.executableQuery(s"CREATE (:Person {dates: [date('2009-10-10'), date('2009-10-11')]})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val row = df.select("dates").collectAsList().get(0).getAs[mutable.Seq[Date]](0)
      assertThat(row.head).isEqualTo(Date.valueOf("2009-10-10"))
      assertThat(row(1)).isEqualTo(Date.valueOf("2009-10-11"))
    }

    @Test
    def returns_selected_zoneddatetime_array_column(): Unit = {
      driver.executableQuery(
        """
       CREATE (:Person {aTime: [
        datetime('2015-06-24T12:50:35.556+01:00'),
        datetime('2015-06-23T12:50:35.556+01:00')
       ]})
       """
      ).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val result = df.select("aTime").collectAsList().get(0).getAs[mutable.Seq[Timestamp]](0)
      assertThat(result.head).isEqualTo(Timestamp.from(OffsetDateTime.parse("2015-06-24T12:50:35.556+01:00").toInstant))
      assertThat(result(1)).isEqualTo(Timestamp.from(OffsetDateTime.parse("2015-06-23T12:50:35.556+01:00").toInstant))
    }

    @Test
    def returns_selected_duration_array_column(): Unit = {
      driver.executableQuery(
        s"CREATE (:Person {durations: [duration({months: 0.75}), duration({weeks: 2.5})]})"
      ).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val row = df.select("durations").collectAsList().get(0).getAs[mutable.Seq[GenericRowWithSchema]](0)
      val value1 = row.head
      assertThat(value1.get(0)).isEqualTo("duration")
      assertThat(value1.get(1)).isEqualTo(0L)
      assertThat(value1.get(2)).isEqualTo(22L)
      assertThat(value1.get(3)).isEqualTo(71509L)
      assertThat(value1.get(4)).isEqualTo(500000000)
      assertThat(value1.get(5)).isEqualTo("P0M22DT71509.500000000S")
      val value2 = row(1)
      assertThat(value2.get(0)).isEqualTo("duration")
      assertThat(value2.get(1)).isEqualTo(0L)
      assertThat(value2.get(2)).isEqualTo(17L)
      assertThat(value2.get(3)).isEqualTo(43200L)
      assertThat(value2.get(4)).isEqualTo(0)
      assertThat(value2.get(5)).isEqualTo("P0M17DT43200S")
    }

    @Test
    def returns_selected_byte_array_column(): Unit = {
      val bytes = "hello, world!".map(_.toByte).toArray
      val parameters = new java.util.HashMap[String, Object]()
      parameters.put("bytes", bytes)
      driver.executableQuery("CREATE (h:Hello {b: $bytes})")
        .withParameters(parameters)
        .execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Hello")
        .load()
        .select("b")

      val result = df.collect()
      assertThat(result).hasSize(1)
      val actualBytes = result.head.getAs[Array[Byte]](0)
      assertThat(actualBytes).isEqualTo(bytes)
    }

    @Test
    def returns_selected_field_with_unconventional_name(): Unit = {
      driver.executableQuery(
        s"""UNWIND range(1, 100) as id
           |CREATE (:Product {id: id, `(╯°□°)╯︵ ┻━┻`: 'Product ' + id})
    """.stripMargin
      )
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Product")
        .load()
        .select("(╯°□°)╯︵ ┻━┻")

      assertThat(df.columns.toSeq).isEqualTo(immutable.Seq("(╯°□°)╯︵ ┻━┻"))
    }

    @Test
    def returns_results_filtered_with_string_value_equality(): Unit = {
      driver.executableQuery(
        s"""UNWIND range(1, 100) as id
           |CREATE (:Product {id: id, name: 'Product ' + id})
    """.stripMargin
      )
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Product")
        .load()
        .filter("name = 'Product 1'")

      assertThat(df.columns.toSeq).isEqualTo(immutable.Seq("<elementId>", "<labels>", "name", "id"))
      assertThat(df.select("name").collect().map(_.getString(0)).toSet)
        .isEqualTo(Set("Product 1"))
    }

    @Test
    def returns_results_filtered_with_date_value_equality(): Unit = {
      driver.executableQuery(s"""
       CREATE (:Person {birth: date('1998-02-04')}),
        (:Person {birth: date('1988-01-05')})
       """).execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("birth")
        .where("birth = '1988-01-05'")

      val rows = df.collectAsList()
      assertThat(rows).hasSize(1)
      assertThat(rows.get(0).getDate(0)).isEqualTo(Date.valueOf("1988-01-05"))
    }

    @Test
    def returns_results_filtered_with_string_negated_equality(): Unit = {
      driver.executableQuery("CREATE (:Person {name: 'John Doe'}), (:Person {name: 'Jane Doe'})")
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("name")
        .where("NOT name = 'John Doe'")

      val rows = df.collectAsList()
      assertThat(rows).hasSize(1)
      assertThat(rows.get(0).getString(0)).isEqualTo("Jane Doe")
    }

    @Test
    def returns_results_filtered_with_date_negated_equality(): Unit = {
      driver.executableQuery("CREATE (:Person {birth: date('1998-02-04')}), (:Person {birth: date('1988-01-05')})")
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("birth")
        .where("NOT birth = '1988-01-05'")

      val rows = df.collectAsList()
      assertThat(rows).hasSize(1)
      assertThat(rows.get(0).getDate(0)).isEqualTo(Date.valueOf("1998-02-04"))
    }

    @Test
    def returns_results_filtered_with_string_difference(): Unit = {
      driver.executableQuery("CREATE (:Person {name: 'John Doe'}), (:Person {name: 'Jane Doe'})")
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("name")
        .where("name != 'John Doe'")

      val rows = df.collectAsList()
      assertThat(rows).hasSize(1)
      assertThat(rows.get(0).getString(0)).isEqualTo("Jane Doe")
    }

    @Test
    def returns_results_filtered_with_long_greater_comparison(): Unit = {
      driver.executableQuery(s"""
       CREATE (:Person {age: 19}),
        (:Person {age: 20}),
        (:Person {age: 21})
       """).execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("age")
        .where("age >= 20")

      val rows = df.collectAsList()
      assertThat(rows.asScala.map(_.getLong(0)).asJava)
        .containsOnlyOnce(20L, 21L)
    }

    @Test
    def returns_results_filtered_with_timestamp_greater_comparison(): Unit = {
      driver.executableQuery("""
       CREATE (:Person {birth: localdatetime('2007-12-03T10:15:30')}),
        (:Person {birth: localdatetime('2007-12-03T10:15:30')})
       """).execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("birth")
        .where("birth >= '2007-12-03T10:15:30'")

      assertThat(df.collectAsList()).hasSize(2)
    }

    @Test
    def returns_results_filtered_with_long_strict_greater_comparison(): Unit = {
      driver.executableQuery(
        s"""
       CREATE (:Person {age: 19}),
        (:Person {age: 20}),
        (:Person {age: 21})
       """
      ).execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("age")
        .where("age > 20")

      val rows = df.collectAsList()
      assertThat(rows).hasSize(1)
      assertThat(rows.get(0).getLong(0)).isEqualTo(21)
    }

    @Test
    def returns_results_filtered_with_date_strict_greater_comparison(): Unit = {
      driver.executableQuery(
        s"""
       CREATE (:Person {birth: date('1998-02-04')}),
        (:Person {birth: date('1988-01-05')}),
        (:Person {birth: date('1994-10-16')})
       """
      ).execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("birth")
        .where("birth > '1990-01-01'")

      val rows = df.collectAsList()
      assertThat(rows.asScala.map(_.getDate(0)).asJava)
        .containsOnlyOnce(
          Date.valueOf("1994-10-16"),
          Date.valueOf("1998-02-04")
        )
    }

    @Test
    def returns_results_filtered_with_geospatial_point_strict_greater_comparison(): Unit = {
      driver.executableQuery(
        s"""
       CREATE (:Person {location: point({x: 12, y: 12})}),
        (:Person {location: point({x: -6, y: -6})})
       """
      ).execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("location")
        .where("location.x > 0")

      val rows = df.collectAsList()
      assertThat(rows).hasSize(1)
      val row = rows.get(0).getAs[GenericRowWithSchema](0)
      assertThat(row.get(0)).isEqualTo("point-2d")
      assertThat(row.get(1)).isEqualTo(7203)
      assertThat(row.get(2)).isEqualTo(12.0)
      assertThat(row.get(3)).isEqualTo(12.0)
    }

    @Test
    def returns_results_filtered_with_long_lesser_comparison(): Unit = {
      driver.executableQuery(
        s"""
       CREATE (:Person {age: 39}),
        (:Person {age: 41}),
        (:Person {age: 43})
       """
      ).execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("age")
        .where("age <= 41")

      val rows = df.collectAsList()
      assertThat(rows.asScala.map(_.getLong(0)).asJava).containsOnlyOnce(39L, 41L)
    }

    @Test
    def returns_results_filtered_with_long_strict_lesser_comparison(): Unit = {
      driver.executableQuery(
        s"""
       CREATE (:Person {age: 39}),
        (:Person {age: 41}),
        (:Person {age: 43})
       """
      ).execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("age")
        .where("age < 40")

      val rows = df.collectAsList()
      assertThat(rows.asScala.map(_.getLong(0)).asJava).containsExactly(39L)
    }

    @Test
    def returns_results_filtered_in_long_collection(): Unit = {
      driver.executableQuery(
        s"""
       CREATE (:Person {age: 39}),
        (:Person {age: 41}),
        (:Person {age: 43})
       """
      ).execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("age")
        .where("age IN (41,43)")

      val rows = df.collectAsList()
      assertThat(rows.asScala.map(_.getLong(0)).asJava).containsOnlyOnce(41L, 43L)
    }

    @Test
    def returns_results_filtered_as_nullable(): Unit = {
      driver.executableQuery(
        s"""
       CREATE (:Person {age: 39}),
        (:Person {age: null}),
        (:Person {age: 43}),
        (:Person)
       """
      ).execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("age")
        .where("age IS NULL")

      assertThat(df.collectAsList()).hasSize(2)
    }

    @Test
    def returns_results_filtered_as_not_nullable(): Unit = {
      driver.executableQuery(
        s"""
       CREATE (:Person {age: 39}),
        (:Person {age: null}),
        (:Person {age: 43}),
        (:Person)
       """
      ).execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("age")
        .where("age IS NOT NULL")

      val rows = df.collectAsList()
      assertThat(rows.asScala.map(_.getLong(0)).asJava).containsOnlyOnce(39L, 43L)
    }

    @Test
    def returns_results_filtered_with_ORed_equalities(): Unit = {
      driver.executableQuery(
        s"""
       CREATE (:Person {age: 39}),
        (:Person {age: null}),
        (:Person {age: 43}),
        (:Person)
       """
      ).execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("age")
        .where("age = 43 OR age = 39 OR age = 32")

      val rows = df.collectAsList()
      assertThat(rows.asScala.map(_.getLong(0)).asJava).containsOnlyOnce(39L, 43L)
    }

    @Test
    def returns_results_filtered_with_ANDed_comparisons(): Unit = {
      driver.executableQuery(
        s"""
       CREATE (:Person {age: 39}),
        (:Person {age: null}),
        (:Person {age: 43}),
        (:Person)
       """
      ).execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("age")
        .where("age >= 39 AND age <= 43")

      val rows = df.collectAsList()
      assertThat(rows.asScala.map(_.getLong(0)).asJava).containsOnlyOnce(39L, 43L)
    }

    @Test
    def returns_results_filtered_with_string_prefix(): Unit = {
      driver.executableQuery(
        s"""
       CREATE (:Person {name: 'John Mayer'}),
        (:Person {name: 'John Scofield'}),
        (:Person {name: 'John Butler'}),
        (:Person)
       """
      ).execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("name")
        .where("name LIKE 'John%'")

      val rows = df.collectAsList()
      assertThat(rows.asScala.map(_.getString(0)).asJava).containsOnlyOnce("John Butler", "John Mayer", "John Scofield")
    }

    @Test
    def returns_results_filtered_with_string_suffix(): Unit = {
      driver.executableQuery(
        s"""
       CREATE (:Person {name: 'John Mayer'}),
        (:Person {name: 'John Scofield'}),
        (:Person {name: 'John Butler'}),
        (:Person)
       """
      ).execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("name")
        .where("name LIKE '%r'")

      val rows = df.collectAsList()
      assertThat(rows.asScala.map(_.getString(0)).asJava).containsOnlyOnce("John Butler", "John Mayer")
    }

    @Test
    def returns_results_filtered_with_substring_suffix(): Unit = {
      driver.executableQuery(
        s"""
       CREATE (:Person {name: 'John Mayer'}),
        (:Person {name: 'John Scofield'}),
        (:Person {name: 'John Butler'}),
        (:Person)
       """
      ).execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .select("name")
        .where("name LIKE '%ay%'")

      val rows = df.collectAsList()
      assertThat(rows.asScala.map(_.getString(0)).asJava).containsExactly("John Mayer")
    }

    @Test
    def throws_exception_when_wrong_database_is_provided(): Unit = {
      assertThatExceptionOfType(classOf[ClientException])
        .isThrownBy(() => {
          spark.read.format(classOf[DataSource].getName)
            .option("labels", "Household")
            .option("database", "not_existing_db")
            .load()
        })
        .withMessage("Database does not exist. Database name: 'not_existing_db'.")
    }

    @Test
    def supports_heterogeneous_schemas_for_same_label_nodes(): Unit = {
      driver.executableQuery("CREATE (:Person {id: 1, field: [12,34]}), (:Person {id: 2, field: 123})").execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      val rows = df.orderBy("id").collectAsList()
      assertThat(rows).hasSize(2)
      assertThat(rows.get(0).get(3)).isEqualTo("[12,34]")
      assertThat(rows.get(1).get(3)).isEqualTo("123")
    }

    @Test
    def supports_heterogeneous_schemas_for_nodes_with_multiple_labels(): Unit = {
      driver.executableQuery(s"""CREATE (:Person { prop: 25 }),
                                |(:Person:Player { prop: "hello" }),
                                |(:Person:Player:Weirdo { prop: true })
    """.stripMargin).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      assertThat(df.columns.toSeq.asJava)
        .containsOnlyOnce("prop", "<elementId>", "<labels>")
    }

    @Test
    def returns_same_properties_for_nodes_with_multiple_labels(): Unit = {
      driver.executableQuery(
        s"""CREATE (actor:Person:Actor {name: 'Keanu Reeves', born: 1964, actor: true})
           |CREATE (soccerPlayer:Person:SoccerPlayer {name: 'Zlatan Ibrahimović', born: 1981, soccerPlayer: true})
           |CREATE (writer:Person:Writer {name: 'Philip K. Dick', born: 1928, writer: true})""".stripMargin
      )
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()

      assertThat(df.columns.toSeq.asJava)
        .containsOnlyOnce("name", "born", "actor", "soccerPlayer", "writer", "<elementId>", "<labels>")
    }

    @Test
    def supports_repartitioning(): Unit = {
      driver.executableQuery("""UNWIND range(1,100) as id
                               |CREATE (p:Person {id:id,ids:[id,id]}) WITH collect(p) as people
                               |UNWIND people as p1
                               |UNWIND range(1,10) as friend
                               |WITH p1, people[(p1.id + friend) % size(people)] as p2
                               |CREATE (p1)-[:KNOWS]->(p2)
    """.stripMargin).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", "Person")
        .load()
        .repartition(10)

      assertThat(df.rdd.getNumPartitions).isEqualTo(10)
      assertThat(df.collect().length).isEqualTo(100)
    }

    @Test
    def supports_custom_partitions(): Unit = {
      driver.executableQuery("""UNWIND range(1,100) as id
                               |CREATE (:Person:Customer {id: id, name: 'Person ' + id})
      """.stripMargin).execute()
      driver.executableQuery("""UNWIND range(1,100) as id
                               |CREATE (p:Person {id:id,ids:[id,id]}) WITH collect(p) as people
                               |UNWIND people as p1
                               |UNWIND range(1,10) as friend
                               |WITH p1, people[(p1.id + friend) % size(people)] as p2
                               |CREATE (p1)-[:KNOWS]->(p2)
      """.stripMargin).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("labels", ":Person:Customer")
        .option("partitions", "5")
        .load()

      assertThat(df.rdd.getNumPartitions).isEqualTo(5)
      val ids = df.collect().map(_.getAs[Long]("id"))
      assertThat(ids.toSet.asJava).hasSize(100)
      assertThat(ids.toSeq.asJava).hasSize(100)
    }

    @Test
    def supports_limit(): Unit = {
      driver.executableQuery(s"""UNWIND range(1, 100) as id
                                |CREATE (:Product {id: id, name: 'Product ' + id})""".stripMargin)
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("labels", "Product")
        .load()
        .limit(10)

      assertThat(df.count()).isEqualTo(10)
    }
  }

  @Nested
  @DisplayName("by relationship type")
  class ByRelationshipType {

    @Test
    def returns_only_selected_field(): Unit = {
      driver.executableQuery(
        s"""UNWIND range(1, 100) as id
           |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
           |CREATE (pe:Person {id: id, name: 'Person ' + id})
           |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
    """.stripMargin
      )
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("relationship.source.labels", "Person")
        .option("relationship", "BOUGHT")
        .option("relationship.target.labels", "Product")
        .load()
        .select("`source.name`", "`<source.elementId>`")

      assertThat(df.columns.toSeq).isEqualTo(immutable.Seq("source.name", "<source.elementId>"))
    }

    @Test
    def supports_selecting_relationship_builtin_field(): Unit = {
      driver.executableQuery(
        s"""UNWIND range(1, 100) as id
           |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
           |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
           |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
    """.stripMargin
      )
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("relationship.source.labels", "Person")
        .option("relationship", "BOUGHT")
        .option("relationship.target.labels", "Product")
        .load()
        .select("`<rel.type>`")

      assertThat(df.columns.toSet).isEqualTo(Set("<rel.type>"))
    }

    @Test
    def returns_only_selected_field_with_unconventional_name(): Unit = {
      driver.executableQuery(
        s"""UNWIND range(1, 100) as id
           |CREATE (pr:Product {id: id * rand(), `(╯°□°)╯︵ ┻━┻`: 'Product ' + id})
           |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
           |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
    """.stripMargin
      )
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("relationship.source.labels", "Person")
        .option("relationship", "BOUGHT")
        .option("relationship.target.labels", "Product")
        .load()
        .select("`target.(╯°□°)╯︵ ┻━┻`", "`<source.elementId>`")

      assertThat(df.columns.toSeq).isEqualTo(immutable.Seq("target.(╯°□°)╯︵ ┻━┻", "<source.elementId>"))
    }

    @Test
    def returns_results_filtered_with_source_and_target_attributes(): Unit = {
      driver.executableQuery(
        """UNWIND range(1,100) as id
          |CREATE (p:Person {id:id,ids:[id,id]}) WITH collect(p) as people
          |UNWIND people as p1
          |UNWIND range(1,10) as friend
          |WITH p1, people[(p1.id + friend) % size(people)] as p2
          |CREATE (p1)-[:KNOWS]->(p2)
    """.stripMargin
      )
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", "Person")
        .option("relationship", "KNOWS")
        .option("relationship.target.labels", "Person")
        .load()
        .filter("`source.id` = '14' AND `target.id` = '16'")

      assertThat(df.count()).isEqualTo(1L)
    }

    @Test
    def returns_results_filtered_with_source_and_target_map_attributes(): Unit = {
      driver.executableQuery(
        """UNWIND range(1,100) as id
          |CREATE (p:Person {id:id,ids:[id,id]}) WITH collect(p) as people
          |UNWIND people as p1
          |UNWIND range(1,10) as friend
          |WITH p1, people[(p1.id + friend) % size(people)] as p2
          |CREATE (p1)-[:KNOWS]->(p2)
    """.stripMargin
      )
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("relationship.nodes.map", "true")
        .option("relationship.source.labels", "Person")
        .option("relationship", "KNOWS")
        .option("relationship.target.labels", "Person")
        .load()
        .filter("`<source>`.`id` = '14' AND `<target>`.`id` = '16'")

      assertThat(df.count()).isEqualTo(1L)
    }

    @Test
    def returns_results_filtered_with_target_attributes(): Unit = {
      driver.executableQuery(
        s"""UNWIND range(1, 100) as id
           |CREATE (pr:Product {id: id, name: 'Product ' + id})
           |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
           |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
    """.stripMargin
      )
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("relationship", "BOUGHT")
        .option("relationship.source.labels", "Person")
        .option("relationship.target.labels", "Product")
        .load()
        .filter("`target.name` = 'Product 16' AND `target.id` = 16")
        .select("`target.name`", "`target.id`")

      assertThat(df.columns.toSeq).isEqualTo(immutable.Seq("target.name", "target.id"))
      assertThat(df.select("`target.name`").collect().map(_.getString(0)).toSet)
        .isEqualTo(Set("Product 16"))
      assertThat(df.select("`target.id`").collect().map(_.getLong(0)).toSet)
        .isEqualTo(Set(16L))
    }

    @Test
    def supports_heterogeneous_schemas_for_property_value_types(): Unit = {
      driver.executableQuery(s"""CREATE (pr1:Product {id: '1'})
                                |CREATE (pr2:Product {id: 2})
                                |CREATE (pe1:Person {id: '3'})
                                |CREATE (pe2:Person {id: 4})
                                |CREATE (pe1)-[:BOUGHT]->(pr1)
                                |CREATE (pe2)-[:BOUGHT]->(pr2)
    """.stripMargin).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("relationship.nodes.map", "false")
        .option("relationship", "BOUGHT")
        .option("relationship.source.labels", ":Person")
        .option("relationship.target.labels", ":Product")
        .load()

      val rows = df.collectAsList()
      assertThat(rows.asScala.map(row => (row.get(4), row.get(7))).asJava)
        .containsOnlyOnce(("3", "1"), ("4", "2"))
    }

    @Test
    def supports_custom_partitions(): Unit = {
      driver.executableQuery("""UNWIND range(1,100) as id
                               |CREATE (:Person {id: id, name: 'Person ' + id})-[:BOUGHT{quantity: ceil(rand() * 100)}]->(:Product{id: id, name: 'Product ' + id})
    """.stripMargin).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("relationship.nodes.map", "true")
        .option("relationship.source.labels", ":Person")
        .option("relationship", "BOUGHT")
        .option("relationship.target.labels", ":Product")
        .option("partitions", "5")
        .load()

      assertThat(df.rdd.getNumPartitions).isEqualTo(5)
      val ids = df.collect().map(_.getAs[String]("<rel.elementId>"))
      assertThat(ids.toSet.asJava).hasSize(100)
      assertThat(ids.toSeq.asJava).hasSize(100)
    }

    @Test
    def supports_flattening(): Unit = {
      driver.executableQuery(s"""UNWIND range(1, 100) as id
                                |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
                                |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
                                |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
    """.stripMargin).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("relationship", "BOUGHT")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Person")
        .option("relationship.target.labels", ":Product")
        .load()

      val rows = df.collectAsList()
      assertThat(rows.asScala.count(row =>
        row.getAs[String]("<rel.elementId>").nonEmpty
          && row.getAs[String]("<rel.type>") != null
          && row.getAs[Double]("rel.when") >= 0
          && row.getAs[Double]("rel.quantity") >= 0
          && row.getAs[String]("<source.elementId>").nonEmpty
          && row.getAs[Long]("source.id") >= 0
          && row.getAs[immutable.Seq[String]]("<source.labels>").nonEmpty
          && row.getAs[String]("source.fullName") != null
          && row.getAs[String]("<target.elementId>").nonEmpty
          && row.getAs[Double]("target.id") >= 0
          && row.getAs[immutable.Seq[String]]("<target.labels>").nonEmpty
          && row.getAs[String]("target.name") != null
      )).isEqualTo(100)
    }

    @Test
    def supports_nodes_as_map(): Unit = {
      driver.executableQuery(s"""UNWIND range(1, 100) as id
                                |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
                                |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
                                |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
    """.stripMargin).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("relationship", "BOUGHT")
        .option("relationship.source.labels", ":Person")
        .option("relationship.nodes.map", "true")
        .option("relationship.target.labels", ":Product")
        .load()

      val rows = df.collectAsList()
      assertThat(rows.asScala.count(row =>
        row.getAs[String]("<rel.elementId>").nonEmpty
          && row.getAs[String]("<rel.type>") != null
          && row.getAs[Double]("rel.when") >= 0
          && row.getAs[Double]("rel.quantity") >= 0
          && row.getAs[Map[String, String]]("<source>") != null
          && row.getAs[Map[String, String]]("<target>") != null
      )).isEqualTo(100)
      assertThat(rows.asScala.map(row => row.getAs[Map[String, String]]("<source>"))
        .count(row => row.keys == Set("id", "fullName", "<elementId>", "<labels>"))).isEqualTo(100)
      assertThat(rows.asScala.map(row => row.getAs[Map[String, String]]("<target>"))
        .count(row => row.keys == Set("id", "name", "<elementId>", "<labels>"))).isEqualTo(100)
    }

    @Test
    def supports_limit(): Unit = {
      driver.executableQuery(s"""UNWIND range(1, 100) as id
                                |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
                                |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
                                |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)""".stripMargin)
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("relationship.source.labels", "Person")
        .option("relationship", "BOUGHT")
        .option("relationship.target.labels", "Product")
        .load()
        .limit(10)

      assertThat(df.count()).isEqualTo(10)
    }

    @Test
    def supports_limit_in_conjunction_with_ordering(): Unit = {
      driver.executableQuery(s"""UNWIND range(1, 100) as id
                                |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
                                |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
                                |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
    """.stripMargin)
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("relationship.source.labels", "Person")
        .option("relationship", "BOUGHT")
        .option("relationship.target.labels", "Product")
        .load()
        .select("`target.name`", "`target.id`")
        .orderBy(col("`target.name`").desc)
        .limit(10)

      assertThat(df.count()).isEqualTo(10)
      assertThat(df.columns.toSet).isEqualTo(Set("target.name", "target.id"))
    }

    @Test
    def supports_SQL_sum_aggregation(): Unit = {
      driver.executableQuery(s"""CREATE (pe:Person {id: 1, fullName: 'Person'})-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr:Product {id: 0, name: 'Product ' + 0, price: 1})
                                |WITH pe
                                |UNWIND range(1, 10) as id
                                |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id, price: id})
                                |CREATE (pe)-[:BOUGHT {when: rand(), quantity: rand() * 1000}]->(pr)
    """.stripMargin)
        .execute()

      spark.read
        .format(classOf[DataSource].getName)
        .option("relationship.source.labels", "Person")
        .option("relationship", "BOUGHT")
        .option("relationship.target.labels", "Product")
        .load()
        .createTempView("BOUGHT")
      val df = spark.sql("""SELECT `source.fullName`,
                           |   SUM(DISTINCT(`target.price`)) AS distinctTotal,
                           |   SUM(`target.price`) AS total
                           |FROM BOUGHT
                           |group by `source.fullName`""".stripMargin)

      val rows = df.collectAsList()
      assertThat(rows).hasSize(1)
      val row = rows.get(0)
      assertThat(row.getAs[String]("source.fullName")).isEqualTo("Person")
      assertThat(row.getAs[Long]("distinctTotal")).isEqualTo(55L)
      assertThat(row.getAs[Long]("total")).isEqualTo(56L)
    }

    @Test
    def supports_SQL_min_max_aggregation(): Unit = {
      driver.executableQuery(s"""CREATE (pe:Person {id: 1, fullName: 'Person'})-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr:Product {id: 0, name: 'Product ' + 0, price: 1})
                                |WITH pe
                                |UNWIND range(1, 10) as id
                                |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id, price: id})
                                |CREATE (pe)-[:BOUGHT {when: rand(), quantity: rand() * 1000}]->(pr)
    """.stripMargin)
        .execute()

      spark.read
        .format(classOf[DataSource].getName)
        .option("relationship.source.labels", "Person")
        .option("relationship", "BOUGHT")
        .option("relationship.target.labels", "Product")
        .load()
        .createTempView("BOUGHT")
      val df = spark.sql("""SELECT `source.fullName`,
                           |    MAX(`target.price`) AS max,
                           |    MIN(`target.price`) AS min
                           |FROM BOUGHT
                           |GROUP BY `source.fullName`""".stripMargin)

      val rows = df.collectAsList()
      assertThat(rows).hasSize(1)
      val row = rows.get(0)
      assertThat(row.getAs[String]("source.fullName")).isEqualTo("Person")
      assertThat(row.getAs[Long]("max")).isEqualTo(10L)
      assertThat(row.getAs[Long]("min")).isEqualTo(1L)
    }

    @Test
    def supports_SQL_count_aggregation(): Unit = {
      driver.executableQuery(s"""CREATE (pe:Person {id: 1, fullName: 'Person'})-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr:Product {id: 1, name: 'Product ' + 0, price: 1})
                                |WITH pe
                                |UNWIND range(1, 10) as id
                                |MERGE (p:Product {id: id, name: 'Product ' + id, price: id * rand()})
                                |CREATE (pe)-[:BOUGHT {when: rand(), quantity: rand() * 1000}]->(p)
    """.stripMargin)
        .execute()

      spark.read
        .format(classOf[DataSource].getName)
        .option("relationship.source.labels", "Person")
        .option("relationship", "BOUGHT")
        .option("relationship.target.labels", "Product")
        .load()
        .createTempView("BOUGHT")
      val df = spark.sql("""SELECT `source.fullName`,
                           |    COUNT(DISTINCT(`target.id`)) AS distinctTotal,
                           |    COUNT(`target.id`) AS total
                           |FROM BOUGHT
                           |group by `source.fullName`""".stripMargin)

      val rows = df.collectAsList()
      assertThat(rows).hasSize(1)
      val row = rows.get(0)
      assertThat(row.getAs[String]("source.fullName")).isEqualTo("Person")
      assertThat(row.getAs[Long]("distinctTotal")).isEqualTo(10L)
      assertThat(row.getAs[Long]("total")).isEqualTo(11L)
    }

  }

  @Nested
  @DisplayName("by query")
  class ByQuery {

    @Test
    def supports_returning_lists(): Unit = {
      val df = spark.read.format(classOf[DataSource].getName)
        .option("query", "RETURN [1, 'foo'] AS list")
        .load()

      val result = df.collect()(0).getAs[mutable.Seq[_]]("list")

      assertThat(result).isEqualTo(mutable.Seq("1", "foo"))
    }

    @Test
    def supports_returning_maps(): Unit = {
      val df = spark.read.format(classOf[DataSource].getName)
        .option("query", "RETURN {a: 1, b: '3'} AS map")
        .load()

      val result = df.collect()(0).getAs[Map[String, String]]("map")

      assertThat(result).isEqualTo(Map("a" -> "1", "b" -> "3"))
    }

    @Test
    def supports_returning_list_of_maps(): Unit = {
      val df = spark.read.format(classOf[DataSource].getName)
        .option("query", "RETURN [{a: 1, b: '3'}, {a: 'foo'}] AS listMap")
        .load()

      val result = df.collect()(0).getAs[immutable.Seq[_]]("listMap").toList

      assertThat(result).isEqualTo(immutable.Seq(Map("a" -> "1", "b" -> "3"), Map("a" -> "foo")))
    }

    @Test
    def supports_calling_procedure(): Unit = {
      val df = spark.read.format(classOf[DataSource].getName)
        .option("query", "CALL db.info() YIELD name RETURN *")
        .load()

      val dbName = df.select("name").collectAsList().get(0).getString(0)

      assertThat(dbName).isEqualTo("neo4j")
    }

    @Test
    def supports_calling_apoc_procedure(): Unit = {
      Assumptions.assumeTrue(driver.serverSupportsApoc())

      val df = spark.read.format(classOf[DataSource].getName)
        .option("query", "RETURN apoc.convert.toSet([1,1,3]) AS foo, 'bar' AS bar")
        .load()

      assertThat(df.columns.toSeq.asJava).containsExactly("foo", "bar")
      assertThat(df.count()).isEqualTo(1)
    }

    @Test
    def returns_only_selected_field(): Unit = {
      driver.executableQuery(
        s"""UNWIND range(1, 100) as id
           |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
           |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
           |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
    """.stripMargin
      )
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("query", "MATCH (p:Product) RETURN p.name as name")
        .option("partitions", 2)
        .option("query.count", 20)
        .load()
        .select("name")

      assertThat(df.columns.toSeq).isEqualTo(immutable.Seq("name"))
    }

    @Test
    def returns_empty_dataset(): Unit = {
      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("query", "MATCH (e:NotAnExistingLabel) RETURN elementId(e) as f, 1 as g")
        .load()

      assertThat(df.count()).isEqualTo(0)
      assertThat(df.columns.toSeq).isEqualTo(immutable.Seq("f", "g"))
    }

    @Test
    def returns_ordered_results(): Unit = {
      driver.executableQuery("CREATE (:Instrument {name: 'Drums', id: 1}), (:Instrument {name: 'Guitar', id: 2})")
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("query", "MATCH (i:Instrument) RETURN i.id as id, i.name as name")
        .load()
        .orderBy("id")

      assertThat(df.collect()).containsExactly(
        Row(1L, "Drums"),
        Row(2L, "Guitar")
      )
    }

    @Test
    def supports_complex_return_clauses(): Unit = {
      driver.executableQuery(
        s"""UNWIND range(1, 100) as id
           |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
           |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
           |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
    """.stripMargin
      )
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option(
          "query",
          """MATCH (p:Person)-[b:BOUGHT]->(pr:Product)
            |RETURN elementId(p) AS personId, elementId(pr) AS productId,
            |       {quantity: b.quantity, when: b.when} AS map,
            |       "some string" as someString,
            |       {anotherField: "201"} as map2
          """.stripMargin
        )
        .option("schema.strategy", "string")
        .load()

      assertThat(df.count()).isEqualTo(100)
      assertThat(df.columns.toSeq).isEqualTo(immutable.Seq("personId", "productId", "map", "someString", "map2"))
    }

    @Test
    def supports_complex_return_clauses_on_empty_data_set(): Unit = {
      val df = spark.read
        .format(classOf[DataSource].getName)
        .option(
          "query",
          """MATCH (p:PersonNotInGraph)-[b:BOUGHT]->(pr:ProductNotInGraph)
            |RETURN elementId(p) AS personId, elementId(pr) AS productId,
            |       {quantity: b.quantity, when: b.when} AS map,
            |       "some string" as someString,
            |       {anotherField: "201"} as map2
                                 """.stripMargin
        )
        .option("schema.strategy", "string")
        .load()

      assertThat(df.count()).isEqualTo(0)
      assertThat(df.columns.toSeq).isEqualTo(immutable.Seq("personId", "productId", "map", "someString", "map2"))
    }

    @Test
    def supports_custom_partitions(): Unit = {
      driver.executableQuery(
        """CREATE (pr:Product{id: 1, name: 'Product 1'})
          |WITH pr
          |UNWIND range(1,100) as id
          |CREATE (:Person {id: id, name: 'Person ' + id})-[:BOUGHT{quantity: ceil(rand() * 100)}]->(pr)
    """.stripMargin
      ).execute()
      driver.executableQuery(
        """CREATE (pr:Product{id: 2, name: 'Product 2'})
          |WITH pr
          |UNWIND range(1,50) as id
          |MATCH (p:Person {id: id})
          |CREATE (p)-[:BOUGHT{quantity: ceil(rand() * 100)}]->(pr)
    """.stripMargin
      ).execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option(
          "query",
          """
            |MATCH (p:Person)-[r:BOUGHT]->(pr:Product)
            |RETURN p.name AS person, pr.name AS product, r.quantity AS quantity""".stripMargin
        )
        .option("partitions", "5")
        .load()

      assertThat(df.rdd.getNumPartitions).isEqualTo(5)
      val rows = df.collect()
        .map(row => s"${row.getAs[String]("person")}-${row.getAs[String]("product")}")
      assertThat(rows).hasSize(150)
    }

    @Test
    def supports_filtering_on_nodes(): Unit = {
      driver.executableQuery(s"""UNWIND range(1, 100) as id
                                |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
                                |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
                                |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
    """.stripMargin)
        .execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option("query", "MATCH (n:Person) WITH n LIMIT 2 RETURN collect(n) AS nodes")
        .load()

      val rows = df.collect()
      assertThat(rows.flatMap(row => row.getAs[mutable.Seq[Row]]("nodes")).count(row =>
        row.getAs[String]("<elementId>").nonEmpty
          && row.getAs[mutable.Seq[String]]("<labels>").nonEmpty
          && row.getAs[String]("fullName").nonEmpty
          && row.getAs[Long]("id") >= 0
      ))
        .isEqualTo(2)
    }

    @Test
    def supports_filtering_on_complex_results_encoded_as_strings(): Unit = {
      driver.executableQuery(s"""UNWIND range(1, 100) as id
                                |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
                                |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
                                |CREATE (pe)-[:BOUGHT {when: rand(), quantity: rand() * 1000}]->(pr)
    """.stripMargin)
        .execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option(
          "query",
          """MATCH (p:Person)-[b:BOUGHT]->(pr:Product)
            |RETURN elementId(p) AS personId, elementId(pr) AS productId, {quantity: b.quantity, when: b.when} AS map""".stripMargin
        )
        .option("schema.strategy", "string")
        .load()

      val rows = df.collect()
      assertThat(rows.count(row =>
        row.getAs[String]("personId").nonEmpty
          && row.getAs[String]("productId").nonEmpty
          && row.getAs[String]("map").nonEmpty
      ))
        .isEqualTo(100)
    }

    @Test
    def supports_filtering_on_relationships(): Unit = {
      driver.executableQuery(s"""UNWIND range(1, 100) as id
                                |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
                                |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
                                |CREATE (pe)-[:BOUGHT {when: rand(), quantity: rand() * 1000}]->(pr)
    """.stripMargin)
        .execute()

      val df = spark.read.format(classOf[DataSource].getName)
        .option(
          "query",
          """MATCH (p:Person)-[b:BOUGHT]->(pr:Product)
            |RETURN b AS rel""".stripMargin
        )
        .load()

      val rows = df.collect()
      assertThat(rows.map(_.getAs[Row]("rel")).count(row =>
        row.getAs[String]("<rel.elementId>").nonEmpty
          && row.getAs[String]("<rel.type>").nonEmpty
          && row.getAs[String]("<source.elementId>").nonEmpty
          && row.getAs[String]("<target.elementId>").nonEmpty
          && row.getAs[java.lang.Double]("when") != null
          && row.getAs[java.lang.Double]("quantity") != null
      ))
        .isEqualTo(100)
    }

    @Test
    def orders_columns_by_their_declaration_order_in_query_return_clause(): Unit = {
      driver.executableQuery("CREATE (:Instrument{name: 'Drums', id: 1}), (:Instrument{name: 'Guitar', id: 2})")
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("query", "MATCH (i:Instrument) RETURN elementId(i) as internal_id, i.id as id, i.name as name, i.name")
        .load()
        .orderBy("id")

      assertThat(df.columns.toSet).isEqualTo(Set("internal_id", "id", "name", "i.name"))
    }

    @Test
    def supports_script_result_injection(): Unit = {
      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("script", "RETURN 'foo' AS val")
        .option("query", "UNWIND range(1,2) as id RETURN id AS val, scriptResult[0].val AS script")
        .option("partitions", 2)
        .option("query.count", 2)
        .load()

      val rows = df.collect()
      assertThat(rows
        .map(row => (row.getAs[String]("script"), row.getAs[Long]("val")))
        .toSeq.asJava)
        .containsOnlyOnce(("foo", 1), ("foo", 2))
    }

    @ParameterizedTest
    @ValueSource(strings = Array("limit", "\nlimit", "LIMIT", "\nLIMIT", "lImIT", "\nlImIT"))
    def fails_if_limit_is_used_at_the_end_of_the_query(limitKeyword: String): Unit = {
      assertThatExceptionOfType(classOf[IllegalArgumentException])
        .isThrownBy(() => {
          spark.read
            .format(classOf[DataSource].getName)
            .option("query", s"MATCH (n:Label) RETURN elementId(n) as id $limitKeyword 100")
            .load()
            .show() // show is needed to trigger the exception because of changes in Spark 3
        })
        .withMessage("SKIP/LIMIT are not allowed at the end of the query")
    }

    @ParameterizedTest
    @ValueSource(strings = Array("limit", "\nlimit", "LIMIT", "\nLIMIT", "lImIT", "\nlImIT"))
    def supports_limit_keyword_when_not_at_the_end_of_the_query(limitKeyword: String): Unit = {
      driver.executableQuery(s"""UNWIND range(1, 100) as id
                                |CREATE (:Product {id: id, name: 'Product ' + id})""".stripMargin)
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("query", s"MATCH (p:Product) WITH p $limitKeyword 10\nRETURN p")
        .load()

      assertThat(df.count()).isEqualTo(10)
    }

    @Test
    def supports_user_defined_schema(): Unit = {
      driver.executableQuery("CREATE (p:Person {name: 'Foo Bar', age: 8})")
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .schema(StructType(immutable.Seq(StructField("age", DataTypes.StringType))))
        .option("query", "MATCH (n:Person) RETURN n.age AS age")
        .load()

      val rows = df.collect()
      assertThat(rows.map(_.get(0)).toSeq.asJava).containsOnlyOnce("8")
    }

    @Test
    def supports_query_ordering(): Unit = {
      driver.executableQuery("CREATE (p:Person {name: 'Foo Bar', age: 8})")
        .execute()

      val df = spark.read
        .format(classOf[DataSource].getName)
        .option("query", "MATCH (n:Person) RETURN n.age AS age ORDER by age")
        .load()

      val rows = df.collect()
      assertThat(rows.map(_.get(0)).toSeq.asJava).containsOnlyOnce(8L)
    }

    @Test // https://github.com/neo4j/neo4j-spark-connector/issues/531
    def supports_nullable_datetime_properties(): Unit = {
      val df = spark.read
        .format(classOf[DataSource].getName)
        .option(
          "query",
          """
            |UNWIND [
            |  {first: '2022-06-14T10:02:28.192Z', second: null},
            |  {first: '2022-06-15T10:02:28.192Z', second: '2022-06-16T10:02:28.192Z'}]AS event
            |RETURN datetime(event.first) AS first, datetime(event.second) AS second
            |""".stripMargin
        )
        .load()

      assertThat(df.schema).isEqualTo(StructType(Array(
        StructField("first", DataTypes.TimestampType),
        StructField("second", DataTypes.StringType)
      )))
      val rows = df.collect()
      assertThat(rows.map(row => (row.getTimestamp(0), row.getString(1))).toSeq.asJava)
        .containsOnlyOnce(
          (Timestamp.from(OffsetDateTime.parse("2022-06-14T10:02:28.192Z").toInstant), null),
          (Timestamp.from(OffsetDateTime.parse("2022-06-15T10:02:28.192Z").toInstant), "2022-06-16T10:02:28.192Z")
        )
    }

    @Test // https://github.com/neo4j/neo4j-spark-connector/issues/531
    def supports_nullable_datetime_properties_with_schema(): Unit = {
      val df = spark.read
        .format(classOf[DataSource].getName)
        .schema(StructType(Array(
          StructField("first", DataTypes.TimestampType),
          StructField("second", DataTypes.TimestampType)
        )))
        .option(
          "query",
          """
            |UNWIND [
            |  {first: '2022-06-14T10:02:28.192Z', second: null},
            |  {first: '2022-06-15T10:02:28.192Z', second: '2022-06-16T10:02:28.192Z'}]AS event
            |RETURN datetime(event.first) AS first, datetime(event.second) AS second
            |""".stripMargin
        )
        .load()

      assertThat(df.schema).isEqualTo(StructType(Array(
        StructField("first", DataTypes.TimestampType),
        StructField("second", DataTypes.TimestampType)
      )))
      val rows = df.collect()
      assertThat(rows.map(row => (row.getTimestamp(0), row.getTimestamp(1))).toSeq.asJava)
        .containsOnlyOnce(
          (Timestamp.from(OffsetDateTime.parse("2022-06-14T10:02:28.192Z").toInstant), null),
          (
            Timestamp.from(OffsetDateTime.parse("2022-06-15T10:02:28.192Z").toInstant),
            Timestamp.from(OffsetDateTime.parse("2022-06-16T10:02:28.192Z").toInstant)
          )
        )
    }
  }
}
