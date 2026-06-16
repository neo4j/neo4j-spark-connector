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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.util.SetSystemProperty
import org.junit.jupiter.params.Parameter
import org.junit.jupiter.params.ParameterizedClass
import org.junit.jupiter.params.provider.ArgumentsSource
import org.neo4j.driver.Driver
import org.neo4j.spark.testsupport.Assert
import org.neo4j.spark.testsupport.Neo4jContainerProvider
import org.neo4j.spark.testsupport.Neo4jExtensions.DriverExtensions
import org.neo4j.spark.testsupport.Neo4jExtensions.Neo4jContainerExtensions
import org.testcontainers.neo4j.Neo4jContainer

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import scala.collection.immutable
import scala.collection.mutable
import scala.jdk.CollectionConverters.SeqHasAsJava

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ParameterizedClass(name = "{argumentSetName}")
@ArgumentsSource(classOf[Neo4jContainerProvider])
@DisplayName("streaming")
@SetSystemProperty(key = "strict.cypher", value = "true")
class StreamingIT {

  @Parameter
  var neo4jContainer: Neo4jContainer = _

  @TempDir
  var folder: Path = _

  var driver: Driver = _

  var spark: SparkSession = _

  private var query: StreamingQuery = _

  private val createdTables = mutable.ListBuffer.empty[String]

  private val dataSourceFormat = classOf[DataSource].getName

  private val OptPropertyName = "streaming.property.name"
  private val OptFrom = "streaming.from"
  private val OptQueryOffset = "streaming.query.offset"

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
    Option(query).foreach(_.stop())
    Option(spark).foreach { session =>
      createdTables.foreach(table => session.sql(s"DROP TABLE IF EXISTS $table"))
      createdTables.clear()
      session.close()
    }
    Option(driver).foreach(_.close())
  }

  @Nested
  @DisplayName("from source")
  class FromSource {

    private val total = 60

    @Test
    def reads_nodes_from_now(): Unit = {
      createMovieNodes(0, 1)

      query = spark.readStream.format(dataSourceFormat)
        .option("labels", "Movie")
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "NOW")
        .load()
        .writeStream
        .format("memory")
        .queryName("nodesFromNow")
        .start()

      Executors.newSingleThreadExecutor().execute(() => createMovieNodes(1, total, 1000, 200))

      val expected = (1 to total)
        .map(index => Map("<labels>" -> List("Movie"), "title" -> s"My movie $index"))
        .toList

      Assert.assertEventually(
        expected,
        () => select("SELECT * FROM nodesFromNow ORDER BY timestamp", "<labels>", "title"),
        30L,
        TimeUnit.SECONDS
      )
    }

    @Test
    def reads_all_nodes(): Unit = {
      createMovieNodes(0, 1)

      query = spark.readStream.format(dataSourceFormat)
        .option("labels", "Movie")
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "ALL")
        .load()
        .writeStream
        .format("memory")
        .queryName("allNodes")
        .start()

      Executors.newSingleThreadExecutor().execute(() => createMovieNodes(1, total, 1000, 200))

      val expected = (0 to total)
        .map(index => Map("<labels>" -> List("Movie"), "title" -> s"My movie $index"))
        .toList

      Assert.assertEventually(
        expected,
        () => select("SELECT * FROM allNodes ORDER BY timestamp", "<labels>", "title"),
        30L,
        TimeUnit.SECONDS
      )
    }

    @Test
    def resumes_reading_nodes_from_checkpoint(): Unit = {
      createMovieNodes(0, 1)

      val stream = spark.readStream.format(dataSourceFormat)
        .option("labels", "Movie")
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "NOW")
        .load()

      val location = checkpoint()
      val table = uniqueTable("nodesCheckpoint")
      val partial = total / 2

      drainToTable(stream, location, table)
      createMovieNodes(1, partial, 0, 10)
      drainToTable(stream, location, table)
      createMovieNodes(partial + 1, total - partial, 0, 10)
      drainToTable(stream, location, table)

      val expected = (1 to total)
        .map(index => Map("<labels>" -> List("Movie"), "title" -> s"My movie $index"))
        .toList
      assertThat(select(s"SELECT * FROM $table ORDER BY timestamp", "<labels>", "title").asJava)
        .containsExactlyElementsOf(expected.asJava)
    }

    @Test
    def reads_relationships_from_now(): Unit = {
      createLikesRelationships(0, 1)

      query = spark.readStream.format(dataSourceFormat)
        .option("relationship", "LIKES")
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "NOW")
        .option("relationship.source.labels", "Person")
        .option("relationship.target.labels", "Post")
        .load()
        .writeStream
        .format("memory")
        .queryName("relationshipsFromNow")
        .start()

      Executors.newSingleThreadExecutor().execute(() => createLikesRelationships(1, total, 1000, 200))

      val expected = (1 to total).map(likeRow).toList
      Assert.assertEventually(
        expected,
        () =>
          select(
            "SELECT * FROM relationshipsFromNow ORDER BY `rel.timestamp`",
            "<rel.type>",
            "<source.labels>",
            "source.age",
            "<target.labels>",
            "target.hash",
            "rel.id"
          ),
        30L,
        TimeUnit.SECONDS
      )
    }

    @Test
    def reads_all_relationships(): Unit = {
      createLikesRelationships(0, 1)

      query = spark.readStream.format(dataSourceFormat)
        .option("relationship", "LIKES")
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "ALL")
        .option("relationship.source.labels", "Person")
        .option("relationship.target.labels", "Post")
        .load()
        .writeStream
        .format("memory")
        .queryName("allRelationships")
        .start()

      Executors.newSingleThreadExecutor().execute(() => createLikesRelationships(1, total, 1000, 200))

      val expected = (0 to total).map(likeRow).toList
      Assert.assertEventually(
        expected,
        () =>
          select(
            "SELECT * FROM allRelationships ORDER BY `rel.timestamp`",
            "<rel.type>",
            "<source.labels>",
            "source.age",
            "<target.labels>",
            "target.hash",
            "rel.id"
          ),
        30L,
        TimeUnit.SECONDS
      )
    }

    @Test
    def resumes_reading_relationships_from_checkpoint(): Unit = {
      createLikesRelationships(0, 1)

      val stream = spark.readStream.format(dataSourceFormat)
        .option("relationship", "LIKES")
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "ALL")
        .option("relationship.source.labels", "Person")
        .option("relationship.target.labels", "Post")
        .load()

      val location = checkpoint()
      val table = uniqueTable("relationshipsCheckpoint")
      val partial = total / 2

      drainToTable(stream, location, table)
      createLikesRelationships(1, partial, 0, 10)
      drainToTable(stream, location, table)
      createLikesRelationships(partial + 1, total - partial, 0, 10)
      drainToTable(stream, location, table)

      val expected = (0 to total).map(likeRow).toList
      assertThat(select(
        s"SELECT * FROM $table ORDER BY `rel.timestamp`",
        "<rel.type>",
        "<source.labels>",
        "source.age",
        "<target.labels>",
        "target.hash",
        "rel.id"
      ).asJava)
        .containsExactlyElementsOf(expected.asJava)
    }

    @Test
    def reads_query_results_from_now(): Unit = {
      createPersonNodes(0, 1)

      query = spark.readStream.format(dataSourceFormat)
        .option(OptFrom, "NOW")
        .option(OptPropertyName, "timestamp")
        .option("query", personStreamingQuery)
        .option(OptQueryOffset, "MATCH (p:Person) RETURN max(p.timestamp)")
        .load()
        .writeStream
        .format("memory")
        .queryName("queryFromNow")
        .start()

      Executors.newSingleThreadExecutor().execute(() => createPersonNodes(1, total, 1000, 200))

      val expected = (1 to total).map(index => Map("age" -> s"$index")).toList
      Assert.assertEventually(
        expected,
        () => select("SELECT * FROM queryFromNow ORDER BY timestamp", "age"),
        30L,
        TimeUnit.SECONDS
      )
    }

    @Test
    def reads_all_query_results(): Unit = {
      createPersonNodes(0, 1)

      query = spark.readStream.format(dataSourceFormat)
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "ALL")
        .option("query", personStreamingQuery)
        .option(OptQueryOffset, "MATCH (p:Person) RETURN max(p.timestamp)")
        .load()
        .writeStream
        .format("memory")
        .queryName("allQuery")
        .start()

      Executors.newSingleThreadExecutor().execute(() => createPersonNodes(1, total, 1000, 200))

      val expected = (0 to total).map(index => Map("age" -> s"$index")).toList
      Assert.assertEventually(
        expected,
        () => select("SELECT * FROM allQuery ORDER BY timestamp", "age"),
        30L,
        TimeUnit.SECONDS
      )
    }

    @Test
    def resumes_reading_query_results_from_checkpoint(): Unit = {
      createPersonNodes(0, 1)

      val stream = spark.readStream.format(dataSourceFormat)
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "ALL")
        .option("query", personStreamingQuery)
        .option(OptQueryOffset, "MATCH (p:Person) RETURN max(p.timestamp)")
        .load()

      val location = checkpoint()
      val table = uniqueTable("queryCheckpoint")
      val partial = total / 2

      drainToTable(stream, location, table)
      createPersonNodes(1, partial, 0, 10)
      drainToTable(stream, location, table)
      createPersonNodes(partial + 1, total - partial, 0, 10)
      drainToTable(stream, location, table)

      val expected = (0 to total).map(index => Map("age" -> s"$index")).toList
      assertThat(select(s"SELECT * FROM $table ORDER BY timestamp", "age").asJava)
        .containsExactlyElementsOf(expected.asJava)
    }

    @Test
    def keeps_offset_when_query_returns_nothing(): Unit = {
      createPersonNodes(0, 50)
      driver.executableQuery("MATCH (p:Person) SET p:Human").execute()

      val stream = spark.readStream.format(dataSourceFormat)
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "ALL")
        .option("query", personStreamingQuery)
        .option(OptQueryOffset, "MATCH (p:Human) RETURN max(p.timestamp)")
        .load()

      val location = checkpoint()
      val table = uniqueTable("queryReturnsNothing")

      // 1st trigger: every node is processed
      var streamTable = stream.writeStream
        .trigger(Trigger.AvailableNow())
        .option("checkpointLocation", location)
        .toTable(table)
      streamTable.awaitTermination()
      val firstSource = streamTable.lastProgress.sources.head
      assertThat(firstSource.numInputRows).isEqualTo(50L)
      val endOffset = firstSource.endOffset

      driver.executableQuery("MATCH (p:Human) REMOVE p:Human").execute()

      // 2nd trigger: previous offset is kept intact
      streamTable = stream.writeStream
        .trigger(Trigger.AvailableNow())
        .option("checkpointLocation", location)
        .toTable(table)
      streamTable.awaitTermination()
      val source = streamTable.lastProgress.sources.head
      assertThat(source.startOffset).isEqualTo(endOffset)
      assertThat(source.endOffset).isEqualTo(endOffset)
      assertThat(source.numInputRows).isEqualTo(0L)
    }

    private val personStreamingQuery =
      """
        |MATCH (p:Person)
        |WHERE p.timestamp > $stream.from AND p.timestamp <= $stream.to
        |RETURN p.age AS age, p.timestamp AS timestamp
        |""".stripMargin

    private def drainToTable(stream: DataFrame, location: String, table: String): Unit = {
      stream.writeStream
        .trigger(Trigger.AvailableNow())
        .option("checkpointLocation", location)
        .toTable(table)
        .awaitTermination()
    }

    private def createMovieNodes(from: Int, count: Int, delayMs: Int = 0, intervalMs: Int = 0): Unit = {
      Thread.sleep(delayMs)
      (from until from + count).foreach { index =>
        Thread.sleep(intervalMs)
        driver.executableQuery(s"CREATE (:Movie {title: 'My movie $index', timestamp: timestamp()})").execute()
      }
    }

    private def createPersonNodes(from: Int, count: Int, delayMs: Int = 0, intervalMs: Int = 0): Unit = {
      Thread.sleep(delayMs)
      (from until from + count).foreach { index =>
        Thread.sleep(intervalMs)
        driver.executableQuery(s"CREATE (:Person {age: '$index', timestamp: timestamp()})").execute()
      }
    }

    private def createLikesRelationships(from: Int, count: Int, delayMs: Int = 0, intervalMs: Int = 0): Unit = {
      Thread.sleep(delayMs)
      (from until from + count).foreach { index =>
        Thread.sleep(intervalMs)
        driver.executableQuery(
          s"""
             |CREATE (person:Person {age: $index})
             |CREATE (post:Post {hash: 'hash$index'})
             |CREATE (person)-[:LIKES {id: $index, timestamp: timestamp()}]->(post)
             |""".stripMargin
        ).execute()
      }
    }

    private def likeRow(index: Int): Map[String, Any] = Map(
      "<rel.type>" -> "LIKES",
      "<source.labels>" -> List("Person"),
      "source.age" -> index,
      "<target.labels>" -> List("Post"),
      "target.hash" -> s"hash$index",
      "rel.id" -> index
    )

    private def select(sql: String, columns: String*): immutable.Seq[Map[String, Any]] =
      spark.sql(sql).collect().map(row => columns.map(column => column -> row.getAs[Any](column)).toMap).toList
  }

  @Nested
  @DisplayName("to sink")
  class ToSink {

    private val recordSize = 2000
    private val partitions = 5

    @Test
    def writes_nodes_in_append_mode(): Unit = {
      val stream = memoryStream()
      query = stream.toDF().writeStream
        .format(dataSourceFormat)
        .option("save.mode", "Append")
        .option("labels", "Timestamp")
        .option("node.keys", "value")
        .option("checkpointLocation", checkpoint())
        .start()

      feed(stream)((1 to recordSize * partitions).toArray)

      assertEventuallyContainsValues("Timestamp", "value", (1 to recordSize * partitions).toList)
    }

    @Test
    def writes_nodes_in_overwrite_mode(): Unit = {
      driver.executableQuery(
        "CREATE CONSTRAINT timestamp_value FOR (t:Timestamp) REQUIRE t.value IS UNIQUE"
      ).execute()

      val stream = memoryStream()
      query = stream.toDF().writeStream
        .format(dataSourceFormat)
        .option("save.mode", "Overwrite")
        .option("labels", "Timestamp")
        .option("node.keys", "value")
        .option("checkpointLocation", checkpoint())
        .start()

      (1 to partitions).foreach(_ => stream.addData((1 to 500).toArray))

      assertEventuallyContainsValues("Timestamp", "value", (1 to 500).toList)
    }

    @Test
    def writes_relationships_in_append_mode(): Unit = {
      val stream = memoryStream()
      query = stream.toDF().writeStream
        .format(dataSourceFormat)
        .option("save.mode", "Append")
        .option("relationship", "PAIRS")
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.labels", ":From")
        .option("relationship.source.node.keys", "value")
        .option("relationship.source.save.mode", "Append")
        .option("relationship.target.labels", ":To")
        .option("relationship.target.node.keys", "value")
        .option("relationship.target.save.mode", "Append")
        .option("checkpointLocation", checkpoint())
        .start()

      feed(stream)((1 to recordSize * partitions).toArray)

      val expected = (1 to recordSize * partitions).map(value => (value, value)).toList
      assertEventuallyContainsPairs(expected)
    }

    @Test
    def appends_relationships_while_overwriting_nodes(): Unit = {
      driver.executableQuery("CREATE CONSTRAINT From_value FOR (p:From) REQUIRE p.value IS UNIQUE").execute()
      driver.executableQuery("CREATE CONSTRAINT To_value FOR (p:To) REQUIRE p.value IS UNIQUE").execute()

      val stream = memoryStream()
      query = stream.toDF().writeStream
        .format(dataSourceFormat)
        .option("save.mode", "Append")
        .option("relationship", "PAIRS")
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.labels", ":From")
        .option("relationship.source.node.keys", "value")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.target.labels", ":To")
        .option("relationship.target.node.keys", "value")
        .option("relationship.target.save.mode", "Overwrite")
        .option("checkpointLocation", checkpoint())
        .start()

      (1 to partitions).foreach(_ => stream.addData((1 to 500).toArray))

      val expected = (1 to 500).flatMap(value => (1 to partitions).map(_ => (value, value))).toList
      assertEventuallyContainsPairs(expected)
    }

    @Test
    def writes_with_query(): Unit = {
      val stream = memoryStream()
      query = stream.toDF().writeStream
        .format(dataSourceFormat)
        .option("query", "MERGE (m:MyNewNode {the_value: event.value})")
        .option("checkpointLocation", checkpoint())
        .start()

      feed(stream)((1 to recordSize * partitions).toArray)

      assertEventuallyContainsValues("MyNewNode", "the_value", (1 to recordSize * partitions).toList)
    }

    private def memoryStream(): MemoryStream[Int] = {
      val session = spark
      import session.implicits._
      MemoryStream[Int](session)
    }

    private def feed(stream: MemoryStream[Int])(values: Array[Int]): Unit = {
      values.grouped(values.length / partitions).foreach(stream.addData(_))
    }

    private def assertEventuallyContainsValues(label: String, column: String, expected: immutable.Seq[Int]): Unit = {
      Assert.assertEventually(
        expected.size,
        () => readValues(label, column).size,
        30L,
        TimeUnit.SECONDS
      )
      assertThat(readValues(label, column).map(Int.box).asJava)
        .containsExactlyInAnyOrderElementsOf(expected.map(Int.box).asJava)
    }

    private def assertEventuallyContainsPairs(expected: immutable.Seq[(Int, Int)]): Unit = {
      Assert.assertEventually(
        expected.size,
        () => readPairs().size,
        30L,
        TimeUnit.SECONDS
      )
      assertThat(readPairs().asJava).containsExactlyInAnyOrderElementsOf(expected.asJava)
    }

    private def readValues(label: String, column: String): immutable.Seq[Int] = {
      val df = spark.read.format(dataSourceFormat).option("labels", label).load()
      if (df.columns.contains(column)) df.collect().map(_.getAs[Long](column).toInt).toList
      else immutable.Seq.empty
    }

    private def readPairs(): immutable.Seq[(Int, Int)] = {
      val df: DataFrame = spark.read.format(dataSourceFormat)
        .option("relationship", "PAIRS")
        .option("relationship.source.labels", ":From")
        .option("relationship.target.labels", ":To")
        .load()
      if (df.columns.contains("source.value") && df.columns.contains("target.value")) {
        df.collect()
          .map(row => (row.getAs[Long]("source.value").toInt, row.getAs[Long]("target.value").toInt))
          .toList
      } else {
        immutable.Seq.empty
      }
    }
  }

  private def checkpoint(): String =
    Files.createTempDirectory(folder, "checkpoint").toAbsolutePath.toString

  private def uniqueTable(prefix: String): String = {
    val table = s"${prefix}_${java.util.UUID.randomUUID().toString.replace("-", "")}"
    createdTables += table
    table
  }
}
