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
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.junit.jupiter.params.Parameter
import org.neo4j.caniuse.Neo4j
import org.neo4j.driver.Driver
import org.neo4j.spark.testsupport.Assert
import org.neo4j.spark.testsupport.InjectNeo4jContainerParameter
import org.neo4j.spark.testsupport.StreamingTestState
import org.testcontainers.neo4j.Neo4jContainer

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import scala.collection.immutable
import scala.collection.mutable
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

@InjectNeo4jContainerParameter
@DisplayName("streaming")
class StreamingIT {

  @Parameter
  var neo4jContainer: Neo4jContainer = _

  @TempDir
  var folder: Path = _

  private def query: StreamingQuery = StreamingTestState.current.query

  private def query_=(value: StreamingQuery): Unit = {
    StreamingTestState.current.query = value
  }

  private def createdTables: mutable.ListBuffer[String] = StreamingTestState.current.createdTables

  private val dataSourceFormat = classOf[DataSource].getName

  private val OptPropertyName = "streaming.property.name"
  private val OptFrom = "streaming.from"
  private val OptQueryOffset = "streaming.query.offset"

  private def connectionOptions(spark: SparkSession): java.util.Map[String, String] =
    Map(
      "url" -> spark.conf.get("neo4j.url"),
      "authentication.basic.username" -> spark.conf.get("neo4j.authentication.basic.username"),
      "authentication.basic.password" -> spark.conf.get("neo4j.authentication.basic.password")
    ).asJava

  @BeforeEach
  def prepare(): Unit = {
    StreamingTestState.set()
  }

  @AfterEach
  def cleanUp(spark: SparkSession): Unit = {
    Option(query).foreach(_.stop())
    Option(spark).foreach { session =>
      createdTables.foreach(table => session.sql(s"DROP TABLE IF EXISTS $table"))
      createdTables.clear()
    }
    StreamingTestState.clear()
  }

  @Nested
  @DisplayName("from source")
  @Execution(ExecutionMode.SAME_THREAD)
  class FromSource {

    private val total = 20

    @Test
    def reads_nodes_from_now(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      createMovieNodes(driver, 0, 1)

      query = spark.readStream.format(dataSourceFormat)
        .options(connectionOptions(spark))
        .option("labels", "Movie")
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "NOW")
        .load()
        .writeStream
        .format("memory")
        .queryName("nodesFromNow")
        .start()

      Executors.newSingleThreadExecutor().execute(() => createMovieNodes(driver, 1, total, 1000, 25))

      val expected = (1 to total)
        .map(index => Map("<labels>" -> List("Movie"), "title" -> s"My movie $index"))
        .toList

      Assert.assertEventually(
        expected,
        () => select(spark, "SELECT * FROM nodesFromNow ORDER BY timestamp", "<labels>", "title"),
        30L,
        TimeUnit.SECONDS
      )
    }

    @Test
    def reads_all_nodes(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      createMovieNodes(driver, 0, 1)

      query = spark.readStream.format(dataSourceFormat)
        .options(connectionOptions(spark))
        .option("labels", "Movie")
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "ALL")
        .load()
        .writeStream
        .format("memory")
        .queryName("allNodes")
        .start()

      Executors.newSingleThreadExecutor().execute(() => createMovieNodes(driver, 1, total, 1000, 25))

      val expected = (0 to total)
        .map(index => Map("<labels>" -> List("Movie"), "title" -> s"My movie $index"))
        .toList

      Assert.assertEventually(
        expected,
        () => select(spark, "SELECT * FROM allNodes ORDER BY timestamp", "<labels>", "title"),
        30L,
        TimeUnit.SECONDS
      )
    }

    @Test
    def resumes_reading_nodes_from_checkpoint(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      createMovieNodes(driver, 0, 1)

      val stream = spark.readStream.format(dataSourceFormat)
        .options(connectionOptions(spark))
        .option("labels", "Movie")
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "NOW")
        .load()

      val location = checkpoint()
      val table = uniqueTable("nodesCheckpoint")
      val partial = total / 2

      drainToTable(stream, location, table)
      createMovieNodes(driver, 1, partial, 0, 10)
      drainToTable(stream, location, table)
      createMovieNodes(driver, partial + 1, total - partial, 0, 10)
      drainToTable(stream, location, table)

      val expected = (1 to total)
        .map(index => Map("<labels>" -> List("Movie"), "title" -> s"My movie $index"))
        .toList
      assertThat(select(spark, s"SELECT * FROM $table ORDER BY timestamp", "<labels>", "title").asJava)
        .containsExactlyElementsOf(expected.asJava)
    }

    @Test
    def reads_relationships_from_now(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      createLikesRelationships(driver, 0, 1)

      query = spark.readStream.format(dataSourceFormat)
        .options(connectionOptions(spark))
        .option("relationship", "LIKES")
        .option("relationship.save.strategy", "native")
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "NOW")
        .option("relationship.source.labels", "Person")
        .option("relationship.target.labels", "Post")
        .load()
        .writeStream
        .format("memory")
        .queryName("relationshipsFromNow")
        .start()

      Executors.newSingleThreadExecutor().execute(() => createLikesRelationships(driver, 1, total, 1000, 25))

      val expected = (1 to total).map(likeRow).toList
      Assert.assertEventually(
        expected,
        () =>
          select(
            spark,
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
    def reads_all_relationships(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      createLikesRelationships(driver, 0, 1)

      query = spark.readStream.format(dataSourceFormat)
        .options(connectionOptions(spark))
        .option("relationship", "LIKES")
        .option("relationship.save.strategy", "native")
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "ALL")
        .option("relationship.source.labels", "Person")
        .option("relationship.target.labels", "Post")
        .load()
        .writeStream
        .format("memory")
        .queryName("allRelationships")
        .start()

      Executors.newSingleThreadExecutor().execute(() => createLikesRelationships(driver, 1, total, 1000, 25))

      val expected = (0 to total).map(likeRow).toList
      Assert.assertEventually(
        expected,
        () =>
          select(
            spark,
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
    def resumes_reading_relationships_from_checkpoint(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      createLikesRelationships(driver, 0, 1)

      val stream = spark.readStream.format(dataSourceFormat)
        .options(connectionOptions(spark))
        .option("relationship", "LIKES")
        .option("relationship.save.strategy", "native")
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "ALL")
        .option("relationship.source.labels", "Person")
        .option("relationship.target.labels", "Post")
        .load()

      val location = checkpoint()
      val table = uniqueTable("relationshipsCheckpoint")
      val partial = total / 2

      drainToTable(stream, location, table)
      createLikesRelationships(driver, 1, partial, 0, 10)
      drainToTable(stream, location, table)
      createLikesRelationships(driver, partial + 1, total - partial, 0, 10)
      drainToTable(stream, location, table)

      val expected = (0 to total).map(likeRow).toList
      assertThat(select(
        spark,
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
    def reads_query_results_from_now(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      createPersonNodes(driver, 0, 1)

      query = spark.readStream.format(dataSourceFormat)
        .options(connectionOptions(spark))
        .option(OptFrom, "NOW")
        .option(OptPropertyName, "timestamp")
        .option("query", personStreamingQuery)
        .option(OptQueryOffset, "MATCH (p:Person) RETURN max(p.timestamp)")
        .load()
        .writeStream
        .format("memory")
        .queryName("queryFromNow")
        .start()

      Executors.newSingleThreadExecutor().execute(() => createPersonNodes(driver, 1, total, 1000, 25))

      val expected = (1 to total).map(index => Map("age" -> s"$index")).toList
      Assert.assertEventually(
        expected,
        () => select(spark, "SELECT * FROM queryFromNow ORDER BY timestamp", "age"),
        30L,
        TimeUnit.SECONDS
      )
    }

    @Test
    def reads_all_query_results(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      createPersonNodes(driver, 0, 1)

      query = spark.readStream.format(dataSourceFormat)
        .options(connectionOptions(spark))
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "ALL")
        .option("query", personStreamingQuery)
        .option(OptQueryOffset, "MATCH (p:Person) RETURN max(p.timestamp)")
        .load()
        .writeStream
        .format("memory")
        .queryName("allQuery")
        .start()

      Executors.newSingleThreadExecutor().execute(() => createPersonNodes(driver, 1, total, 1000, 25))

      val expected = (0 to total).map(index => Map("age" -> s"$index")).toList
      Assert.assertEventually(
        expected,
        () => select(spark, "SELECT * FROM allQuery ORDER BY timestamp", "age"),
        30L,
        TimeUnit.SECONDS
      )
    }

    @Test
    def resumes_reading_query_results_from_checkpoint(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      createPersonNodes(driver, 0, 1)

      val stream = spark.readStream.format(dataSourceFormat)
        .options(connectionOptions(spark))
        .option(OptPropertyName, "timestamp")
        .option(OptFrom, "ALL")
        .option("query", personStreamingQuery)
        .option(OptQueryOffset, "MATCH (p:Person) RETURN max(p.timestamp)")
        .load()

      val location = checkpoint()
      val table = uniqueTable("queryCheckpoint")
      val partial = total / 2

      drainToTable(stream, location, table)
      createPersonNodes(driver, 1, partial, 0, 10)
      drainToTable(stream, location, table)
      createPersonNodes(driver, partial + 1, total - partial, 0, 10)
      drainToTable(stream, location, table)

      val expected = (0 to total).map(index => Map("age" -> s"$index")).toList
      assertThat(select(spark, s"SELECT * FROM $table ORDER BY timestamp", "age").asJava)
        .containsExactlyElementsOf(expected.asJava)
    }

    @Test
    def keeps_offset_when_query_returns_nothing(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      createPersonNodes(driver, 0, 50)
      driver.executableQuery("MATCH (p:Person) SET p:Human").execute()

      val stream = spark.readStream.format(dataSourceFormat)
        .options(connectionOptions(spark))
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

    private def createMovieNodes(
      driver: Driver,
      from: Int,
      count: Int,
      delayMs: Int = 0,
      intervalMs: Int = 0
    ): Unit = {
      Thread.sleep(delayMs)
      (from until from + count).foreach { index =>
        Thread.sleep(intervalMs)
        driver.executableQuery(s"CREATE (:Movie {title: 'My movie $index', timestamp: timestamp()})").execute()
      }
    }

    private def createPersonNodes(
      driver: Driver,
      from: Int,
      count: Int,
      delayMs: Int = 0,
      intervalMs: Int = 0
    ): Unit = {
      Thread.sleep(delayMs)
      (from until from + count).foreach { index =>
        Thread.sleep(intervalMs)
        driver.executableQuery(s"CREATE (:Person {age: '$index', timestamp: timestamp()})").execute()
      }
    }

    private def createLikesRelationships(
      driver: Driver,
      from: Int,
      count: Int,
      delayMs: Int = 0,
      intervalMs: Int = 0
    ): Unit = {
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

    private def select(spark: SparkSession, sql: String, columns: String*): immutable.Seq[Map[String, Any]] =
      spark.sql(sql).collect().map(row => columns.map(column => column -> row.getAs[Any](column)).toMap).toList
  }

  @Nested
  @DisplayName("to sink")
  @Execution(ExecutionMode.SAME_THREAD)
  class ToSink {

    private val recordSize = 2000
    private val partitions = 5

    @Test
    def writes_nodes_in_append_mode(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      val stream = memoryStream(spark)
      query = stream.toDF().writeStream
        .format(dataSourceFormat)
        .options(connectionOptions(spark))
        .option("save.mode", "Append")
        .option("labels", "Timestamp")
        .option("node.keys", "value")
        .option("checkpointLocation", checkpoint())
        .start()

      feed(stream)((1 to recordSize * partitions).toArray)

      assertEventuallyContainsValues(spark, "Timestamp", "value", (1 to recordSize * partitions).toList)
    }

    @Test
    def writes_nodes_in_overwrite_mode(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      driver.executableQuery(
        "CREATE CONSTRAINT timestamp_value FOR (t:Timestamp) REQUIRE t.value IS UNIQUE"
      ).execute()

      val stream = memoryStream(spark)
      query = stream.toDF().writeStream
        .format(dataSourceFormat)
        .options(connectionOptions(spark))
        .option("save.mode", "Overwrite")
        .option("labels", "Timestamp")
        .option("node.keys", "value")
        .option("checkpointLocation", checkpoint())
        .start()

      (1 to partitions).foreach(_ => stream.addData((1 to 500).toArray))

      assertEventuallyContainsValues(spark, "Timestamp", "value", (1 to 500).toList)
    }

    @Test
    def writes_relationships_in_append_mode(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      val stream = memoryStream(spark)
      query = stream.toDF().writeStream
        .format(dataSourceFormat)
        .options(connectionOptions(spark))
        .option("save.mode", "Append")
        .option("relationship", "PAIRS")
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
      assertEventuallyContainsPairs(spark, expected)
    }

    @Test
    def appends_relationships_while_overwriting_nodes(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      driver.executableQuery("CREATE CONSTRAINT From_value FOR (p:From) REQUIRE p.value IS UNIQUE").execute()
      driver.executableQuery("CREATE CONSTRAINT To_value FOR (p:To) REQUIRE p.value IS UNIQUE").execute()

      val stream = memoryStream(spark)
      query = stream.toDF().writeStream
        .format(dataSourceFormat)
        .options(connectionOptions(spark))
        .option("save.mode", "Append")
        .option("relationship", "PAIRS")
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
      assertEventuallyContainsPairs(spark, expected)
    }

    @Test
    def writes_with_query(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
      val stream = memoryStream(spark)
      query = stream.toDF().writeStream
        .format(dataSourceFormat)
        .options(connectionOptions(spark))
        .option("query", "MERGE (m:MyNewNode {the_value: event.value})")
        .option("checkpointLocation", checkpoint())
        .start()

      feed(stream)((1 to recordSize * partitions).toArray)

      assertEventuallyContainsValues(spark, "MyNewNode", "the_value", (1 to recordSize * partitions).toList)
    }

    private def memoryStream(spark: SparkSession): MemoryStream[Int] = {
      val session = spark
      import session.implicits._
      MemoryStream[Int](session)
    }

    private def feed(stream: MemoryStream[Int])(values: Array[Int]): Unit = {
      values.grouped(values.length / partitions).foreach(stream.addData(_))
    }

    private def assertEventuallyContainsValues(
      spark: SparkSession,
      label: String,
      column: String,
      expected: immutable.Seq[Int]
    ): Unit = {
      assertEventuallyWithQueryDiagnostics(expected.size, () => readValues(spark, label, column).size)
      assertThat(readValues(spark, label, column).map(Int.box).asJava)
        .containsExactlyInAnyOrderElementsOf(expected.map(Int.box).asJava)
    }

    private def assertEventuallyContainsPairs(spark: SparkSession, expected: immutable.Seq[(Int, Int)]): Unit = {
      assertEventuallyWithQueryDiagnostics(expected.size, () => readPairs(spark).size)
      assertThat(readPairs(spark).asJava).containsExactlyInAnyOrderElementsOf(expected.asJava)
    }

    private def assertEventuallyWithQueryDiagnostics(expected: Int, actual: () => Int): Unit = {
      try {
        Assert.assertEventually(
          expected,
          new Assert.ThrowingSupplier[Int, RuntimeException] {
            override def get(): Int = actual()
          },
          30L,
          TimeUnit.SECONDS
        )
      } catch {
        case error: AssertionError =>
          throw new AssertionError(s"${error.getMessage}\n${queryDiagnostics}", error)
      }
    }

    private def queryDiagnostics: String =
      Option(query)
        .map { streamingQuery =>
          val progress = streamingQuery.recentProgress.map(_.json).mkString("[", ",", "]")
          s"""Streaming query diagnostics:
             |active=${streamingQuery.isActive}
             |status=${streamingQuery.status}
             |exception=${Option(streamingQuery.exception).map(_.toString).getOrElse("<none>")}
             |recentProgress=$progress
             |""".stripMargin
        }
        .getOrElse("Streaming query diagnostics: <no query>")

    private def readValues(spark: SparkSession, label: String, column: String): immutable.Seq[Int] = {
      val df = spark.read.format(dataSourceFormat).options(connectionOptions(spark)).option("labels", label).load()
      if (df.columns.contains(column)) df.collect().map(_.getAs[Long](column).toInt).toList
      else immutable.Seq.empty
    }

    private def readPairs(spark: SparkSession): immutable.Seq[(Int, Int)] = {
      val df: DataFrame = spark.read.format(dataSourceFormat)
        .options(connectionOptions(spark))
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
