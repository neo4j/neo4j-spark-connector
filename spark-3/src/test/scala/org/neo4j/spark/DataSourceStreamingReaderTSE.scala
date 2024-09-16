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
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger
import org.hamcrest.Matchers
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.neo4j.Closeables.use

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import scala.annotation.meta.getter

class DataSourceStreamingReaderTSE extends SparkConnectorScalaBaseTSE {

  @(Rule @getter)
  val folder: TemporaryFolder = new TemporaryFolder()

  private var query: StreamingQuery = _

  @After
  def close(): Unit = {
    if (query != null) {
      query.stop()
    }
  }

  @Test
  def testReadStreamWithLabels(): Unit = {
    createMovieNodes(0, 1)

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Movie")
      .option("streaming.property.name", "timestamp")
      .option("streaming.from", "NOW")
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("readStreamWithLabels")
      .start()

    val total = 60
    val expected: Seq[Map[String, Any]] = (1 to total).map(index =>
      Map(
        "<labels>" -> Seq("Movie"),
        "title" -> s"My movie $index"
      )
    )

    // Continue creating nodes in the background
    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        createMovieNodes(1, total, 1000, 200)
      }
    })

    Assert.assertEventually(
      new Assert.ThrowingSupplier[Seq[Map[String, Any]], Exception] {
        override def get(): Seq[Map[String, Any]] = {
          selectRowsFromTable("select * from readStreamWithLabels order by timestamp", mapMovie)
        }
      },
      Matchers.equalTo(expected),
      30L,
      TimeUnit.SECONDS
    )
  }

  @Test
  def testReadStreamWithLabelsGetAll(): Unit = {
    createMovieNodes(0, 1)

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Movie")
      .option("streaming.property.name", "timestamp")
      .option("streaming.from", "ALL")
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("readStreamWithLabelsAll")
      .start()

    val total = 60
    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        createMovieNodes(1, total, 1000, 200)
      }
    })

    val expected: Seq[Map[String, Any]] = (0 to total).map(index =>
      Map(
        "<labels>" -> Seq("Movie"),
        "title" -> s"My movie $index"
      )
    )

    Assert.assertEventually(
      new Assert.ThrowingSupplier[Seq[Map[String, Any]], Exception] {
        override def get(): Seq[Map[String, Any]] = {
          selectRowsFromTable("select * from readStreamWithLabelsAll order by timestamp", mapMovie)
        }
      },
      Matchers.equalTo(expected),
      30L,
      TimeUnit.SECONDS
    )
  }

  @Test
  def testReadStreamWithLabelsResumesFromCheckpoint(): Unit = {
    createMovieNodes(0, 1)

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Movie")
      .option("streaming.property.name", "timestamp")
      .option("streaming.from", "NOW")
      .load()

    val total = 60
    val expected: Seq[Map[String, Any]] = (1 to total).map(index =>
      Map(
        "<labels>" -> List("Movie"),
        "title" -> s"My movie $index"
      )
    )

    val checkpoint = folder.newFolder()

    stream.writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpoint.getAbsolutePath)
      .toTable("readStreamWithLabelsCheckpoint")
      .awaitTermination()

    val partial: Int = total / 2
    // create partial movies starting from 1
    createMovieNodes(1, partial, 0, 10)

    // fetch whatever is available
    stream.writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpoint.getAbsolutePath)
      .toTable("readStreamWithLabelsCheckpoint")
      .awaitTermination()

    // create rest of the movies starting from partial+1
    createMovieNodes(partial + 1, total - partial, 0, 10)

    // fetch rest of the items from where we left off
    stream.writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpoint.getAbsolutePath)
      .toTable("readStreamWithLabelsCheckpoint")
      .awaitTermination()

    assertEquals(
      expected,
      selectRowsFromTable("select * from readStreamWithLabelsCheckpoint order by timestamp", mapMovie)
    )
  }

  @Test
  def testReadStreamWithRelationship(): Unit = {
    createLikesRelationships(0, 1)

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "LIKES")
      .option("streaming.property.name", "timestamp")
      .option("streaming.from", "NOW")
      .option("relationship.source.labels", "Person")
      .option("relationship.target.labels", "Post")
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("readStreamWithRelationship")
      .start()

    val total = 60

    val expected: Seq[Map[String, Any]] = (1 to total).map(index =>
      Map(
        "<rel.type>" -> "LIKES",
        "<source.labels>" -> Seq("Person"),
        "source.age" -> index,
        "<target.labels>" -> Seq("Post"),
        "target.hash" -> s"hash$index",
        "rel.id" -> index
      )
    )

    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        createLikesRelationships(1, total, 1000, 200)
      }
    })

    Assert.assertEventually(
      new Assert.ThrowingSupplier[Seq[Map[String, Any]], Exception] {
        override def get(): Seq[Map[String, Any]] = {
          selectRowsFromTable("select * from readStreamWithRelationship order by `rel.timestamp`", mapLikes)
        }
      },
      Matchers.equalTo(expected),
      30L,
      TimeUnit.SECONDS
    )
  }

  @Test
  def testReadStreamWithRelationshipGetAll(): Unit = {
    createLikesRelationships(0, 1)

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "LIKES")
      .option("streaming.property.name", "timestamp")
      .option("streaming.from", "ALL")
      .option("relationship.source.labels", "Person")
      .option("relationship.target.labels", "Post")
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("readStreamWithRelationshipAll")
      .start()

    val total = 60
    val expected: Seq[Map[String, Any]] = (0 to total).map(index =>
      Map(
        "<rel.type>" -> "LIKES",
        "<source.labels>" -> Seq("Person"),
        "source.age" -> index,
        "<target.labels>" -> Seq("Post"),
        "target.hash" -> s"hash$index",
        "rel.id" -> index
      )
    )

    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        createLikesRelationships(1, total, 1000, 200)
      }
    })

    Assert.assertEventually(
      new Assert.ThrowingSupplier[Seq[Map[String, Any]], Exception] {
        override def get(): Seq[Map[String, Any]] = {
          selectRowsFromTable("select * from readStreamWithRelationshipAll order by `rel.timestamp`", mapLikes)
        }
      },
      Matchers.equalTo(expected),
      30L,
      TimeUnit.SECONDS
    )
  }

  @Test
  def testReadStreamWithRelationshipResumesFromCheckpoint(): Unit = {
    createLikesRelationships(0, 1)

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "LIKES")
      .option("streaming.property.name", "timestamp")
      .option("streaming.from", "ALL")
      .option("relationship.source.labels", "Person")
      .option("relationship.target.labels", "Post")
      .load()

    val total = 60
    val expected: Seq[Map[String, Any]] = (0 to total).map(index =>
      Map(
        "<rel.type>" -> "LIKES",
        "<source.labels>" -> Seq("Person"),
        "source.age" -> index,
        "<target.labels>" -> Seq("Post"),
        "target.hash" -> s"hash$index",
        "rel.id" -> index
      )
    )

    val checkpoint = folder.newFolder()

    stream.writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpoint.getAbsolutePath)
      .toTable("readStreamWithRelationshipCheckpoint")
      .awaitTermination()

    val partial: Int = total / 2
    // create partial number of likes starting from 1
    createLikesRelationships(1, partial, 0, 10)

    // fetch whatever is available
    stream.writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpoint.getAbsolutePath)
      .toTable("readStreamWithRelationshipCheckpoint")
      .awaitTermination()

    // create rest of the likes starting from partial+1
    createLikesRelationships(partial + 1, total - partial, 0, 10)

    // fetch rest of the items from where we left off
    stream.writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpoint.getAbsolutePath)
      .toTable("readStreamWithRelationshipCheckpoint")
      .awaitTermination()

    assertEquals(
      expected,
      selectRowsFromTable("select * from readStreamWithRelationshipCheckpoint order by `rel.timestamp`", mapLikes)
    )
  }

  @Test
  def testReadStreamWithQuery(): Unit = {
    createPersonNodes(0, 1)

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("streaming.from", "NOW")
      .option("streaming.property.name", "timestamp")
      .option(
        "query",
        """
          |MATCH (p:Person)
          |WHERE p.timestamp > $stream.offset
          |RETURN p.age AS age, p.timestamp AS timestamp
          |""".stripMargin
      )
      .option(
        "streaming.query.offset",
        """
          |MATCH (p:Person)
          |RETURN max(p.timestamp)
          |""".stripMargin
      )
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("readStreamWithQuery")
      .start()

    val total = 60
    val expected: Seq[Map[String, Any]] = (1 to total).map(index =>
      Map(
        "age" -> s"$index"
      )
    )

    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        createPersonNodes(1, total, 1000, 200)
      }
    })

    Assert.assertEventually(
      new Assert.ThrowingSupplier[Seq[Map[String, Any]], Exception] {
        override def get(): Seq[Map[String, Any]] = {
          selectRowsFromTable("select * from readStreamWithQuery order by timestamp", mapPerson)
        }
      },
      Matchers.equalTo(expected),
      30L,
      TimeUnit.SECONDS
    )
  }

  @Test
  def testReadStreamWithQueryGetAll(): Unit = {
    createPersonNodes(0, 1)

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("streaming.property.name", "timestamp")
      .option("streaming.from", "ALL")
      .option(
        "query",
        """
          |MATCH (p:Person)
          |WHERE p.timestamp > $stream.offset
          |RETURN p.age AS age, p.timestamp AS timestamp
          |""".stripMargin
      )
      .option(
        "streaming.query.offset",
        """
          |MATCH (p:Person)
          |RETURN max(p.timestamp)
          |""".stripMargin
      )
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("readStreamWithQueryAll")
      .start()

    val total = 60
    val expected: Seq[Map[String, Any]] = (0 to total).map(index =>
      Map(
        "age" -> s"$index"
      )
    ).toList

    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        createPersonNodes(1, total, 1000, 200)
      }
    })

    Assert.assertEventually(
      new Assert.ThrowingSupplier[Seq[Map[String, Any]], Exception] {
        override def get(): Seq[Map[String, Any]] = {
          selectRowsFromTable("select * from readStreamWithQueryAll order by timestamp", mapPerson)
        }
      },
      Matchers.equalTo(expected),
      30L,
      TimeUnit.SECONDS
    )
  }

  @Test
  def testReadStreamWithQueryResumesFromCheckpoint(): Unit = {
    createPersonNodes(0, 1)

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("streaming.property.name", "timestamp")
      .option("streaming.from", "ALL")
      .option(
        "query",
        """
          |MATCH (p:Person)
          |WHERE p.timestamp > $stream.offset
          |RETURN p.age AS age, p.timestamp AS timestamp
          |""".stripMargin
      )
      .option(
        "streaming.query.offset",
        """
          |MATCH (p:Person)
          |RETURN max(p.timestamp)
          |""".stripMargin
      )
      .load()

    val total = 60
    val expected: Seq[Map[String, Any]] = (0 to total).map(index =>
      Map(
        "age" -> s"$index"
      )
    ).toList

    val checkpoint = folder.newFolder()

    stream.writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpoint.getAbsolutePath)
      .toTable("readStreamWithQueryCheckpoint")
      .awaitTermination()

    val partial: Int = total / 2
    // create partial number of persons starting from 1
    createPersonNodes(1, partial, 0, 10)

    // fetch whatever is available
    stream.writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpoint.getAbsolutePath)
      .toTable("readStreamWithQueryCheckpoint")
      .awaitTermination()

    // create rest of the persons starting from partial+1
    createPersonNodes(partial + 1, total - partial, 0, 10)

    // fetch rest of the items from where we left off
    stream.writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpoint.getAbsolutePath)
      .toTable("readStreamWithQueryCheckpoint")
      .awaitTermination()

    assertEquals(
      expected,
      selectRowsFromTable("select * from readStreamWithQueryCheckpoint order by timestamp", mapPerson)
    )
  }

  @Test
  def testReadStreamWithQueryResumesFromCheckpointWithNewParams(): Unit = {
    createPersonNodes(0, 1)

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("streaming.property.name", "timestamp")
      .option("streaming.from", "ALL")
      .option(
        "query",
        """
          |MATCH (p:Person)
          |WHERE p.timestamp > $stream.from AND p.timestamp <= $stream.to
          |RETURN p.age AS age, p.timestamp AS timestamp
          |""".stripMargin
      )
      .option(
        "streaming.query.offset",
        """
          |MATCH (p:Person)
          |RETURN max(p.timestamp)
          |""".stripMargin
      )
      .load()

    val total = 60
    val expected: Seq[Map[String, Any]] = (0 to total).map(index =>
      Map(
        "age" -> s"$index"
      )
    ).toList

    val checkpoint = folder.newFolder()

    stream.writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpoint.getAbsolutePath)
      .toTable("readStreamWithQueryCheckpointNewParams")
      .awaitTermination()

    val partial: Int = total / 2
    // create partial number of persons starting from 1
    createPersonNodes(1, partial, 0, 10)

    // fetch whatever is available
    stream.writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpoint.getAbsolutePath)
      .toTable("readStreamWithQueryCheckpointNewParams")
      .awaitTermination()

    // create rest of the persons starting from partial+1
    createPersonNodes(partial + 1, total - partial, 0, 10)

    // fetch rest of the items from where we left off
    stream.writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", checkpoint.getAbsolutePath)
      .toTable("readStreamWithQueryCheckpointNewParams")
      .awaitTermination()

    assertEquals(
      expected,
      selectRowsFromTable("select * from readStreamWithQueryCheckpointNewParams order by timestamp", mapPerson)
    )
  }

  private def createPersonNodes(from: Int, count: Int, delayMs: Int = 0, intervalMs: Int = 0): Unit = {
    use(SparkConnectorScalaSuiteIT.session()) { session =>
      Thread.sleep(delayMs)
      (from until from + count).foreach(index => {
        Thread.sleep(intervalMs)
        session.run(
          s"CREATE (p:Person {age: '$index', timestamp: timestamp()})"
        ).consume()
      })
    }
  }

  private def createMovieNodes(from: Int, count: Int, delayMs: Int = 0, intervalMs: Int = 0): Unit = {
    use(SparkConnectorScalaSuiteIT.session()) { session =>
      Thread.sleep(delayMs)
      (from until from + count).foreach(index => {
        Thread.sleep(intervalMs)
        session.run(
          s"CREATE (n:Movie {title: 'My movie $index', timestamp: timestamp()})"
        ).consume()
      })
    }
  }

  private def createLikesRelationships(from: Int, count: Int, delayMs: Int = 0, intervalMs: Int = 0): Unit = {
    use(SparkConnectorScalaSuiteIT.session()) { session =>
      Thread.sleep(delayMs)
      (from until from + count).foreach(index => {
        Thread.sleep(intervalMs)
        session.run(
          s"""
             |CREATE (person:Person {age: $index})
             |CREATE (post:Post {hash: "hash$index"})
             |CREATE (person)-[:LIKES{id: $index, timestamp: timestamp()}]->(post)
             |""".stripMargin
        ).consume()
      })
    }
  }

  private def selectRowsFromTable(
    query: String,
    mapper: (Row) => Map[String, Any]
  ): Seq[Map[String, Any]] = {
    ss.sql(query)
      .collect()
      .map(row => mapper(row))
      .toList
  }

  private def mapPerson(row: Row): Map[String, Any] = {
    Map(
      "age" -> row.getAs[String]("age")
    )
  }

  private def mapMovie(row: Row): Map[String, Any] = {
    Map(
      "<labels>" -> row.getAs[java.util.List[String]]("<labels>"),
      "title" -> row.getAs[String]("title")
    )
  }

  private def mapLikes(row: Row): Map[String, Any] = {
    Map(
      "<rel.type>" -> row.getAs[String]("<rel.type>"),
      "<source.labels>" -> row.getAs[java.util.List[String]]("<source.labels>"),
      "source.age" -> row.getAs[Long]("source.age"),
      "<target.labels>" -> row.getAs[java.util.List[String]]("<target.labels>"),
      "target.hash" -> row.getAs[String]("target.hash"),
      "rel.id" -> row.getAs[Long]("rel.id")
    )
  }

}
