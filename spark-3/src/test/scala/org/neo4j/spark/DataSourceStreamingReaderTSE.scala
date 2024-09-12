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

import org.apache.spark.sql.streaming.StreamingQuery
import org.hamcrest.Matchers
import org.junit.After
import org.junit.Test
import org.neo4j.driver.Transaction
import org.neo4j.driver.TransactionWork
import org.neo4j.driver.summary.ResultSummary

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class DataSourceStreamingReaderTSE extends SparkConnectorScalaBaseTSE {

  private var query: StreamingQuery = null

  @After
  def close(): Unit = {
    if (query != null) {
      query.stop()
    }
  }

  @Test
  def testReadStreamWithLabels(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = {
            tx.run(s"CREATE (n:Test1_Movie {title: 'My movie 0', timestamp: timestamp()})").consume()
          }
        }
      )

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Test1_Movie")
      .option("streaming.property.name", "timestamp")
      .option("streaming.from", "NOW")
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("testReadStream")
      .start()

    val total = 60

    val expected: Seq[Map[String, Any]] = (1 to total).map(index =>
      Map(
        "<labels>" -> Seq("Test1_Movie"),
        "title" -> s"My movie $index"
      )
    )

    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        Thread.sleep(1000)
        (1 to total).foreach(index => {
          Thread.sleep(200)
          SparkConnectorScalaSuiteIT.session()
            .writeTransaction(new TransactionWork[ResultSummary] {
              override def execute(tx: Transaction): ResultSummary = {
                tx.run(s"CREATE (n:Test1_Movie {title: 'My movie $index', timestamp: timestamp()})")
                  .consume()
              }
            })
        })
      }
    })

    Assert.assertEventually(
      new Assert.ThrowingSupplier[Seq[Map[String, Any]], Exception] {
        override def get(): Seq[Map[String, Any]] = {
          val df = ss.sql("select * from testReadStream order by timestamp")
          val collect = df.collect()
          val actual = if (!df.columns.contains("title")) {
            Array.empty
          } else {
            collect.map(row =>
              Map(
                "<labels>" -> row.getAs[java.util.List[String]]("<labels>"),
                "title" -> row.getAs[String]("title")
              )
            )
          }
          actual.toList
        }
      },
      Matchers.equalTo(expected),
      30L,
      TimeUnit.SECONDS
    )
  }

  @Test
  def testReadStreamWithLabelsGetAll(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = {
            tx.run(s"CREATE (n:Test4_Movie {title: 'My movie 0', timestamp: timestamp()})").consume()
          }
        }
      )

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Test4_Movie")
      .option("streaming.property.name", "timestamp")
      .option("streaming.from", "ALL")
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("testReadStream")
      .start()

    val total = 60
    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        Thread.sleep(1000)
        (1 to total).foreach(index => {
          Thread.sleep(200)
          SparkConnectorScalaSuiteIT.session()
            .writeTransaction(new TransactionWork[ResultSummary] {
              override def execute(tx: Transaction): ResultSummary = {
                tx.run(s"CREATE (n:Test4_Movie {title: 'My movie $index', timestamp: timestamp()})")
                  .consume()
              }
            })
        })
      }
    })

    val expected: Seq[Map[String, Any]] = (0 to total).map(index =>
      Map(
        "<labels>" -> Seq("Test4_Movie"),
        "title" -> s"My movie $index"
      )
    )

    Assert.assertEventually(
      new Assert.ThrowingSupplier[Seq[Map[String, Any]], Exception] {
        override def get(): Seq[Map[String, Any]] = {
          val df = ss.sql("select * from testReadStream order by timestamp")
          val collect = df.collect()
          val actual = if (!df.columns.contains("title")) {
            Array.empty
          } else {
            collect.map(row =>
              Map(
                "<labels>" -> row.getAs[java.util.List[String]]("<labels>"),
                "title" -> row.getAs[String]("title")
              )
            )
          }
          actual.toList
        }
      },
      Matchers.equalTo(expected),
      30L,
      TimeUnit.SECONDS
    )
  }

  @Test
  def testReadStreamWithRelationship(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(
            """
              |CREATE (person:Test2_Person {age: 0})
              |CREATE (post:Test2_Post {hash: "hash0"})
              |CREATE (person)-[:LIKES{id: 0, timestamp: timestamp()}]->(post)
              |""".stripMargin
          ).consume()
        }
      )

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "LIKES")
      .option("streaming.property.name", "timestamp")
      .option("streaming.from", "NOW")
      .option("relationship.source.labels", "Test2_Person")
      .option("relationship.target.labels", "Test2_Post")
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("testReadStream")
      .start()

    val total = 60

    val expected: Seq[Map[String, Any]] = (1 to total).map(index =>
      Map(
        "<rel.type>" -> "LIKES",
        "<source.labels>" -> Seq("Test2_Person"),
        "source.age" -> index,
        "<target.labels>" -> Seq("Test2_Post"),
        "target.hash" -> s"hash$index",
        "rel.id" -> index
      )
    )

    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        Thread.sleep(1000)
        (1 to total).foreach(index => {
          Thread.sleep(200)
          SparkConnectorScalaSuiteIT.session()
            .writeTransaction(new TransactionWork[ResultSummary] {
              override def execute(tx: Transaction): ResultSummary = {
                tx.run(
                  s"""
                     |CREATE (person:Test2_Person {age: $index})
                     |CREATE (post:Test2_Post {hash: "hash$index"})
                     |CREATE (person)-[:LIKES{id: $index, timestamp: timestamp()}]->(post)
                     |""".stripMargin
                )
                  .consume()
              }
            })
        })
      }
    })

    Assert.assertEventually(
      new Assert.ThrowingSupplier[Seq[Map[String, Any]], Exception] {
        override def get(): Seq[Map[String, Any]] = {
          val df = ss.sql("select * from testReadStream order by `rel.timestamp`")
          val collect = df.collect()
          val actual: Array[Map[String, Any]] =
            if (!df.columns.contains("source.age") || !df.columns.contains("target.hash")) {
              Array.empty
            } else {
              collect.map(row =>
                Map(
                  "<rel.type>" -> row.getAs[String]("<rel.type>"),
                  "<source.labels>" -> row.getAs[java.util.List[String]]("<source.labels>"),
                  "source.age" -> row.getAs[Long]("source.age"),
                  "<target.labels>" -> row.getAs[java.util.List[String]]("<target.labels>"),
                  "target.hash" -> row.getAs[String]("target.hash"),
                  "rel.id" -> row.getAs[Long]("rel.id")
                )
              )
            }
          actual.toList
        }
      },
      Matchers.equalTo(expected),
      40L,
      TimeUnit.SECONDS
    )
  }

  @Test
  def testReadStreamWithRelationshipGetAll(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(
            """
              |CREATE (person:Test5_Person {age: 0})
              |CREATE (post:Test5_Post {hash: "hash0"})
              |CREATE (person)-[:LIKES{id: 0, timestamp: timestamp()}]->(post)
              |""".stripMargin
          ).consume()
        }
      )

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "LIKES")
      .option("streaming.property.name", "timestamp")
      .option("streaming.from", "ALL")
      .option("relationship.source.labels", "Test5_Person")
      .option("relationship.target.labels", "Test5_Post")
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("testReadStream")
      .start()

    val total = 60
    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        Thread.sleep(1000)
        (1 to total).foreach(index => {
          Thread.sleep(200)
          SparkConnectorScalaSuiteIT.session()
            .writeTransaction(new TransactionWork[ResultSummary] {
              override def execute(tx: Transaction): ResultSummary = {
                tx.run(
                  s"""
                     |CREATE (person:Test5_Person {age: $index})
                     |CREATE (post:Test5_Post {hash: "hash$index"})
                     |CREATE (person)-[:LIKES{id: $index, timestamp: timestamp()}]->(post)
                     |""".stripMargin
                )
                  .consume()
              }
            })
        })
      }
    })

    val expected: Seq[Map[String, Any]] = (0 to total).map(index =>
      Map(
        "<rel.type>" -> "LIKES",
        "<source.labels>" -> Seq("Test5_Person"),
        "source.age" -> index,
        "<target.labels>" -> Seq("Test5_Post"),
        "target.hash" -> s"hash$index",
        "rel.id" -> index
      )
    )

    Assert.assertEventually(
      new Assert.ThrowingSupplier[Seq[Map[String, Any]], Exception] {
        override def get(): Seq[Map[String, Any]] = {
          val df = ss.sql("select * from testReadStream order by `rel.timestamp`")
          val collect = df.collect()
          val actual: Array[Map[String, Any]] =
            if (!df.columns.contains("source.age") || !df.columns.contains("target.hash")) {
              Array.empty
            } else {
              collect.map(row =>
                Map(
                  "<rel.type>" -> row.getAs[String]("<rel.type>"),
                  "<source.labels>" -> row.getAs[java.util.List[String]]("<source.labels>"),
                  "source.age" -> row.getAs[Long]("source.age").toInt,
                  "<target.labels>" -> row.getAs[java.util.List[String]]("<target.labels>"),
                  "target.hash" -> row.getAs[String]("target.hash"),
                  "rel.id" -> row.getAs[Long]("rel.id").toInt
                )
              )
            }
          actual.toList
        }
      },
      Matchers.equalTo(expected),
      40L,
      TimeUnit.SECONDS
    )
  }

  @Test
  def testReadStreamWithQuery(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx
            .run("CREATE (person:Test3_Person) SET person.age = 0, person.timestamp = timestamp()")
            .consume()
        }
      )

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("streaming.from", "NOW")
      .option("streaming.property.name", "timestamp")
      .option(
        "query",
        """
          |MATCH (p:Test3_Person)
          |WHERE p.timestamp > $stream.offset
          |RETURN p.age AS age, p.timestamp AS timestamp
          |""".stripMargin
      )
      .option(
        "streaming.query.offset",
        """
          |MATCH (p:Test3_Person)
          |RETURN max(p.timestamp)
          |""".stripMargin
      )
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("testReadStream")
      .start()

    val total = 60

    val expected: Seq[Int] = (1 to total).toList

    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        Thread.sleep(1000)
        (1 to total).foreach(index => {
          Thread.sleep(200)
          SparkConnectorScalaSuiteIT.session()
            .writeTransaction(new TransactionWork[ResultSummary] {
              override def execute(tx: Transaction): ResultSummary = {
                tx.run(s"CREATE (person:Test3_Person) SET person.age = $index, person.timestamp = timestamp()")
                  .consume()
              }
            })
        })
      }
    })

    Assert.assertEventually(
      new Assert.ThrowingSupplier[Seq[Int], Exception] {
        override def get(): Seq[Int] = {
          val df = ss.sql("select * from testReadStream ")
          val collect = df.collect()
          val actual: Array[Int] = if (!df.columns.contains("age")) {
            Array.empty
          } else {
            collect.map(row => row.getAs[String]("age").toInt)
              .sorted
          }
          actual.toList
        }
      },
      Matchers.equalTo(expected),
      40L,
      TimeUnit.SECONDS
    )
  }

  @Test
  def testReadStreamWithQueryGetAll(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx
            .run("CREATE (person:Test3_Person) SET person.age = 0, person.timestamp = timestamp()")
            .consume()
        }
      )

    val stream = ss.readStream.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("streaming.property.name", "timestamp")
      .option("streaming.from", "ALL")
      .option(
        "query",
        """
          |MATCH (p:Test3_Person)
          |WHERE p.timestamp > $stream.offset
          |RETURN p.age AS age, p.timestamp AS timestamp
          |""".stripMargin
      )
      .option(
        "streaming.query.offset",
        """
          |MATCH (p:Test3_Person)
          |RETURN max(p.timestamp)
          |""".stripMargin
      )
      .load()

    query = stream.writeStream
      .format("memory")
      .queryName("testReadStream")
      .start()

    val total = 60

    val expected: Seq[Int] = (0 to total).toList

    Executors.newSingleThreadExecutor().submit(new Runnable {
      override def run(): Unit = {
        Thread.sleep(1000)
        (1 to total).foreach(index => {
          Thread.sleep(200)
          SparkConnectorScalaSuiteIT.session()
            .writeTransaction(new TransactionWork[ResultSummary] {
              override def execute(tx: Transaction): ResultSummary = {
                tx.run(s"CREATE (person:Test3_Person) SET person.age = $index, person.timestamp = timestamp()")
                  .consume()
              }
            })
        })
      }
    })

    Assert.assertEventually(
      new Assert.ThrowingSupplier[Seq[Int], Exception] {
        override def get(): Seq[Int] = {
          val df = ss.sql("select * from testReadStream ")
          val collect = df.collect()
          val actual: Array[Int] = if (!df.columns.contains("age")) {
            Array.empty
          } else {
            collect.map(row => row.getAs[String]("age").toInt)
              .sorted
          }
          actual.toList
        }
      },
      Matchers.equalTo(expected),
      40L,
      TimeUnit.SECONDS
    )
  }
}
