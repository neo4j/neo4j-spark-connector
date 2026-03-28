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

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQuery
import org.hamcrest.Matchers
import org.junit.After
import org.junit.Test
import org.neo4j.driver.Transaction
import org.neo4j.driver.TransactionWork
import org.neo4j.spark.Assert.ThrowingSupplier

import java.util.UUID
import java.util.concurrent.TimeUnit

class DataSourceStreamingWriterTSE extends SparkConnectorScalaBaseTSE {

  private var query: StreamingQuery = null

  @After
  def close(): Unit = {
    if (query != null) {
      query.stop()
    }
  }

  @Test
  def testSinkStreamWithLabelsWithAppend(): Unit = {
    implicit val ctx = ss.sqlContext
    import ss.implicits._
    val memStream = MemoryStream[Int]
    val recordSize = 2000
    val partition = 5
    val checkpointLocation = "/tmp/checkpoint/" + UUID.randomUUID().toString
    query = memStream.toDF().writeStream
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("save.mode", "Append")
      .option("labels", "Timestamp")
      .option("checkpointLocation", checkpointLocation)
      .option("node.keys", "value")
      .start()

    (1 to partition).foreach(index => {
      // we send the total of records in 5 times
      val start = ((index - 1) * recordSize) + 1
      val end = index * recordSize
      memStream.addData((start to end).toArray)
    })

    Assert.assertEventually(
      new ThrowingSupplier[Boolean, Exception] {
        override def get(): Boolean = {
          val dataFrame = ss.read.format(classOf[DataSource].getName)
            .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
            .option("labels", "Timestamp")
            .load()

          val collect = dataFrame.collect()
          val data = if (dataFrame.columns.contains("value")) {
            collect
              .map(row => row.getAs[Long]("value").toInt)
              .sorted
          } else {
            Array.empty[Int]
          }
          data.toList == (1 to (recordSize * partition)).toList
        }
      },
      Matchers.equalTo(true),
      30L,
      TimeUnit.SECONDS
    )
  }

  @Test
  def testSinkStreamWithRelationshipWithAppend(): Unit = {
    implicit val ctx = ss.sqlContext
    import ss.implicits._
    val memStream = MemoryStream[Int]
    val recordSize = 2000
    val partition = 5
    val checkpointLocation = "/tmp/checkpoint/" + UUID.randomUUID().toString

    query = memStream.toDF().writeStream
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("save.mode", "Append")
      .option("relationship", "PAIRS")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":From")
      .option("relationship.source.node.keys", "value")
      .option("relationship.source.save.mode", "Append")
      .option("relationship.target.labels", ":To")
      .option("relationship.target.node.keys", "value")
      .option("relationship.target.save.mode", "Append")
      .option("checkpointLocation", checkpointLocation)
      .start()

    (1 to partition).foreach(index => {
      // we send the total of records in 5 times
      val start = ((index - 1) * recordSize) + 1
      val end = index * recordSize
      memStream.addData((start to end).toArray)
    })

    Assert.assertEventually(
      new ThrowingSupplier[Boolean, Exception] {
        override def get(): Boolean =
          try {
            val dataFrame = ss.read.format(classOf[DataSource].getName)
              .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
              .option("relationship", "PAIRS")
              .option("relationship.source.labels", ":From")
              .option("relationship.target.labels", ":To")
              .load()

            val collect = dataFrame.collect()
            val data = if (dataFrame.columns.contains("source.value") && dataFrame.columns.contains("target.value")) {
              collect
                .map(row => (row.getAs[Long]("source.value").toInt, row.getAs[Long]("target.value").toInt))
                .sorted
            } else {
              Array.empty[(Int, Int)]
            }
            data.toList == (1 to (recordSize * partition)).map(v => (v, v)).toList
          } catch {
            case _: Throwable => false
          }
      },
      Matchers.equalTo(true),
      30L,
      TimeUnit.SECONDS
    )
  }

  @Test
  def testSinkStreamWithQuery(): Unit = {
    implicit val ctx = ss.sqlContext
    import ss.implicits._
    val memStream = MemoryStream[Int]
    val recordSize = 2000
    val partition = 5
    val checkpointLocation = "/tmp/checkpoint/" + UUID.randomUUID().toString

    query = memStream.toDF().writeStream
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query", "MERGE (m:MyNewNode {the_value: event.value})")
      .option("checkpointLocation", checkpointLocation)
      .start()

    (1 to partition).foreach(index => {
      // we send the total of records in 5 times
      val start = ((index - 1) * recordSize) + 1
      val end = index * recordSize
      memStream.addData((start to end).toArray)
    })

    Assert.assertEventually(
      new ThrowingSupplier[Boolean, Exception] {
        override def get(): Boolean =
          try {
            val dataFrame = ss.read.format(classOf[DataSource].getName)
              .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
              .option("labels", "MyNewNode")
              .load()

            val collect = dataFrame.collect()
            val data = if (dataFrame.columns.contains("the_value")) {
              collect
                .map(row => row.getAs[Long]("the_value").toInt)
                .sorted
            } else {
              Array.empty[Int]
            }
            val l1 = data.toList
            val l2 = (1 to (recordSize * partition)).map(v => v).toList
            l1 == l2
          } catch {
            case _: Throwable => false
          }
      },
      Matchers.equalTo(true),
      30L,
      TimeUnit.SECONDS
    )
  }

  @Test
  def testSinkStreamWithLabelsWithOverwrite(): Unit = {
    implicit val ctx = ss.sqlContext
    import ss.implicits._
    val memStream = MemoryStream[Int]
    val partition = 5
    val checkpointLocation = "/tmp/checkpoint/" + UUID.randomUUID().toString

    SparkConnectorScalaSuiteIT.session().run(
      "CREATE CONSTRAINT timestamp_value FOR (t:Timestamp) REQUIRE (t.value) IS UNIQUE"
    )

    query = memStream.toDF().writeStream
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("save.mode", "Overwrite")
      .option("labels", "Timestamp")
      .option("checkpointLocation", checkpointLocation)
      .option("node.keys", "value")
      .start()
    (1 to partition).foreach(index => {
      memStream.addData((1 to 500).toArray)
    })

    Assert.assertEventually(
      new ThrowingSupplier[Boolean, Exception] {
        override def get(): Boolean = {
          val dataFrame = ss.read.format(classOf[DataSource].getName)
            .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
            .option("labels", "Timestamp")
            .load()

          val collect = dataFrame.collect()
          val data = if (dataFrame.columns.contains("value")) {
            collect
              .map(row => row.getAs[Long]("value").toInt)
              .sorted
          } else {
            Array.empty[Int]
          }
          data.toList == (1 to 500).toList
        }
      },
      Matchers.equalTo(true),
      30L,
      TimeUnit.SECONDS
    )

    SparkConnectorScalaSuiteIT.session().run("DROP CONSTRAINT timestamp_value")
  }

  @Test
  def testSinkStreamWithRelationshipWithAppendAndOverwrite(): Unit = {
    implicit val ctx = ss.sqlContext
    import ss.implicits._
    val memStream = MemoryStream[Int]
    val partition = 5
    val checkpointLocation = "/tmp/checkpoint/" + UUID.randomUUID().toString

    SparkConnectorScalaSuiteIT.driver.session()
      .writeTransaction(
        new TransactionWork[Unit] {
          override def execute(tx: Transaction): Unit = {
            tx.run("CREATE CONSTRAINT From_value FOR (p:From) REQUIRE p.value IS UNIQUE")
            tx.run("CREATE CONSTRAINT To_value FOR (p:To) REQUIRE p.value IS UNIQUE")
          }
        }
      )

    query = memStream.toDF().writeStream
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("save.mode", "Append")
      .option("relationship", "PAIRS")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":From")
      .option("relationship.source.node.keys", "value")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.target.labels", ":To")
      .option("relationship.target.node.keys", "value")
      .option("relationship.target.save.mode", "Overwrite")
      .option("checkpointLocation", checkpointLocation)
      .start()

    (1 to partition).foreach(index => {
      memStream.addData((1 to 500).toArray)
    })

    Assert.assertEventually(
      new ThrowingSupplier[Boolean, Exception] {
        override def get(): Boolean =
          try {
            val dataFrame = ss.read.format(classOf[DataSource].getName)
              .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
              .option("relationship", "PAIRS")
              .option("relationship.source.labels", ":From")
              .option("relationship.target.labels", ":To")
              .load()

            val collect = dataFrame.collect()
            val data = if (dataFrame.columns.contains("source.value") && dataFrame.columns.contains("target.value")) {
              collect
                .map(row => (row.getAs[Long]("source.value").toInt, row.getAs[Long]("target.value").toInt))
                .sorted
            } else {
              Array.empty[(Int, Int)]
            }
            data.toList == (1 to 500).flatMap(v => (1 to 5).map(_ => (v, v)))
          } catch {
            case _: Throwable => false
          }
      },
      Matchers.equalTo(true),
      30L,
      TimeUnit.SECONDS
    )

    SparkConnectorScalaSuiteIT.driver.session()
      .writeTransaction(
        new TransactionWork[Unit] {
          override def execute(tx: Transaction): Unit = {
            tx.run("DROP CONSTRAINT From_value")
            tx.run("DROP CONSTRAINT To_value")
          }
        }
      )
  }
}
