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
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.hamcrest.Matchers
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.neo4j.driver.Result
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.Transaction
import org.neo4j.driver.TransactionWork
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.spark.writer.DataWriterMetrics

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class DataSourceWriterNeo4jTSE extends SparkConnectorScalaBaseTSE {

  private val sparkSession = SparkSession.builder().getOrCreate()

  import sparkSession.implicits._

  @Test
  def `should read and write relations with append mode`(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id * $total, name: 'Product ' + id})
         |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db1"))
      .writeTransaction(
        new TransactionWork[Unit] {
          override def execute(tx: Transaction): Unit = {
            tx.run("MATCH (n) DETACH DELETE n")
            tx.run(fixtureQuery)
          }
        }
      )

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db2"))
      .writeTransaction(
        new TransactionWork[Unit] {
          override def execute(tx: Transaction): Unit = {
            tx.run("MATCH (n) DETACH DELETE n")
          }
        }
      )

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db2"))
      .writeTransaction(
        new TransactionWork[Unit] {
          override def execute(tx: Transaction): Unit = {
            tx.run("CREATE CONSTRAINT person_id FOR (p:Person) REQUIRE p.id IS UNIQUE")
            tx.run("CREATE CONSTRAINT product_id FOR (p:Product) REQUIRE p.id IS UNIQUE")
          }
        }
      )

    try {
      val dfOriginal: DataFrame = ss.read.format(classOf[DataSource].getName)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("database", "db1")
        .option("relationship", "BOUGHT")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Person")
        .option("relationship.target.labels", ":Product")
        .load()

      dfOriginal.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Append)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("database", "db2")
        .option("relationship", "SOLD")
        .option("relationship.save.strategy", "NATIVE")
        .option("relationship.source.labels", ":Person")
        .option("relationship.source.save.mode", "Append")
        .option("relationship.target.labels", ":Product")
        .option("relationship.target.save.mode", "Append")
        .option("batch.size", "11")
        .save()

      // let's write again to prove that 2 relationship are being added
      dfOriginal.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Append)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("database", "db2")
        .option("relationship", "SOLD")
        .option("relationship.save.strategy", "NATIVE")
        .option("relationship.source.labels", ":Person")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.node.keys", "source.id:id")
        .option("relationship.target.labels", ":Product")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.node.keys", "target.id:id")
        .option("batch.size", "11")
        .save()

      val dfCopy = ss.read.format(classOf[DataSource].getName)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("database", "db2")
        .option("relationship", "SOLD")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Person")
        .option("relationship.target.labels", ":Product")
        .load()

      val dfOriginalCount = dfOriginal.count()
      assertEquals(dfOriginalCount * 2, dfCopy.count())

      val resSourceOrig = dfOriginal.select("`source.id`").orderBy("`source.id`").collectAsList()
      val resSourceCopy = dfCopy.select("`source.id`").orderBy("`source.id`").collectAsList()
      val resTargetOrig = dfOriginal.select("`target.id`").orderBy("`target.id`").collectAsList()
      val resTargetCopy = dfCopy.select("`target.id`").orderBy("`target.id`").collectAsList()

      for (i <- 0 until 1) {
        assertEquals(
          resSourceOrig.get(i).getLong(0),
          resSourceCopy.get(i).getLong(0)
        )
        assertEquals(
          resTargetOrig.get(i).getLong(0),
          resTargetCopy.get(i).getLong(0)
        )
      }

      assertEquals(
        2,
        dfCopy.where("`source.id` = 1").count()
      )
    } finally {
      SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db2"))
        .writeTransaction(
          new TransactionWork[Unit] {
            override def execute(tx: Transaction): Unit = {
              tx.run("DROP CONSTRAINT person_id")
              tx.run("DROP CONSTRAINT product_id")
            }
          }
        )
    }
  }

  @Test
  def `should read and write relations with overwrite mode`(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id * $total, name: 'Product ' + id})
         |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db1"))
      .writeTransaction(
        new TransactionWork[Unit] {
          override def execute(tx: Transaction): Unit = {
            tx.run("MATCH (n) DETACH DELETE n")
            tx.run(fixtureQuery)
          }
        }
      )

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db2"))
      .writeTransaction(
        new TransactionWork[Unit] {
          override def execute(tx: Transaction): Unit = {
            tx.run("MATCH (n) DETACH DELETE n")
          }
        }
      )
    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db2"))
      .writeTransaction(
        new TransactionWork[Unit] {
          override def execute(tx: Transaction): Unit = {
            tx.run("CREATE CONSTRAINT person_id FOR (p:Person) REQUIRE p.id IS UNIQUE")
            tx.run("CREATE CONSTRAINT product_id FOR (p:Product) REQUIRE p.id IS UNIQUE")
          }
        }
      )

    try {
      val dfOriginal: DataFrame = ss.read.format(classOf[DataSource].getName)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("database", "db1")
        .option("relationship", "BOUGHT")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Person")
        .option("relationship.target.labels", ":Product")
        .load()
        .orderBy("`source.id`", "`target.id`")

      dfOriginal.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Overwrite)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("database", "db2")
        .option("relationship", "SOLD")
        .option("relationship.save.strategy", "NATIVE")
        .option("relationship.source.labels", ":Person")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Product")
        .option("relationship.target.save.mode", "Overwrite")
        .option("batch.size", "11")
        .save()

      // let's write the same thing again to prove there will be just one relation
      dfOriginal.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Overwrite)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("database", "db2")
        .option("relationship", "SOLD")
        .option("relationship.save.strategy", "NATIVE")
        .option("relationship.source.labels", ":Person")
        .option("relationship.source.node.keys", "source.id:id")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Product")
        .option("relationship.target.node.keys", "target.id:id")
        .option("relationship.target.save.mode", "Overwrite")
        .option("batch.size", "11")
        .save()

      val dfCopy = ss.read.format(classOf[DataSource].getName)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
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
    } finally {
      SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db2"))
        .writeTransaction(
          new TransactionWork[Unit] {
            override def execute(tx: Transaction): Unit = {
              tx.run("DROP CONSTRAINT person_id")
              tx.run("DROP CONSTRAINT product_id")
            }
          }
        )
    }
  }

  @Test
  def `should read and write relations with MATCH and node keys`(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id * $total, name: 'Product ' + id})
         |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db1"))
      .writeTransaction(
        new TransactionWork[Unit] {
          override def execute(tx: Transaction): Unit = {
            tx.run("MATCH (n) DETACH DELETE n")
            tx.run(fixtureQuery)
            tx.commit()
          }
        }
      )

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db2"))
      .writeTransaction(
        new TransactionWork[Unit] {
          override def execute(tx: Transaction): Unit = {
            tx.run("MATCH (n) DETACH DELETE n")
            tx.run(fixtureQuery)
            tx.commit()
          }
        }
      )

    val dfOriginal: DataFrame = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db1")
      .option("relationship", "BOUGHT")
      .option("relationship.nodes.map", "false")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .load()
      .orderBy("`source.id`", "`target.id`")

    dfOriginal.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db2")
      .option("relationship", "SOLD")
      .option("relationship.save.strategy", "NATIVE")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .option("relationship.source.node.keys", "source.id:id")
      .option("relationship.target.node.keys", "target.id:id")
      .option("relationship.source.save.mode", "Match")
      .option("relationship.target.save.mode", "Match")
      .option("batch.size", "11")
      .save()

    val dfCopy = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
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
  def `should read and write relations with MERGE and node keys`(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(new TransactionWork[Result] {
        override def execute(transaction: Transaction): Result =
          transaction.run("CREATE CONSTRAINT instrument_name FOR (i:Instrument) REQUIRE i.name IS UNIQUE")
      })

    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id * $total, name: 'Product ' + id})
         |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db1"))
      .writeTransaction(
        new TransactionWork[Unit] {
          override def execute(tx: Transaction): Unit = {
            tx.run("MATCH (n) DETACH DELETE n")
            tx.run(fixtureQuery)
            tx.commit()
          }
        }
      )

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db2"))
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run("MATCH (n) DETACH DELETE n").consume()
        }
      )

    val dfOriginal: DataFrame = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db1")
      .option("relationship", "BOUGHT")
      .option("relationship.nodes.map", "false")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .load()
      .orderBy("`source.id`", "`target.id`")

    dfOriginal.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db2")
      .option("relationship", "SOLD")
      .option("relationship.save.strategy", "NATIVE")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .option("relationship.source.node.keys", "source.id:id")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.target.node.keys", "target.id:id")
      .option("relationship.target.save.mode", "Overwrite")
      .option("batch.size", "11")
      .save()

    val dfCopy = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
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

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(new TransactionWork[Result] {
        override def execute(transaction: Transaction): Result = transaction.run("DROP CONSTRAINT instrument_name")
      })
  }

  @Test
  def `should read relations and write relation with match mode`(): Unit = {
    val fixtureQuery: String =
      s"""CREATE (m:Musician {name: "John Bonham", age: 32})
         |CREATE (i:Instrument {name: "Drums"})
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db1"))
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        }
      )

    val musicDf = Seq(
      (12, 32, "John Bonham", "Drums")
    ).toDF("experience", "age", "name", "instrument")

    musicDf.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db1")
      .option("relationship.nodes.map", "false")
      .option("relationship", "PLAYS")
      .option("relationship.source.save.mode", "Match")
      .option("relationship.target.save.mode", "Match")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.node.keys", "name,age")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.keys", "instrument:name")
      .save()

    val df2 = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
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
  def `should give a more clear error if properties or keys are inverted`(): Unit = {
    val musicDf = Seq(
      (1, 12, "John Henry Bonham", "Drums"),
      (2, 19, "John Mayer", "Guitar"),
      (3, 32, "John Scofield", "Guitar"),
      (4, 15, "John Butler", "Guitar")
    ).toDF("id", "experience", "name", "instrument")

    try {
      musicDf.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Overwrite)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("database", "db1")
        .option("relationship", "PLAYS")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.save.strategy", "keys")
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
  def `should give a more clear error if properties or keys are inverted on different options`(): Unit = {
    val musicDf = Seq(
      (1, 12, "John Henry Bonham", "Drums"),
      (2, 19, "John Mayer", "Guitar"),
      (3, 32, "John Scofield", "Guitar"),
      (4, 15, "John Butler", "Guitar")
    ).toDF("id", "experience", "name", "instrument")

    try {
      musicDf.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Overwrite)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("database", "db1")
        .option("relationship", "PLAYS")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.save.strategy", "keys")
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
  def `should give a more clear error if node properties or keys are inverted`(): Unit = {
    val musicDf = Seq(
      (1, 12, "John Henry Bonham", "Drums"),
      (2, 19, "John Mayer", "Guitar"),
      (3, 32, "John Scofield", "Guitar"),
      (4, 15, "John Butler", "Guitar")
    ).toDF("id", "experience", "name", "instrument")

    try {
      musicDf.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Append)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("database", "db1")
        .option("labels", "Person")
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
  def `exports write metrics`(): Unit = {
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
    sparkSession.sparkContext.addSparkListener(listener)

    try {
      input.toDF("name")
        .repartition(1)
        .write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Append)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("database", "db1")
        .option("batch.size", 1) // TODO: remove this when https://issues.apache.org/jira/browse/SPARK-45759 is fixed
        .option("query", query)
        .save()

      val db1Session = SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db1"))
      Assert.assertEventually(
        () => {
          db1Session.run(
            "MATCH (:Name)-[r:STARTS_WITH]->(:Letter) RETURN count(r) as cnt"
          ).single().get("cnt").asLong()
        },
        Matchers.equalTo(4L),
        30L,
        TimeUnit.SECONDS
      )
      assertNotNull(metrics.get())
      assertEquals(metrics.get(), expectedMetrics)

    } finally {
      sparkSession.sparkContext.removeSparkListener(listener)
    }
  }

  @Test
  def `does not create constraint if schema validation fails`(): Unit = {
    val cities = Seq(
      (1, "Cherbourg en Cotentin"),
      (2, "London"),
      (3, "Malmö")
    ).toDF("id", "city")

    try {
      cities.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Overwrite)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("labels", ":News")
        .option("node.keys", "newsId")
        .option("schema.optimization.node.keys", "UNIQUE")
        .save()
    } catch {
      case _: Exception => {}
    }

    var session: Session = null
    try {
      session = SparkConnectorScalaSuiteIT.driver.session()
      val result =
        session.run("SHOW CONSTRAINTS YIELD labelsOrTypes WHERE labelsOrTypes[0] = 'News' RETURN count(*) AS count")
          .single()
          .get("count")
          .asLong()
      assertEquals(0, result)
    } finally {
      if (session != null) {
        session.close()
      }
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
