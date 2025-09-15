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
import org.apache.spark.sql.SparkSession
import org.junit.AfterClass
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.BeforeClass
import org.junit.Test
import org.neo4j.Neo4jContainerExtension
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.spark.SparkConnectorScalaSuiteWithApocIT.conf
import org.neo4j.spark.SparkConnectorScalaSuiteWithApocIT.server
import org.neo4j.spark.SparkConnectorScalaSuiteWithApocIT.ss
import org.neo4j.spark.TransactionTimeoutIT.NEO4J_LOW_TX_TIMEOUT

import java.util.TimeZone

class TransactionTimeoutIT extends SparkConnectorScalaSuiteWithApocIT {

  @Test
  def sparkConnectorRespectsTransactionTimeout(): Unit = {
    val cypher = "UNWIND range(1, 3) AS i " +
      "CALL apoc.util.sleep(1000) " +
      "RETURN i as number"
    val df = ss.read.format("org.neo4j.spark.DataSource")
      .option("url", server.getBoltUrl)
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", server.getAdminPassword)
      .option("db.transaction.timeout", "4000")
      .option("query", cypher)
      .load()
      .toDF()

    val results = df.select("number").rdd.map(_.getLong(0)).collect().toList
    val expected = Range.inclusive(1, 3).map(_.toLong).toList

    assertEquals(expected, results)
  }

  @Test
  def sparkConnectorFailsWithTransactionTimeoutWhenSetOnSessionLevel(): Unit = {
    val newConf = conf.clone().set("neo4j.url", server.getBoltUrl)
      .set("neo4j.authentication.basic.username", "neo4j")
      .set("neo4j.authentication.basic.password", server.getAdminPassword)
      .set("neo4j.db.transaction.timeout", "1000")
    val session = SparkSession.builder.config(newConf).getOrCreate()

    val cypher = "UNWIND range(1, 20) AS i " +
      "CALL apoc.util.sleep(1000) " +
      "RETURN i as number"

    val df = session.read.format("org.neo4j.spark.DataSource")
      .option("query", cypher)

    val exc = assertThrows(
      classOf[ClientException],
      () => {
        df.load()
          .toDF()
          .select("number").rdd.map(_.getLong(0)).collect().toList
      }
    )
    assertTrue(exc.getMessage.contains("The transaction has been terminated"))
  }

  @Test
  def sparkConnectorFailsWithTransactionTimeoutWhenSetOnDatasourceLevel(): Unit = {
    val newConf = conf.clone().set("neo4j.url", server.getBoltUrl)
      .set("neo4j.authentication.basic.username", "neo4j")
      .set("neo4j.authentication.basic.password", server.getAdminPassword)
    val session = SparkSession.builder.config(newConf).getOrCreate()

    val cypher = "UNWIND range(1, 20) AS i " +
      "CALL apoc.util.sleep(1000) " +
      "RETURN i as number"

    val df = session.read.format("org.neo4j.spark.DataSource")
      .option("query", cypher)
      .option("db.transaction.timeout", "1000")

    val exc = assertThrows(
      classOf[ClientException],
      () => {
        df.load()
          .toDF()
          .select("number").rdd.map(_.getLong(0)).collect().toList
      }
    )
    assertTrue(exc.getMessage.contains("The transaction has been terminated"))
  }

  @Test
  def sparkConnectorExtendsDefaultTimeut(): Unit = {
    val cypher = "UNWIND range(1, 6) AS i " +
      "CALL apoc.util.sleep(1000) " +
      "RETURN i as number"
    val df = ss.read.format("org.neo4j.spark.DataSource")
      .option("url", NEO4J_LOW_TX_TIMEOUT.getBoltUrl)
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", NEO4J_LOW_TX_TIMEOUT.getAdminPassword)
      .option("db.transaction.timeout", "7000")
      .option("query", cypher)
      .load()
      .toDF()

    val results = df.select("number").rdd.map(_.getLong(0)).collect().toList
    val expected = Range.inclusive(1, 6).map(_.toLong).toList

    assertEquals(expected, results)
  }

}

object TransactionTimeoutIT {

  private val NEO4J_LOW_TX_TIMEOUT = new Neo4jContainerExtension()
    .withNeo4jConfig("dbms.security.auth_enabled", "false")
    .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    .withEnv("NEO4JLABS_PLUGINS", "[\"apoc\"]")
    .withEnv("NEO4J_db_temporal_timezone", TimeZone.getDefault.getID)
    .withNeo4jConfig("db.transaction.timeout", "5s")
    .withDatabases(Seq("db1", "db2"))

  @BeforeClass
  def setUp(): Unit = {
    NEO4J_LOW_TX_TIMEOUT.start()
  }

  @AfterClass
  def tearDown() = {
    TestUtil.closeSafely(NEO4J_LOW_TX_TIMEOUT)
  }
}
