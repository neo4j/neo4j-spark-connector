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
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import org.neo4j.spark.SparkConnectorScalaSuiteWithApocIT.conf
import org.neo4j.spark.SparkConnectorScalaSuiteWithApocIT.server
import org.neo4j.spark.SparkConnectorScalaSuiteWithApocIT.ss

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
      .load()
      .toDF()

    val exc = assertThrows(
      classOf[SparkException],
      () => {
        df.select("number").rdd.map(_.getLong(0)).collect().toList
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
      .load()
      .toDF()

    val exc = assertThrows(
      classOf[SparkException],
      () => {
        df.select("number").rdd.map(_.getLong(0)).collect().toList
      }
    )
    assertTrue(exc.getMessage.contains("The transaction has been terminated"))
  }

}
