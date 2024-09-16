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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.hamcrest.Matchers
import org.junit.After
import org.junit.AfterClass
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.rules.TestName
import org.neo4j.Closeables.use
import org.neo4j.Neo4jContainerExtension
import org.neo4j.driver._
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.spark
import org.neo4j.spark.SparkConnectorScalaSuiteWithApocIT.driver

import java.util.TimeZone
import java.util.concurrent.TimeUnit

import scala.annotation.meta.getter

object SparkConnectorScalaSuiteWithGdsBase {

  val server: Neo4jContainerExtension = new Neo4jContainerExtension()
    .withNeo4jConfig("dbms.security.auth_enabled", "false")
    .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    .withEnv("NEO4JLABS_PLUGINS", "[\"graph-data-science\"]")
    .withEnv("NEO4J_db_temporal_timezone", TimeZone.getDefault.getID)
    .withDatabases(Seq("db1", "db2"))

  var conf: SparkConf = _
  var ss: SparkSession = _
  var driver: Driver = _

  @BeforeClass
  def setUpContainer(): Unit = {
    if (!server.isRunning) {
      try {
        server.start()
      } catch {
        case _: Throwable => //
      }
      Assume.assumeTrue("Neo4j container is not started", server.isRunning)
      conf = new SparkConf()
        .setAppName("neoTest")
        .setMaster("local[*]")
        .set("spark.driver.host", "127.0.0.1")
      ss = SparkSession.builder.config(conf).getOrCreate()
      driver = GraphDatabase.driver(server.getBoltUrl, AuthTokens.none())
      session()
        .readTransaction((tx: Transaction) => tx.run("RETURN 1").consume())
      ()
    }
  }

  @AfterClass
  def tearDownContainer(): Unit = {
    TestUtil.closeSafely(driver)
    TestUtil.closeSafely(server)
    TestUtil.closeSafely(ss)
  }

  def session(database: String = ""): Session = {
    if (database.isEmpty) {
      driver.session()
    } else {
      driver.session(SessionConfig.forDatabase(database))
    }
  }
}

class SparkConnectorScalaSuiteWithGdsBase {

  val conf: SparkConf = SparkConnectorScalaSuiteWithGdsBase.conf
  val ss: SparkSession = SparkConnectorScalaSuiteWithGdsBase.ss

  @(Rule @getter)
  val testName: TestName = new TestName

  @Before
  def before(): Unit = {
    use(SparkConnectorScalaSuiteWithGdsBase.session("system")) {
      session =>
        session.run("CREATE OR REPLACE DATABASE neo4j WAIT 30 seconds")
          .consume()
    }
  }
}
