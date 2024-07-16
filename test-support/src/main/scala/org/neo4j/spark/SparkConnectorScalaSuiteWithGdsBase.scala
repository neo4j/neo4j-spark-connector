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
import org.neo4j.Neo4jContainerExtension
import org.neo4j.driver._
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.spark

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

  private var _session: Session = _

  var connections: Long = 0

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
      if (TestUtil.isCI()) {
        org.apache.log4j.LogManager.getLogger("org")
          .setLevel(org.apache.log4j.Level.OFF)
      }
      driver = GraphDatabase.driver(server.getBoltUrl, AuthTokens.none())
      session()
        .readTransaction((tx: Transaction) => tx.run("RETURN 1").consume())
      connections = getActiveConnections
      ()
    }
  }

  @AfterClass
  def tearDownContainer(): Unit = {
    TestUtil.closeSafely(session())
    TestUtil.closeSafely(driver)
    TestUtil.closeSafely(server)
    TestUtil.closeSafely(ss)
  }

  def session(): Session = {
    if (_session == null || !_session.isOpen) {
      _session = if (driver != null) driver.session else null
    }
    _session
  }

  def getActiveConnections: Long = session()
    .readTransaction((tx: Transaction) =>
      tx.run(
        """|CALL dbms.listConnections() YIELD connectionId, connector
           |WHERE connector = 'bolt'
           |RETURN count(*) AS connections""".stripMargin
      )
        .single()
        .get("connections")
        .asLong()
    )
}

class SparkConnectorScalaSuiteWithGdsBase {

  val conf: SparkConf = SparkConnectorScalaSuiteWithGdsBase.conf
  val ss: SparkSession = SparkConnectorScalaSuiteWithGdsBase.ss

  @(Rule @getter)
  val testName: TestName = new TestName

  @Before
  def before(): Unit = {
    SparkConnectorScalaSuiteWithGdsBase.session()
      .writeTransaction((tx: Transaction) => tx.run("MATCH (n) DETACH DELETE n").consume())
  }

  @After
  def after(): Unit = {
    if (!TestUtil.isCI()) {
      try {
        spark.Assert.assertEventually(
          new spark.Assert.ThrowingSupplier[Boolean, Exception] {
            override def get(): Boolean = {
              val afterConnections = SparkConnectorScalaSuiteWithGdsBase.getActiveConnections
              SparkConnectorScalaSuiteWithGdsBase.connections == afterConnections
            }
          },
          Matchers.equalTo(true),
          45,
          TimeUnit.SECONDS
        )
      } finally {
        val afterConnections = SparkConnectorScalaSuiteWithGdsBase.getActiveConnections
        if (SparkConnectorScalaSuiteWithGdsBase.connections != afterConnections) { // just for debug purposes
          println(
            s"For test ${testName.getMethodName.replaceAll("$u0020", " ")} => connections before: ${SparkConnectorScalaSuiteWithGdsBase.connections}, after: $afterConnections"
          )
        }
      }
    }
  }
}
