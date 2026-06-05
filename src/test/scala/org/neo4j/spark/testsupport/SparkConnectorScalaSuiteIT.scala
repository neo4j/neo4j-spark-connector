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
package org.neo4j.spark.testsupport

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.util.SetSystemProperty
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDetector
import org.neo4j.driver._

import java.io.File
import java.nio.file.Files
import java.util.TimeZone

object SparkConnectorScalaSuiteIT {

  val server: Neo4jContainerExtension = new Neo4jContainerExtension()
    .withNeo4jConfig("dbms.security.auth_enabled", "false")
    .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    .withEnv("NEO4J_db_temporal_timezone", TimeZone.getDefault.getID)
    .withDatabases(Seq("db1", "db2"))

  var conf: SparkConf = _
  var ss: SparkSession = _
  var driver: Driver = _
  var tmpDir: File = _
  var neo4j: Neo4j = _

  @BeforeAll
  def setUpContainer(): Unit = {
    if (!server.isRunning) {
      try {
        server.start()
      } catch {
        case _: Throwable => //
      }
    }
    if (server.isRunning && driver == null) {
      tmpDir = Files.createTempDirectory("spark-warehouse").toFile
      tmpDir.deleteOnExit()
      conf = new SparkConf()
        .setAppName("neoTest")
        .setMaster("local[*]")
        .set("spark.driver.host", "127.0.0.1")
        .set("spark.sql.warehouse.dir", tmpDir.getAbsolutePath)
      ss = SparkSession.builder().config(conf).getOrCreate()
      driver = GraphDatabase.driver(server.getBoltUrl, AuthTokens.none())
      neo4j = Neo4jDetector.INSTANCE.detect(driver)
    }
    assumeTrue(server.isRunning, "Neo4j container is not started")
  }

  @AfterAll
  def tearDownContainer(): Unit = {
    TestUtil.closeSafely(driver)
    driver = null
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

@SetSystemProperty(key = "neo4j.spark.strict.query", value = "true")
class SparkConnectorScalaSuiteIT {}
