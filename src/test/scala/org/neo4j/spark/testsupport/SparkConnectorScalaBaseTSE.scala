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
import org.junit.jupiter.api._
import org.neo4j.spark.testsupport.Closeables.use

object SparkConnectorScalaBaseTSE {

  private var startedFromSuite = true

  @BeforeAll
  def setUpContainer() = {
    if (!SparkConnectorScalaSuiteIT.server.isRunning) {
      startedFromSuite = false
    }
    SparkConnectorScalaSuiteIT.setUpContainer()
  }

  @AfterAll
  def tearDownContainer() = {
    if (!startedFromSuite) {
      SparkConnectorScalaSuiteIT.tearDownContainer()
    }
  }

}

class SparkConnectorScalaBaseTSE {
  val conf: SparkConf = SparkConnectorScalaSuiteIT.conf
  val ss: SparkSession = SparkConnectorScalaSuiteIT.ss

  private var _testInfo: TestInfo = _

  @BeforeEach
  def before(testInfo: TestInfo): Unit = {
    _testInfo = testInfo
    use(SparkConnectorScalaSuiteIT.session("system")) { session =>
      session
        .run("CREATE OR REPLACE DATABASE neo4j WAIT 30 seconds").consume()
    }
  }

  def testName: String = _testInfo.getDisplayName

  @AfterEach
  def after(): Unit = {
    ss.catalog.listTables()
      .collect()
      .foreach(t => ss.catalog.dropTempView(t.name))
    ss.catalog.listTables()
      .collect()
      .foreach(t => ss.catalog.dropGlobalTempView(t.name))
  }
}
