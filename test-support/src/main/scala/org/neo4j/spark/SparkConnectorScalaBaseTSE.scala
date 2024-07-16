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
import org.junit._
import org.junit.rules.TestName
import org.neo4j.driver.Transaction
import org.neo4j.driver.TransactionWork
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.spark

import java.util.concurrent.TimeUnit

import scala.annotation.meta.getter
import scala.collection.JavaConverters.asScalaBufferConverter

object SparkConnectorScalaBaseTSE {

  private var startedFromSuite = true

  @BeforeClass
  def setUpContainer() = {
    if (!SparkConnectorScalaSuiteIT.server.isRunning) {
      startedFromSuite = false
      SparkConnectorScalaSuiteIT.setUpContainer()
    }
  }

  @AfterClass
  def tearDownContainer() = {
    if (!startedFromSuite) {
      SparkConnectorScalaSuiteIT.tearDownContainer()
    }
  }

}

class SparkConnectorScalaBaseTSE {

  val conf: SparkConf = SparkConnectorScalaSuiteIT.conf
  val ss: SparkSession = SparkConnectorScalaSuiteIT.ss

  @(Rule @getter)
  val testName: TestName = new TestName

  @Before
  def before() {
    ss.catalog.listTables()
      .collect()
      .foreach(t => ss.catalog.dropTempView(t.name))
    ss.catalog.listTables()
      .collect()
      .foreach(t => ss.catalog.dropGlobalTempView(t.name))
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(new TransactionWork[ResultSummary] {
        override def execute(tx: Transaction): ResultSummary = tx.run("MATCH (n) DETACH DELETE n").consume()
      })
    SparkConnectorScalaSuiteIT.session()
      .readTransaction(tx =>
        tx.run("SHOW CONSTRAINTS YIELD name RETURN name")
          .list()
          .asScala
          .map(_.get("name").asString())
      )
      .foreach(constraint =>
        SparkConnectorScalaSuiteIT.session()
          .writeTransaction(tx => tx.run(s"DROP CONSTRAINT `$constraint`").consume())
      )
    SparkConnectorScalaSuiteIT.session()
      .readTransaction(tx =>
        tx.run("SHOW INDEXES YIELD name RETURN name")
          .list()
          .asScala
          .map(_.get("name").asString())
      )
      .foreach(constraint =>
        SparkConnectorScalaSuiteIT.session()
          .writeTransaction(tx => tx.run(s"DROP INDEX `$constraint`").consume())
      )
  }

  @After
  def after() {
    if (!TestUtil.isCI()) {
      try {
        spark.Assert.assertEventually(
          new spark.Assert.ThrowingSupplier[Boolean, Exception] {
            override def get(): Boolean = {
              val afterConnections = SparkConnectorScalaSuiteIT.getActiveConnections
              SparkConnectorScalaSuiteIT.connections == afterConnections
            }
          },
          Matchers.equalTo(true),
          45,
          TimeUnit.SECONDS
        )
      } finally {
        val afterConnections = SparkConnectorScalaSuiteIT.getActiveConnections
        if (SparkConnectorScalaSuiteIT.connections != afterConnections) { // just for debug purposes
          println(
            s"For test ${testName.getMethodName().replaceAll("$u0020", " ")} => connections before: ${SparkConnectorScalaSuiteIT.connections}, after: $afterConnections"
          )
        }
      }
    }
  }

}
