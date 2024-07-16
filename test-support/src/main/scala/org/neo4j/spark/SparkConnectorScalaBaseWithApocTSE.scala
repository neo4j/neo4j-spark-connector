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

object SparkConnectorScalaBaseWithApocTSE {

  private var startedFromSuite = true

  @BeforeClass
  def setUpContainer() = {
    if (!SparkConnectorScalaSuiteWithApocIT.server.isRunning) {
      startedFromSuite = false
      SparkConnectorScalaSuiteWithApocIT.setUpContainer()
    }
  }

  @AfterClass
  def tearDownContainer() = {
    if (!startedFromSuite) {
      SparkConnectorScalaSuiteWithApocIT.tearDownContainer()
    }
  }

}

class SparkConnectorScalaBaseWithApocTSE {

  val conf: SparkConf = SparkConnectorScalaSuiteWithApocIT.conf
  val ss: SparkSession = SparkConnectorScalaSuiteWithApocIT.ss

  @(Rule @getter)
  val testName: TestName = new TestName

  @Before
  def before() {
    SparkConnectorScalaSuiteWithApocIT.session()
      .writeTransaction(new TransactionWork[ResultSummary] {
        override def execute(tx: Transaction): ResultSummary = tx.run("MATCH (n) DETACH DELETE n").consume()
      })
  }

  @After
  def after() {
    if (!TestUtil.isCI()) {
      try {
        spark.Assert.assertEventually(
          new spark.Assert.ThrowingSupplier[Boolean, Exception] {
            override def get(): Boolean = {
              val afterConnections = SparkConnectorScalaSuiteWithApocIT.getActiveConnections
              SparkConnectorScalaSuiteWithApocIT.connections == afterConnections
            }
          },
          Matchers.equalTo(true),
          45,
          TimeUnit.SECONDS
        )
      } finally {
        val afterConnections = SparkConnectorScalaSuiteWithApocIT.getActiveConnections
        if (SparkConnectorScalaSuiteWithApocIT.connections != afterConnections) { // just for debug purposes
          println(
            s"For test ${testName.getMethodName().replaceAll("$u0020", " ")} => connections before: ${SparkConnectorScalaSuiteWithApocIT.connections}, after: $afterConnections"
          )
        }
      }
    }
  }

}
