package org.neo4j.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.hamcrest.Matchers
import org.junit._
import org.junit.rules.TestName
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{Transaction, TransactionWork}
import org.neo4j.spark

import java.util.concurrent.TimeUnit

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

  val _testName: TestName = new TestName

  @Rule
  def testName = _testName

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
        spark.Assert.assertEventually(new spark.Assert.ThrowingSupplier[Boolean, Exception] {
          override def get(): Boolean = {
            val afterConnections = SparkConnectorScalaSuiteWithApocIT.getActiveConnections
            SparkConnectorScalaSuiteWithApocIT.connections == afterConnections
          }
        }, Matchers.equalTo(true), 45, TimeUnit.SECONDS)
      } finally {
        val afterConnections = SparkConnectorScalaSuiteWithApocIT.getActiveConnections
        if (SparkConnectorScalaSuiteWithApocIT.connections != afterConnections) { // just for debug purposes
          println(s"For test ${testName.getMethodName().replaceAll("$u0020", " ")} => connections before: ${SparkConnectorScalaSuiteWithApocIT.connections}, after: $afterConnections")
        }
      }
    }
  }

}
