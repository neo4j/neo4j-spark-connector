package org.neo4j.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.hamcrest.Matchers
import org.junit._
import org.junit.rules.TestName
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{Transaction, TransactionWork}

import java.util.concurrent.TimeUnit

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

  val _testName: TestName = new TestName

  @Rule
  def testName = _testName

  @Before
  def before() {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(new TransactionWork[ResultSummary] {
        override def execute(tx: Transaction): ResultSummary = tx.run("MATCH (n) DETACH DELETE n").consume()
      })
  }

  @After
  def after() {
    if (!TestUtil.isCI()) {
      try {
        Assert.assertEventually(new Assert.ThrowingSupplier[Boolean, Exception] {
          override def get(): Boolean = {
            val afterConnections = SparkConnectorScalaSuiteIT.getActiveConnections
            SparkConnectorScalaSuiteIT.connections == afterConnections
          }
        }, Matchers.equalTo(true), 45, TimeUnit.SECONDS)
      } finally {
        val afterConnections = SparkConnectorScalaSuiteIT.getActiveConnections
        if (SparkConnectorScalaSuiteIT.connections != afterConnections) { // just for debug purposes
          println(s"For test ${testName.getMethodName().replaceAll("$u0020", " ")} => connections before: ${SparkConnectorScalaSuiteIT.connections}, after: $afterConnections")
        }
      }
    }
  }

}
