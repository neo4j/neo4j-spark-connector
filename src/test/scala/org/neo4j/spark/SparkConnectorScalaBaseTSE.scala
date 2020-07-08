package org.neo4j.spark

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, SparkContext}
import org.hamcrest.Matchers
import org.junit._
import org.junit.rules.TestName
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{Transaction, TransactionWork}

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
  val sc: SparkContext = SparkConnectorScalaSuiteIT.sc

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
    if (!Option(System.getenv("TRAVIS")).getOrElse("false").toBoolean) {
      try {
        utils.Assert.assertEventually(new utils.Assert.ThrowingSupplier[Boolean, Exception] {
          override def get(): Boolean = {
            val afterConnections = SparkConnectorScalaSuiteIT.getActiveConnections
            SparkConnectorScalaSuiteIT.connections == afterConnections
          }
        }, Matchers.equalTo(true), 60, TimeUnit.SECONDS)
      } finally {
        val afterConnections = SparkConnectorScalaSuiteIT.getActiveConnections
        if (SparkConnectorScalaSuiteIT.connections != afterConnections) { // just for debug purposes
          println(s"For test ${testName.getMethodName} => connections before: ${SparkConnectorScalaSuiteIT.connections}, after: $afterConnections")
        }
      }
    }
  }

}
