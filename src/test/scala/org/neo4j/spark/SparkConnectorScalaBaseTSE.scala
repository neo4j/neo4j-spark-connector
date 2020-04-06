package org.neo4j.spark

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, SparkContext}
import org.hamcrest.Matchers
import org.junit._
import org.junit.rules.TestName
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{Transaction, TransactionWork}
import org.neo4j.function.ThrowingSupplier
import org.neo4j.test.assertion

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
    assertion.Assert.assertEventually(new ThrowingSupplier[Boolean, Exception] {
      override def get(): Boolean = {
        val afterConnections = SparkConnectorScalaSuiteIT.getActiveConnections
        println(s"For test ${testName.getMethodName} => connections before: ${SparkConnectorScalaSuiteIT.connections}, after: $afterConnections")
        SparkConnectorScalaSuiteIT.connections == afterConnections
      }
    }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
  }

}
