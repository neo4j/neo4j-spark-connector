package org.neo4j.spark

import org.apache.spark.{SparkConf, SparkContext}
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
    val afterConnections = SparkConnectorScalaSuiteIT.getActiveConnections
    println(s"Connections before: ${SparkConnectorScalaSuiteIT.connections}, after: $afterConnections")
    Assert.assertEquals(SparkConnectorScalaSuiteIT.connections, afterConnections) // we use it to track the connection handling
  }

}
