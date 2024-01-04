package org.neo4j.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.hamcrest.Matchers
import org.junit._
import org.junit.rules.{ExpectedException, TestName}
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{Transaction, TransactionWork}
import org.neo4j.spark

import java.util.concurrent.TimeUnit
import scala.annotation.meta.getter

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

  @(Rule@getter)
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
  }

  @After
  def after() {
    if (!TestUtil.isCI()) {
      try {
        spark.Assert.assertEventually(new spark.Assert.ThrowingSupplier[Boolean, Exception] {
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
