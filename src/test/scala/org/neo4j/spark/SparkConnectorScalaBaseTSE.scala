package org.neo4j.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.junit._
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Session}


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

  private var driver: Driver = _

  def session(): Session = {
    if (driver == null) {
      driver = GraphDatabase.driver(SparkConnectorScalaSuiteIT.server.getBoltUrl, AuthTokens.none())
    }
    driver.session
  }

  @Before
  @throws[Exception]
  def start() {
    session().run("MATCH (n) DETACH DELETE n").consume()
  }

}
