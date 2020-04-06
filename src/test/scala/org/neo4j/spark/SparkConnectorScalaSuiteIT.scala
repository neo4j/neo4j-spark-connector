package org.neo4j.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.junit.runners.{MethodSorters, Suite}
import org.junit.{AfterClass, Assume, BeforeClass, FixMethodOrder}
import org.neo4j.Neo4jContainerExtension
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Session, Transaction, TransactionWork}
import org.neo4j.spark.utils.Neo4jUtils


object SparkConnectorScalaSuiteIT {
  val server: Neo4jContainerExtension = new Neo4jContainerExtension("neo4j:4.0.1-enterprise")
    .withNeo4jConfig("dbms.security.auth_enabled", "false")
    .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")

  var conf: SparkConf = _
  var sc: SparkContext = _

  private var driver: Driver = _
  private var _session: Session = _

  var connections: Long = 0

  @BeforeClass
  def setUpContainer(): Unit = {
    if (!server.isRunning) {
      try {
        server.start()
      } catch {
        case _ => //
      }
      Assume.assumeTrue("Neo4j container is not started", server.isRunning)
      conf = new SparkConf().setAppName("neoTest")
        .setMaster("local[*]")
        .set("spark.driver.allowMultipleContexts", "true")
        .set("spark.neo4j.url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      sc = SparkContext.getOrCreate(conf)
      driver = GraphDatabase.driver(server.getBoltUrl, AuthTokens.none())
      session()
        .readTransaction(new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run("RETURN 1").consume() // we init the session so the count is consistent
        })
      connections = getActiveConnections
      Unit
    }
  }

  @AfterClass
  def tearDownContainer() = {
    if (server.isRunning) {
      Neo4jUtils.close(driver, session)
      server.stop()
      sc.stop()
    }
  }

  def session(): Session = {
    if (_session == null || !_session.isOpen) {
      _session = driver.session
    }
    _session
  }

  def getActiveConnections = session()
    .readTransaction(new TransactionWork[Long] {
      override def execute(tx: Transaction): Long = tx.run(
        """|CALL dbms.listConnections() YIELD connectionId, connector
           |WHERE connector = 'bolt'
           |RETURN count(*) AS connections""".stripMargin)
        .single()
        .get("connections")
        .asLong()
    })
}

@RunWith(classOf[Suite])
@Suite.SuiteClasses(Array(classOf[Neo4jDataFrameScalaTSE],
  classOf[Neo4jGraphScalaTSE],
  classOf[Neo4jSparkTSE]))
class SparkConnectorScalaSuiteIT {}
