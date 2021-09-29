package org.neo4j.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.{AfterClass, Assume, BeforeClass}
import org.neo4j.Neo4jContainerExtension
import org.neo4j.driver._
import org.neo4j.driver.summary.ResultSummary

import java.util.TimeZone


object SparkConnectorScalaSuiteIT {
  val server: Neo4jContainerExtension = new Neo4jContainerExtension()
    .withNeo4jConfig("dbms.security.auth_enabled", "false")
    .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    .withEnv("NEO4J_db_temporal_timezone", TimeZone.getDefault.getID)
    .withDatabases(Seq("db1", "db2"))

  var conf: SparkConf = _
  var ss: SparkSession = _
  var driver: Driver = _

  private var _session: Session = _

  var connections: Long = 0

  @BeforeClass
  def setUpContainer(): Unit = {
    if (!server.isRunning) {
      try {
        server.start()
      } catch {
        case _: Throwable => //
      }
      Assume.assumeTrue("Neo4j container is not started", server.isRunning)
      conf = new SparkConf()
        .setAppName("neoTest")
        .setMaster("local[*]")
        .set("spark.driver.host", "127.0.0.1")
      ss = SparkSession.builder.config(conf).getOrCreate()
      if (TestUtil.isCI()) {
        org.apache.log4j.LogManager.getLogger("org")
          .setLevel(org.apache.log4j.Level.OFF)
      }
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
    TestUtil.closeSafety(session())
    TestUtil.closeSafety(driver)
    TestUtil.closeSafety(server)
    TestUtil.closeSafety(ss)
  }

  def session(): Session = try {
    if (_session == null || !_session.isOpen) {
      _session = driver.session
    }
    _session
  } catch {
    case _: Throwable => null
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

class SparkConnectorScalaSuiteIT {}
