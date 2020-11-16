package org.neo4j.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.junit.runners.Suite
import org.junit.{AfterClass, Assume, BeforeClass}
import org.neo4j.Neo4jContainerExtension
import org.neo4j.driver._
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.spark.service.SchemaServiceWithApocTSE
import org.neo4j.spark.util.Neo4jUtil


object SparkConnectorScalaSuiteWithApocIT {
  val server: Neo4jContainerExtension = new Neo4jContainerExtension()
    .withNeo4jConfig("dbms.security.auth_enabled", "false")
    .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    .withEnv("NEO4JLABS_PLUGINS", "[\"apoc\"]")
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
        case _ => //
      }
      Assume.assumeTrue("Neo4j container is not started", server.isRunning)
      conf = new SparkConf().setAppName("neoTest")
        .setMaster("local[*]")
      ss = SparkSession.builder.config(conf).getOrCreate()
      if (TestUtil.isTravis()) {
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
    if (server.isRunning) {
      Neo4jUtil.closeSafety(session())
      Neo4jUtil.closeSafety(driver)
      server.stop()
      ss.stop()
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
@Suite.SuiteClasses(Array(
  classOf[SchemaServiceWithApocTSE],
  classOf[DataSourceReaderWithApocTSE],
  classOf[DataSourceReaderNeo4j4xWithApocTSE]
))
class SparkConnectorScalaSuiteWithApocIT {}