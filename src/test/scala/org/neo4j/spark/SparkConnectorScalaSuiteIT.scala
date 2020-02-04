package org.neo4j.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{AfterClass, Assume, BeforeClass}
import org.junit.runner.RunWith
import org.junit.runners.Suite
import org.neo4j.Neo4jContainerExtension


object SparkConnectorScalaSuiteIT {
  val server: Neo4jContainerExtension = new Neo4jContainerExtension("neo4j:4.0.0-enterprise")
    .withNeo4jConfig("dbms.security.auth_enabled", "false")
    .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")

  var conf: SparkConf = _
  var sc: SparkContext = _

  @BeforeClass
  def setUpContainer() = {
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
        .set("spark.neo4j.bolt.url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      sc = SparkContext.getOrCreate(conf)
    }
  }

  @AfterClass
  def tearDownContainer() = {
    if (server.isRunning) {
      server.stop()
      sc.stop()
    }
  }
}

@RunWith(classOf[Suite])
@Suite.SuiteClasses(Array(classOf[Neo4jDataFrameScalaTSE],
  classOf[Neo4jGraphScalaTSE],
  classOf[Neo4jSparkTSE]))
class SparkConnectorScalaSuiteIT {}
