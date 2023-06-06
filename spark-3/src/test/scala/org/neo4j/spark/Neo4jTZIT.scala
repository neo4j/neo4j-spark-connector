package org.neo4j.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.{BeforeClass, Test}
import org.neo4j.Neo4jContainerExtension
import org.neo4j.driver.{AuthTokens, GraphDatabase}
import org.neo4j.spark.Neo4jTZIT.{source, ss, target}

object Neo4jTZIT {
  val source: Neo4jContainerExtension = new Neo4jContainerExtension()
    .withNeo4jConfig("dbms.security.auth_enabled", "false")
    .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    .withEnv("NEO4J_db_temporal_timezone", "+02:00")
    .withDatabases(Seq("db1", "db2"))
  val target: Neo4jContainerExtension = new Neo4jContainerExtension()
    .withNeo4jConfig("dbms.security.auth_enabled", "false")
    .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    .withEnv("NEO4J_db_temporal_timezone", "-02:00")
    .withDatabases(Seq("db1", "db2"))

  var conf: SparkConf = _
  var ss: SparkSession = _

  @BeforeClass
  def setUpContainer(): Unit = {
    source.start()
    target.start()
    conf = new SparkConf()
      .setAppName("neoTest")
      .setMaster("local[*]")
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.sql.session.timeZone", "+01:00")
      .set("spark.sql.datetime.java8API.enabled", "true")
    ss = SparkSession.builder.config(conf).getOrCreate()
    if (TestUtil.isCI()) {
      org.apache.log4j.LogManager.getLogger("org")
        .setLevel(org.apache.log4j.Level.OFF)
    }
  }

}
class Neo4jTZIT {
  val sparkSession: SparkSession = SparkSession.builder().getOrCreate()

  @Test
  def test(): Unit = {
    val driverSource = GraphDatabase.driver(source.getBoltUrl, AuthTokens.none())
    val driverTarget = GraphDatabase.driver(target.getBoltUrl, AuthTokens.none())

    driverSource.session().run("CREATE (p:Person{localdt: localdatetime('2023-06-06T15:30:00')})") // 13:30 -> 14:30
    driverTarget.session().run("CREATE (p:Person{localdt: localdatetime('2023-06-06T15:30:00')})") // 17:30 -> 18:30


    val sourceDF = ss.read.format(classOf[DataSource].getName)
      .option("url", source.getBoltUrl)
      .option("labels", "Person")
      .load()

    val targetDF = ss.read.format(classOf[DataSource].getName)
      .option("url", target.getBoltUrl)
      .option("labels", "Person")
      .load()

    sourceDF.printSchema()
    sourceDF.show(false)
    println("-----")
    targetDF.printSchema()
    targetDF.show(false)
  }

}
