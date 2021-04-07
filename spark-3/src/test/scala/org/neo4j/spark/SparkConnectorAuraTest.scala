package org.neo4j.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Assume.assumeTrue
import org.junit.{AfterClass, Before, BeforeClass, Test}
import org.neo4j.driver._
import org.neo4j.spark.SparkConnectorAuraTest._

object SparkConnectorAuraTest {
  private var neo4j: Driver = _
  private val username: Option[String] = Option[String](System.getenv("AURA_USER"))
  private val password: Option[String] = Option[String](System.getenv("AURA_PASSWORD"))
  private val url: Option[String] = Option[String](System.getenv("AURA_URI"))

  var sparkSession: SparkSession = _

  @BeforeClass
  def setUpClass(): Unit = {
    assumeTrue(username.isDefined)
    assumeTrue(password.isDefined)
    assumeTrue(url.isDefined)

    sparkSession = SparkSession.builder()
      .config(new SparkConf()
        .setAppName("neoTest")
        .setMaster("local[*]")
        .set("spark.driver.host", "127.0.0.1")
      )
      .getOrCreate()

    neo4j = GraphDatabase.driver(url.get, AuthTokens.basic(username.get, password.get))
  }

  @AfterClass
  def tearDown(): Unit = {
    TestUtil.closeSafety(neo4j)
    TestUtil.closeSafety(sparkSession)
  }
}

class SparkConnectorAuraTest {

  val ss: SparkSession = SparkSession.builder().getOrCreate()

  import ss.implicits._

  @Before
  def setUp() {
    val session = neo4j.session()

    session.writeTransaction(new TransactionWork[Result] {
      override def execute(transaction: Transaction): Result = transaction.run("MATCH (n) DETACH DELETE n")
    })

    session.close()
  }

  @Test
  def shouldWriteToAndReadFromAura(): Unit = {
    val df = Seq(("John Bonham", "Drums", 12), ("John Mayer", "Guitar", 8))
      .toDF("name", "instrument", "experience")

    df.write
      .mode("Overwrite")
      .format(classOf[DataSource].getName)
      .option("url", url.get)
      .option("authentication.type", "basic")
      .option("authentication.basic.username", username.get)
      .option("authentication.basic.password", password.get)
      .option("relationship", "PLAYS")
      .option("relationship.source.save.mode", "Append")
      .option("relationship.target.save.mode", "Append")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.node.keys", "name:name")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.keys", "instrument:name")
      .save()

    val results = sparkSession.read.format(classOf[DataSource].getName)
      .option("url", url.get)
      .option("authentication.type", "basic")
      .option("authentication.basic.username", username.get)
      .option("authentication.basic.password", password.get)
      .option("labels", "Musician")
      .load()
      .collectAsList()

    assertEquals(2, results.size())
  }
}
