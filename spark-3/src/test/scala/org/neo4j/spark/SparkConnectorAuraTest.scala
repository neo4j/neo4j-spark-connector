/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.AfterClass
import org.junit.Assert._
import org.junit.Assume.assumeTrue
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.neo4j.Closeables.use
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
        .set("spark.driver.host", "127.0.0.1"))
      .getOrCreate()

    neo4j = GraphDatabase.driver(url.get, AuthTokens.basic(username.get, password.get))
  }

  @AfterClass
  def tearDown(): Unit = {
    TestUtil.closeSafely(neo4j)
    TestUtil.closeSafely(sparkSession)
  }
}

class SparkConnectorAuraTest {

  val ss: SparkSession = SparkSession.builder().getOrCreate()

  import ss.implicits._

  @Before
  def setUp(): Unit = {
    use(neo4j.session(SessionConfig.forDatabase("system"))) {
      session =>
        session.run("CREATE OR REPLACE DATABASE neo4j WAIT 30 seconds").consume()
    }
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
