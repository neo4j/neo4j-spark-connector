package org.neo4j.spark

import org.apache.spark.SparkConf
import org.junit.{Assert, Test}

class Neo4jConfigTest {

  @Test
  def testParams(): Unit = {
    // given
    val encryption = "true"
    val user = "neo4j"
    val pass = "pass"
    val url = "neo4j://localhost"
    val sparkConf = new SparkConf()
      .set("spark.neo4j.bolt.encryption", encryption)
      .set("spark.neo4j.bolt.user", user)
      .set("spark.neo4j.bolt.password", pass)
      .set("spark.neo4j.bolt.url", url)

    // when
    val neo4jConf = Neo4jConfig(sparkConf)

    // then
    Assert.assertEquals(encryption.toBoolean, neo4jConf.encryption)
    Assert.assertEquals(user, neo4jConf.user)
    Assert.assertEquals(pass, neo4jConf.password.get)
    Assert.assertEquals(url, neo4jConf.url)
  }

}
