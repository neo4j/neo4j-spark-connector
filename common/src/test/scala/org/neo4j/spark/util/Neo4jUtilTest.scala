package org.neo4j.spark.util

import org.junit.{Assert, Test}

import scala.collection.JavaConverters.mapAsJavaMapConverter

class Neo4jUtilTest {

  @Test
  def testSafetyCloseShouldNotFailWithNull(): Unit = {
    Neo4jUtil.closeSafety(null)
  }

  @Test
  def testFlattenMap(): Unit = {
    val input: java.util.Map[String, AnyRef] = Map(
      "foo" -> "bar",
      "key" -> Map(
        "innerKey" -> Map("innnerKey2" -> "value").asJava
      ).asJava
    ).asJava
    val expected = Map(
      "foo" -> "bar",
      "key.innerKey.innnerKey2" -> "value"
    ).asJava
    val actual = Neo4jUtil.flattenMap(input)
    Assert.assertEquals(expected, actual)
  }
}
