package org.neo4j.spark.util

import org.junit.{Assert, Test}

import scala.collection.JavaConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}

class Neo4jUtilTest {

  @Test
  def testSafetyCloseShouldNotFailWithNull(): Unit = {
    Neo4jUtil.closeSafety(null)
  }

  @Test
  def testCreateGdsConfigurationMap(): Unit = {
    val actual = Neo4jUtil.createGdsConfigurationMap(Map(
      "graphName" -> "foo",
      "configuration.number" -> "1",
      "configuration.string" -> "foo",
      "configuration.list" -> "['a', 1]",
      "configuration.map.key" -> "value",
    ))
    val expected: java.util.Map[String, Object] = Map(
      "graphName" -> "foo",
      "configuration" -> Map(
        "number" -> 1,
        "string" -> "foo",
        "list" -> Seq("a", 1).toList.asJava,
        "map" -> Map(
          "key" -> "value"
        ).asJava
      ).asJava
    ).asJava
    Assert.assertEquals(expected, actual)

    val ucActual = Neo4jUtil.createGdsConfigurationMap(Map(
      "graphName" -> "myGraph",
      "nodeProjection" -> "Website",
      "relationshipProjection.LINK.indexInverse" -> "true",
    ))
    val ucExpected: java.util.Map[String, Object] = Map(
      "graphName" -> "myGraph",
      "nodeProjection" -> "Website",
      "relationshipProjection" -> Map(
        "LINK" -> Map(
          "indexInverse" -> true
        ).asJava
      ).asJava
    ).asJava
    Assert.assertEquals(ucExpected, ucActual)
  }

}
