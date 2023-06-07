package org.neo4j.spark.util

import org.junit.{Assert, Test}

import scala.collection.JavaConverters.mapAsJavaMapConverter

class Neo4jUtilTest {

  @Test
  def testSafetyCloseShouldNotFailWithNull(): Unit = {
    Neo4jUtil.closeSafety(null)
  }

}
