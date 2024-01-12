package org.neo4j.spark.util

import org.apache.commons.lang3.StringUtils
import org.junit.Test
import org.junit.Assert

class Neo4jUtilTest {

  @Test
  def testSafetyCloseShouldNotFailWithNull(): Unit = {
    Neo4jUtil.closeSafely(null)
  }

  @Test
  def testConnectorEnv(): Unit = {
    val expected = if (StringUtils.isNotBlank(System.getenv("DATABRICKS_RUNTIME_VERSION"))) {
      "databricks"
    } else {
      "spark"
    }
    val actual = Neo4jUtil.connectorEnv
    Assert.assertEquals(expected, actual)
  }

}
