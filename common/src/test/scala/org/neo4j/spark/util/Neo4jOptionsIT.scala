package org.neo4j.spark.util

import org.junit.Assert.{assertEquals, assertNotNull}
import org.junit.Test
import org.neo4j.spark.SparkConnectorScalaSuiteIT
import org.neo4j.spark.SparkConnectorScalaSuiteIT.server

import scala.util.Using

class Neo4jOptionsIT extends SparkConnectorScalaSuiteIT {

  @Test
  def shouldConstructDriver(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, server.getBoltUrl)
    options.put(Neo4jOptions.AUTH_TYPE, "none")

    val neo4jOptions = new Neo4jOptions(options)

    Using.Manager { use =>
      val driver = use(neo4jOptions.connection.createDriver())
      assertNotNull(driver)

      val session = use(driver.session())
      assertEquals(1, session.run("RETURN 1").single().get(0).asInt())
    }
  }

  @Test
  def shouldConstructDriverWithResolver(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, s"neo4j://localhost.localdomain:8888, bolt://localhost.localdomain:9999, ${server.getBoltUrl}")
    options.put(Neo4jOptions.AUTH_TYPE, "none")

    val neo4jOptions = new Neo4jOptions(options)

    Using.Manager { use =>
      val driver = use(neo4jOptions.connection.createDriver())
      assertNotNull(driver)

      val session = use(driver.session())
      assertEquals(1, session.run("RETURN 1").single().get(0).asInt())
    }
  }

}
