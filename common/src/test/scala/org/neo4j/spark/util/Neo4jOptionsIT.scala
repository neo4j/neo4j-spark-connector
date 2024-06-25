package org.neo4j.spark.util

import org.junit.Assert.{assertEquals, assertNotNull}
import org.junit.{Ignore, Test}
import org.neo4j.spark.SparkConnectorScalaSuiteIT
import org.neo4j.spark.SparkConnectorScalaSuiteIT.server

class Neo4jOptionsIT extends SparkConnectorScalaSuiteIT {

  @Test
  def shouldConstructDriver(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, server.getBoltUrl)
    options.put(Neo4jOptions.AUTH_TYPE, "none")

    val neo4jOptions = new Neo4jOptions(options)

    use(neo4jOptions.connection.createDriver()) { driver =>
      assertNotNull(driver)

      use(driver.session()) { session =>
        assertEquals(1, session.run("RETURN 1").single().get(0).asInt())
      }
    }
  }

  @Test
  @Ignore("This requires a fix on driver, ignoring until it is implemented")
  def shouldConstructDriverWithResolver(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, s"neo4j://localhost.localdomain:8888, bolt://localhost.localdomain:9999, ${server.getBoltUrl}")
    options.put(Neo4jOptions.AUTH_TYPE, "none")

    val neo4jOptions = new Neo4jOptions(options)

    use(neo4jOptions.connection.createDriver()) { driver =>
      assertNotNull(driver)

      use(driver.session()) { session =>
        assertEquals(1, session.run("RETURN 1").single().get(0).asInt())
      }
    }
  }

  def use[A <: AutoCloseable, B](resource: A)(code: A ⇒ B): B =
    try
      code(resource)
    finally
      resource.close()

}
