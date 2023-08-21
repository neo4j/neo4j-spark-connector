package org.neo4j.spark.service

import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.neo4j.driver.AuthToken
import org.neo4j.driver.Config
import org.neo4j.driver.GraphDatabase
import org.neo4j.spark.util.DriverCache
import org.neo4j.spark.util.Neo4jOptions
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner
import org.testcontainers.shaded.com.google.common.io.BaseEncoding

import java.util
import org.junit.Assert.assertEquals
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.times

@PrepareForTest(Array(classOf[GraphDatabase]))
@RunWith(classOf[PowerMockRunner])
class AuthenticationTest {

  @Test
  def testLdapConnectionToken(): Unit = {
    val options = new util.HashMap[String, String]
    options.put("url", "bolt://localhost:7687")
    options.put("authentication.type", "custom")
    options.put("authentication.custom.credentials", BaseEncoding.base64.encode("user:password".getBytes))
    options.put("labels", "Person")

    val argumentCaptor = ArgumentCaptor.forClass(classOf[AuthToken])
    val neo4jOptions = new Neo4jOptions(options)
    val neo4jDriverOptions = neo4jOptions.connection
    val driverCache = new DriverCache(neo4jDriverOptions, "jobId")

    PowerMockito.mockStatic(classOf[GraphDatabase])

    driverCache.getOrCreate()

    PowerMockito.verifyStatic(classOf[GraphDatabase], times(1))
    GraphDatabase.driver(anyString, argumentCaptor.capture, any(classOf[Config]))

    assertEquals(neo4jDriverOptions.toNeo4jAuth, argumentCaptor.getValue)
  }

  @Test
  def testBearerAuthToken(): Unit = {
    val options = new util.HashMap[String, String]
    options.put("url", "bolt://localhost:7687")
    options.put("authentication.type", "bearer")
    options.put("authentication.bearer.token", BaseEncoding.base64.encode("user:password".getBytes))

    val argumentCaptor = ArgumentCaptor.forClass(classOf[AuthToken])
    val neo4jOptions = new Neo4jOptions(options)
    val neo4jDriverOptions = neo4jOptions.connection
    val driverCache = new DriverCache(neo4jDriverOptions, "jobId")

    PowerMockito.mockStatic(classOf[GraphDatabase])

    driverCache.getOrCreate()

    PowerMockito.verifyStatic(classOf[GraphDatabase], times(1))
    GraphDatabase.driver(anyString, argumentCaptor.capture, any(classOf[Config]))

    assertEquals(neo4jDriverOptions.toNeo4jAuth, argumentCaptor.getValue)
  }
}
