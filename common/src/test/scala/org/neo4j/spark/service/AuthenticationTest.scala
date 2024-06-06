package org.neo4j.spark.service

import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.times
import org.neo4j.driver.{AuthTokens, Config, GraphDatabase}
import org.neo4j.spark.util.{DriverCache, Neo4jOptions}
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner
import org.testcontainers.shaded.com.google.common.io.BaseEncoding

import java.net.URI
import java.util

@PrepareForTest(Array(classOf[GraphDatabase]))
@RunWith(classOf[PowerMockRunner])
class AuthenticationTest {

  @Test
  def testLdapConnectionToken(): Unit = {
    val token = BaseEncoding.base64.encode("user:password".getBytes)
    val options = new util.HashMap[String, String]
    options.put("url", "bolt://localhost:7687")
    options.put("authentication.type", "custom")
    options.put("authentication.custom.credentials", token)
    options.put("labels", "Person")

    val neo4jOptions = new Neo4jOptions(options)
    val neo4jDriverOptions = neo4jOptions.connection
    val driverCache = new DriverCache(neo4jDriverOptions, "jobId")

    PowerMockito.mockStatic(classOf[GraphDatabase])

    driverCache.getOrCreate()

    PowerMockito.verifyStatic(classOf[GraphDatabase], times(1))
    GraphDatabase.driver(any[URI](), ArgumentMatchers.eq(AuthTokens.custom("", token, "", "")) , any(classOf[Config]))
  }

  @Test
  def testBearerAuthToken(): Unit = {
    val token = BaseEncoding.base64.encode("user:password".getBytes)
    val options = new util.HashMap[String, String]
    options.put("url", "bolt://localhost:7687")
    options.put("authentication.type", "bearer")
    options.put("authentication.bearer.token", token)

    val neo4jOptions = new Neo4jOptions(options)
    val neo4jDriverOptions = neo4jOptions.connection
    val driverCache = new DriverCache(neo4jDriverOptions, "jobId")

    PowerMockito.mockStatic(classOf[GraphDatabase])

    driverCache.getOrCreate()

    PowerMockito.verifyStatic(classOf[GraphDatabase], times(1))
    GraphDatabase.driver(any[URI](), ArgumentMatchers.eq(AuthTokens.bearer(token)), any())
  }
}
