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
package org.neo4j.spark.service

import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito
import org.mockito.Mockito.times
import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokenManager
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.spark.util.DriverCache
import org.neo4j.spark.util.Neo4jOptions
import org.testcontainers.shaded.com.google.common.io.BaseEncoding

import java.net.URI
import java.util

class AuthenticationTest {

  @Test
  def testLdapConnectionToken(): Unit = {
    val token = BaseEncoding.base64.encode("user:password".getBytes)
    val options = new util.HashMap[String, String]
    options.put("url", "bolt://localhost:7687")
    options.put("authentication.type", "custom")
    options.put("authentication.custom.credentials", token)
    options.put("labels", "Person")

    stubGraphDatabaseConnectionCallAndAssertToken(options, AuthTokens.custom("", token, "", ""))
  }

  @Test
  def testBearerAuthToken(): Unit = {
    val token = BaseEncoding.base64.encode("user:password".getBytes)
    val options = new util.HashMap[String, String]
    options.put("url", "bolt://localhost:7687")
    options.put("authentication.type", "bearer")
    options.put("authentication.bearer.token", token)

    stubGraphDatabaseConnectionCallAndAssertToken(options, AuthTokens.bearer(token))
  }

  def stubGraphDatabaseConnectionCallAndAssertToken(options: java.util.Map[String, String], token: AuthToken): Unit = {
    val neo4jOptions = new Neo4jOptions(options)
    val neo4jDriverOptions = neo4jOptions.connection
    val driverCache = new DriverCache(neo4jDriverOptions)
    val mockedGraphDatabase = Mockito.mockStatic(classOf[GraphDatabase])
    try {
      mockedGraphDatabase.when(() => GraphDatabase.driver(any[URI](), any[AuthTokenManager](), any[Config]()))
        .thenReturn(Mockito.mock(classOf[Driver]))

      driverCache.getOrCreate()

      val managerCaptor = ArgumentCaptor.forClass(classOf[AuthTokenManager])
      mockedGraphDatabase.verify(
        () => GraphDatabase.driver(any[URI](), managerCaptor.capture(), any[Config]()),
        times(1)
      )
      assert(token == managerCaptor.getValue.getToken.toCompletableFuture.join())
    } finally {
      mockedGraphDatabase.close()
    }
  }
}
