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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import org.junit.jupiter.params.support.ParameterDeclarations
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.same
import org.mockito.ArgumentMatchers.{eq => eqTo}
import org.mockito.Mockito
import org.mockito.Mockito.times
import org.neo4j.connectors.driver.auth.AuthConfig
import org.neo4j.connectors.driver.auth.AuthTokenManagerRegistry
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
import java.util.stream

object AuthenticationModeCaseProvider {

  case class Test(testName: String, token: AuthToken, options: Map[String, String]) {
    override def toString: String = testName
  }
}

class AuthenticationModeCaseProvider extends ArgumentsProvider {
  private[this] val TOKEN_STRING = BaseEncoding.base64.encode("user:password".getBytes)

  override def provideArguments(
    parameters: ParameterDeclarations,
    context: ExtensionContext
  ): stream.Stream[_ <: Arguments] = {
    val cases = List(
      (
        "basic",
        AuthTokens.basic("user", "pass"),
        Map(
          "url" -> "bolt://localhost:7687",
          "authentication.type" -> "basic",
          "authentication.basic.username" -> "user",
          "authentication.basic.password" -> "pass"
        )
      ),
      (
        "bearer",
        AuthTokens.bearer(TOKEN_STRING),
        Map(
          "url" -> "bolt://localhost:7687",
          "authentication.type" -> "bearer",
          "authentication.bearer.token" -> TOKEN_STRING
        )
      ),
      (
        "custom",
        AuthTokens.custom("", TOKEN_STRING, "", ""),
        Map(
          "url" -> "bolt://localhost:7687",
          "authentication.type" -> "custom",
          "authentication.custom.credentials" -> TOKEN_STRING
        )
      ),
      (
        "kerberos",
        AuthTokens.kerberos(TOKEN_STRING),
        Map(
          "url" -> "bolt://localhost:7687",
          "authentication.type" -> "kerberos",
          "authentication.kerberos.ticket" -> TOKEN_STRING
        )
      )
    )

    val testArguments = cases.map(c => Arguments.of(AuthenticationModeCaseProvider.Test(c._1, c._2, c._3))).toArray
    java.util.stream.Stream.of(testArguments: _*)
  }
}

class AuthenticationTest {

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(classOf[AuthenticationModeCaseProvider])
  def should_officially_support_our_provided_authn_managers(testCase: AuthenticationModeCaseProvider.Test): Unit = {
    val mockedDriverConnection = Mockito.mockStatic(classOf[GraphDatabase])

    mockedDriverConnection
      .when(() => GraphDatabase.driver(any[URI], any[AuthTokenManager](), any[Config]))
      .thenReturn(Mockito.mock(classOf[Driver]))

    val (cache, tokenSpy) = cacheWithTokenSpy(testCase.options)

    try {
      cache.getOrCreate()

      mockedDriverConnection.verify(
        () => GraphDatabase.driver(any[URI](), tokenSpy.capture(), any[Config]()),
        times(1)
      )
    } finally {
      cache.close()
      mockedDriverConnection.close()
    }

    assertThat(tokenSpy.token).isEqualTo(testCase.token)
  }

  @Test
  def should_create_driver_with_custom_provided_auth_supplier_with_provided_auth_options(): Unit = {
    val authMethod = "keycloak"
    val registry = Mockito.mock(classOf[AuthTokenManagerRegistry])
    val tokenManager = Mockito.mock(classOf[AuthTokenManager])
    val configCaptor = ArgumentCaptor.forClass(classOf[AuthConfig])

    val mockedRegistryLookup = Mockito.mockStatic(classOf[AuthTokenManagerRegistry])
    val mockedDriverConnection = Mockito.mockStatic(classOf[GraphDatabase])

    val options = Map(
      "url" -> "bolt://localhost:7687",
      "authentication.type" -> authMethod,
      "authentication.keycloak.username" -> "user",
      "authentication.keycloak.password" -> "pass",
      "authentication.keycloak.authServerUrl" -> "www.example.com",
      "authentication.keycloak.realm" -> "test",
      "authentication.keycloak.clientId" -> "abc123",
      "authentication.keycloak.clientSecret" -> "super-secret"
    )

    mockedRegistryLookup
      .when[AuthTokenManagerRegistry](() => AuthTokenManagerRegistry.usingDefaultClassLoader())
      .thenReturn(registry)

    Mockito.when(registry.create(eqTo(authMethod), any[AuthConfig]())).thenReturn(tokenManager)

    mockedDriverConnection.when[Driver](() =>
      GraphDatabase.driver(any[URI](), any[AuthTokenManager](), any[Config]())
    ).thenReturn(Mockito.mock(classOf[Driver]))

    val cache = new DriverCache(new Neo4jOptions(options).connection)

    try {
      cache.getOrCreate()

      Mockito.verify(registry, times(1))
        .create(eqTo("keycloak"), configCaptor.capture())

      val config = configCaptor.getValue

      assertThat(config.username()).contains("user")
      assertThat(config.password()).contains("pass")
      assertThat(config.asMap())
        .containsEntry("authServerUrl", "www.example.com")
        .containsEntry("realm", "test")
        .containsEntry("clientId", "abc123")
        .containsEntry("clientSecret", "super-secret")

      mockedDriverConnection.verify(
        () =>
          GraphDatabase.driver(
            any[URI](),
            same(tokenManager),
            any[Config]()
          ),
        times(1)
      )
    } finally {
      cache.close()
      mockedDriverConnection.close()
      mockedRegistryLookup.close()
    }
  }

  private def cacheWithTokenSpy(options: Map[String, String]): (DriverCache, ArgumentCaptor[AuthTokenManager]) = {
    (new DriverCache(new Neo4jOptions(options).connection), ArgumentCaptor.forClass(classOf[AuthTokenManager]))
  }

  implicit private class AuthTokenManagerCaptorOperations(
    private val captor: ArgumentCaptor[AuthTokenManager]
  ) {
    def token: AuthToken = captor.getValue.getToken.toCompletableFuture.join()
  }
}
