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

package org.neo4j.spark.cypher

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatObject
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.argumentSet
import org.junit.jupiter.params.provider.MethodSource
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.Cypher.asterisk
import org.neo4j.cypherdsl.core.Cypher.callRawCypher
import org.neo4j.driver.Driver
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.spark.testsupport.Neo4jContainerProvider.ADMIN_PASSWORD
import org.neo4j.spark.testsupport.Neo4jExtensions.Neo4jContainerExtensions
import org.neo4j.spark.testsupport.TestUtil
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.neo4j.Neo4jContainer

import java.util.stream.Stream

import scala.jdk.CollectionConverters.MapHasAsJava

@Testcontainers
@TestInstance(PER_CLASS)
class AutomaticAliaserIT {

  @Container
  private val container = new Neo4jContainer(TestUtil.neo4jImage())
    .withAdminPassword(ADMIN_PASSWORD)
    .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")

  private var driver: Option[Driver] = None

  private val aliaser = new AutomaticAliaser()

  @BeforeEach
  def prepare(): Unit = {
    val newDriver = container.driver()
    newDriver.verifyConnectivity()
    driver = Option(newDriver)
  }

  @AfterEach
  def cleanUp(): Unit = {
    driver.foreach(_.close())
  }

  @ParameterizedTest
  @MethodSource(Array("alias_cases"))
  def aliases_query_for_subquery_embedding(subquery: String, params: Map[String, AnyRef]): Unit = {
    assertThat(driver.isDefined).isTrue
    val renderer = container.cypherRenderer()
    val unaliasedQueryStatement = callRawCypher(subquery).returning(asterisk()).build()
    // see https://neo4j.com/docs/status-codes/current/errors/gql-errors/42N21/
    verifyQueryFailsWithAliasingError(renderer.render(unaliasedQueryStatement))

    val aliasedQueryStatement = callRawCypher(aliaser.aliasResults(subquery)).returning(asterisk()).build()

    assertThatCode(() =>
      driver.get.executableQuery(renderer.render(aliasedQueryStatement))
        .withParameters(params.asJava)
        .execute()
    )
      .doesNotThrowAnyException()
  }

  private def alias_cases(): Stream[Arguments] = {
    {
      Stream.of(
        argumentSet(
          "unaliased number literal",
          "RETURN 42",
          Map[String, AnyRef]()
        ),
        argumentSet(
          "unaliased boolean literal",
          "RETURN false",
          Map[String, AnyRef]()
        ),
        argumentSet(
          "unaliased string literal",
          "RETURN 'foo'",
          Map[String, AnyRef]()
        ),
        argumentSet(
          "unaliased array literal",
          "RETURN ['foo']",
          Map[String, AnyRef]()
        ),
        argumentSet(
          "unaliased map literal",
          "RETURN {foo: 'bar'}",
          Map[String, AnyRef]()
        ),
        argumentSet(
          "unaliased parameter",
          "RETURN $foo",
          Map[String, AnyRef]("foo" -> "")
        ),
        argumentSet(
          "unaliased function call",
          "UNWIND [1, 2, 3] AS x RETURN avg(x)",
          Map[String, AnyRef]()
        ),
        argumentSet(
          "unaliased node property",
          "MATCH (n:Person) RETURN n.bar",
          Map[String, AnyRef]()
        ),
        argumentSet(
          "unaliased relationship property",
          "MATCH ()-[r:LINKS]->() RETURN r.bar",
          Map[String, AnyRef]()
        ),
        argumentSet(
          "unaliased field access",
          "UNWIND [{foo: 'bar'}] AS object RETURN object.foo",
          Map[String, AnyRef]()
        ),
        argumentSet(
          "unaliased indexed array access",
          "UNWIND [['foo']] AS array RETURN array[0]",
          Map[String, AnyRef]()
        )
      )
    }

  }

  private def verifyQueryFailsWithAliasingError(query: String): Unit = {
    assertThatThrownBy(() => driver.get.executableQuery(query).execute())
      .isInstanceOfSatisfying(
        classOf[ClientException],
        (e: ClientException) =>
          assertThatObject(e.gqlStatus() == "42N21" || Option(e.getMessage).exists(_.contains("must be aliased")))
            .overridingErrorMessage(
              "expected query to fail with GQL status <42N21> or message containing <must be aliased>, " +
                "but got GQL status <%s> and message <%s>",
              e.gqlStatus(),
              e.getMessage
            )
            .isEqualTo(true)
      )
  }
}
