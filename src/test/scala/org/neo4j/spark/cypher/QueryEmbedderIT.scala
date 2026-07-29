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
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.argumentSet
import org.junit.jupiter.params.provider.MethodSource
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
import scala.jdk.CollectionConverters.SeqHasAsJava

@Testcontainers
@TestInstance(PER_CLASS)
class QueryEmbedderIT {

  @Container
  private val container = new Neo4jContainer(TestUtil.neo4jImage())
    .withAdminPassword(ADMIN_PASSWORD)
    .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")

  private var driver: Option[Driver] = None

  private val embedder = new QueryEmbedder()

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

  @Test
  def aliases_query_before_embedding_as_call_subquery(): Unit = {
    assertThat(driver.isDefined).isTrue
    val renderer = container.cypherRenderer()
    val query = "MATCH (o:Object) RETURN 42, false, o.name, o.brother"
    val unaliasedQueryStatement = callRawCypher(query).returning(asterisk()).build()
    verifyQueryFailsWithAliasingError(renderer.render(unaliasedQueryStatement))

    val embeddedQuery = embedder.embedRead(query, "").build()

    assertThat(embeddedQuery.getCypher).isEqualTo(
      "CALL () {MATCH (o:Object) RETURN 42 AS `42`, false AS false, o.name AS `o.name`, o.brother AS `o.brother`} RETURN `42`, false, `o.name`, `o.brother`"
    )
    assertThatCode(() => driver.get.executableQuery(renderer.render(embeddedQuery)).execute())
      .doesNotThrowAnyException()
  }

  @ParameterizedTest
  @MethodSource(Array("embed_read_cases"))
  def embeds_query_as_call_subquery(
    query: String,
    scriptResult: String,
    params: Map[String, AnyRef],
    expectedEmbeddedQuery: String
  ): Unit = {
    assertThat(driver.isDefined).isTrue

    val embeddedQuery = embedder.embedRead(query, scriptResult).build()

    val renderer = container.cypherRenderer()
    assertThat(embeddedQuery.getCypher).isEqualTo(expectedEmbeddedQuery)
    assertThatCode(() =>
      driver.get.executableQuery(renderer.render(embeddedQuery))
        .withParameters(params.asJava)
        .execute()
    )
      .doesNotThrowAnyException()
  }

  private def embed_read_cases(): Stream[Arguments] =
    Stream.of(
      argumentSet(
        "preserves return all",
        "MATCH (o:Object) RETURN *",
        "",
        Map[String, AnyRef](),
        "CALL () {MATCH (o:Object) RETURN *} RETURN *"
      ),
      argumentSet(
        "embeds with script result",
        "MATCH (o:Object) RETURN o",
        "WITH $scriptResult AS scriptResult ",
        Map[String, AnyRef]("scriptResult" -> ""),
        "CALL () {WITH $scriptResult AS scriptResult MATCH (o:Object) RETURN o} RETURN o"
      ),
      argumentSet(
        "keeps ordering through nested call",
        "MATCH (n) CALL (n) { RETURN 1 AS a, 2 AS b } RETURN b AS x, a AS y",
        "",
        Map[String, AnyRef](),
        "CALL () {MATCH (n) CALL (*) {RETURN 1 AS a, 2 AS b} RETURN b AS x, a AS y} RETURN x, y"
      ),
      argumentSet(
        "preserves order after asterisk",
        "WITH 42 as y RETURN *, y AS x",
        "",
        Map[String, AnyRef](),
        "CALL () {WITH 42 AS y RETURN *, y AS x} RETURN *, x"
      )
    )

  @ParameterizedTest
  @MethodSource(Array("embed_write_cases"))
  def embeds_write_query_as_call_subquery(
    query: String,
    scriptResult: String,
    params: Map[String, AnyRef],
    expectedEmbeddedQuery: String,
    resultLabel: String,
    expectedCreatedNodes: Long
  ): Unit = {
    assertThat(driver.isDefined).isTrue

    val embeddedQuery = embedder.embedWrite(query, "events", "event", scriptResult).build()

    val renderer = container.cypherRenderer()
    assertThat(embeddedQuery.getCypher).isEqualTo(expectedEmbeddedQuery)
    assertThatCode(() =>
      driver.get.executableQuery(renderer.render(embeddedQuery))
        .withParameters(params.asJava)
        .execute()
    )
      .doesNotThrowAnyException()
    assertThat(countNodesWithLabel(resultLabel)).isEqualTo(expectedCreatedNodes)
  }

  private def embed_write_cases(): Stream[Arguments] =
    Stream.of(
      argumentSet(
        "embeds write query",
        "CREATE (:EmbeddedWriteBasic {name: event.name})",
        "",
        queryParams(Seq(
          Map[String, AnyRef]("name" -> "Ada"),
          Map[String, AnyRef]("name" -> "Bob")
        )),
        "UNWIND $events AS event CALL (event) {CREATE (:`EmbeddedWriteBasic` {name: event.name})}",
        "EmbeddedWriteBasic",
        2
      ),
      argumentSet(
        "embeds write query with script result",
        "CREATE (:EmbeddedWriteScript {name: event.name, suffix: scriptResult[0].suffix})",
        "WITH $scriptResult AS scriptResult ",
        queryParams(
          Seq(Map[String, AnyRef]("name" -> "Ada")),
          Some(Seq(Map[String, AnyRef]("suffix" -> "from-script")).map(_.asJava).asJava)
        ),
        "UNWIND $events AS event CALL (event) {WITH $scriptResult AS scriptResult CREATE (:`EmbeddedWriteScript` {name: event.name, suffix: scriptResult[0].suffix})}",
        "EmbeddedWriteScript",
        1
      ),
      argumentSet(
        "embeds write query with nested call",
        "CREATE (n:EmbeddedWriteNested {name: event.name}) CALL (n) { SET n.nested = true } SET n.after = true",
        "",
        queryParams(Seq(Map[String, AnyRef]("name" -> "Ada"))),
        "UNWIND $events AS event CALL (event) {CREATE (n:`EmbeddedWriteNested` {name: event.name}) CALL (*) {SET n.nested = true} SET n.after = true}",
        "EmbeddedWriteNested",
        1
      ),
      argumentSet(
        "embeds write query with skip and limit",
        "UNWIND event.values AS value WITH value ORDER BY value SKIP 1 LIMIT 1 CREATE (:EmbeddedWriteSkipLimit {value: value})",
        "",
        queryParams(Seq(
          Map[String, AnyRef]("values" -> Seq(1, 2, 3).map(_.asInstanceOf[AnyRef]).asJava)
        )),
        "UNWIND $events AS event CALL (event) {UNWIND event.values AS value WITH value ORDER BY value ASC SKIP 1 LIMIT 1 CREATE (:`EmbeddedWriteSkipLimit` {value: value})}",
        "EmbeddedWriteSkipLimit",
        1
      )
    )

  private def queryParams(
    events: Seq[Map[String, AnyRef]],
    scriptResult: Option[AnyRef] = None
  ): Map[String, AnyRef] = {
    val params = Map[String, AnyRef]("events" -> events.map(_.asJava).asJava)
    scriptResult.fold(params)(value => params + ("scriptResult" -> value))
  }

  private def countNodesWithLabel(label: String): Long = {
    driver.get.executableQuery(s"MATCH (n:`$label`) RETURN count(n) AS count")
      .execute()
      .records()
      .get(0)
      .get("count")
      .asLong()
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
