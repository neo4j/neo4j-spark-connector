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
package org.neo4j.spark.util

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.neo4j.driver.AccessMode
import org.neo4j.driver.Value
import org.neo4j.driver.net.ServerAddress
import org.neo4j.spark.util.MapConverter.toScala

import java.net.URI
import java.time.Duration

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class Neo4jOptionsTest {

  @Test
  def requires_url(): Unit = {
    val options = Map(QueryType.QUERY.toString -> "Person")

    assertThatExceptionOfType(classOf[IllegalArgumentException])
      .isThrownBy(() => new Neo4jOptions(options)).withMessage("Parameter 'url' is required")
  }

  @Test
  def supports_relationship_table_name(): Unit = {
    val neo4jOptions = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "bolt://localhost",
      QueryType.RELATIONSHIP.toString.toLowerCase -> "KNOWS",
      Neo4jOptions.RELATIONSHIP_SOURCE_LABELS -> "Person",
      Neo4jOptions.RELATIONSHIP_TARGET_LABELS -> "Answer"
    ))

    assertThat(neo4jOptions.getTableName).isEqualTo("table_Person_KNOWS_Answer")
  }

  @Test
  def supports_labels_table_name(): Unit = {
    val neo4jOptions = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "bolt://localhost",
      "labels" -> "Person:Admin"
    ))

    assertThat(neo4jOptions.getTableName).isEqualTo("table_Person-Admin")
  }

  @Test
  def supports_relationship_node_modes_are_case_insensitive(): Unit = {
    val neo4jOptions = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "bolt://localhost",
      QueryType.RELATIONSHIP.toString.toLowerCase -> "KNOWS",
      Neo4jOptions.RELATIONSHIP_SAVE_STRATEGY -> "nAtIve",
      Neo4jOptions.RELATIONSHIP_SOURCE_SAVE_MODE -> "Errorifexists",
      Neo4jOptions.RELATIONSHIP_TARGET_SAVE_MODE -> "overwrite"
    ))

    assertThat(neo4jOptions.relationshipMetadata.saveStrategy).isEqualTo(RelationshipSaveStrategy.NATIVE)
    assertThat(neo4jOptions.relationshipMetadata.sourceSaveMode).isEqualTo(NodeSaveMode.ErrorIfExists)
    assertThat(neo4jOptions.relationshipMetadata.targetSaveMode).isEqualTo(NodeSaveMode.Overwrite)
  }

  @Test
  def supports_relationship_write_strategy_is_not_present_should_throw_exception(): Unit = {
    val options = Map(
      Neo4jOptions.URL -> "bolt://localhost",
      QueryType.LABELS.toString -> "PERSON",
      "relationship.save.strategy" -> "nope"
    )

    assertThatExceptionOfType(classOf[NoSuchElementException])
      .isThrownBy(() => new Neo4jOptions(options))
      .withMessage("No value found for 'NOPE'")
  }

  @Test
  def supports_query_should_have_query_type(): Unit = {
    val query: String = "MATCH n RETURN n"
    val neo4jOptions = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "bolt://localhost",
      QueryType.QUERY.toString.toLowerCase -> query
    ))

    assertThat(neo4jOptions.query.queryType).isEqualTo(QueryType.QUERY)
    assertThat(neo4jOptions.query.value).isEqualTo(query)
  }

  @Test
  def supports_node_should_have_label_type(): Unit = {
    val label: String = "Person"
    val neo4jOptions = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "bolt://localhost",
      QueryType.LABELS.toString.toLowerCase -> label
    ))

    assertThat(neo4jOptions.query.queryType).isEqualTo(QueryType.LABELS)
    assertThat(neo4jOptions.query.value).isEqualTo(label)
  }

  @Test
  def supports_relationship_should_have_relationship_type(): Unit = {
    val relationship: String = "KNOWS"
    val neo4jOptions = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "bolt://localhost",
      QueryType.LABELS.toString.toLowerCase -> relationship
    ))

    assertThat(neo4jOptions.query.queryType).isEqualTo(QueryType.LABELS)
    assertThat(neo4jOptions.query.value).isEqualTo(relationship)
  }

  @Test
  def supports_push_down_column_is_disabled(): Unit = {
    val neo4jOptions = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "bolt://localhost",
      "pushdown.columns.enabled" -> "false"
    ))

    assertThat(neo4jOptions.pushdownColumnsEnabled).isFalse
  }

  @Test
  def supports_driver_defaults(): Unit = {
    val neo4jOptions = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "bolt://localhost",
      QueryType.QUERY.toString.toLowerCase -> "MATCH n RETURN n"
    ))

    assertThat(neo4jOptions.session.database).isEqualTo("")
    assertThat(neo4jOptions.session.accessMode).isEqualTo(AccessMode.READ)
    assertThat(neo4jOptions.connection.auth).isEqualTo("basic")
    assertThat(neo4jOptions.connection.authParameters).isEqualTo(Neo4jOptions.DEFAULT_AUTH_PARAMETERS)
    assertThat(neo4jOptions.connection.encryption).isEqualTo(false)
    assertThat(neo4jOptions.connection.trustStrategy).isEqualTo(None)
    assertThat(neo4jOptions.connection.certificatePath).isEqualTo("")
    assertThat(neo4jOptions.connection.lifetime).isEqualTo(Neo4jOptions.DEFAULT_CONNECTION_MAX_LIFETIME_MSECS)
    assertThat(neo4jOptions.connection.acquisitionTimeout).isEqualTo(-1)
    assertThat(neo4jOptions.connection.connectionTimeout).isEqualTo(-1)
    assertThat(
      neo4jOptions.connection.livenessCheckTimeout
    ).isEqualTo(Neo4jOptions.DEFAULT_CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS)
    assertThat(neo4jOptions.relationshipMetadata.saveStrategy).isEqualTo(RelationshipSaveStrategy.KEYS)
    assertThat(neo4jOptions.pushdownFiltersEnabled).isTrue
  }

  @Test
  def supports_apoc_configuration(): Unit = {
    val neo4jOptions = new Neo4jOptions(Map(
      "apoc.meta.nodeTypeProperties" -> """{"nodeLabels": ["Label"], "mandatory": false}""",
      Neo4jOptions.URL -> "bolt://localhost"
    ))

    val expected = Map("apoc.meta.nodeTypeProperties" -> Map(
      "nodeLabels" -> Seq("Label").asJava,
      "mandatory" -> false
    ))

    assertThat(expected).isEqualTo(neo4jOptions.apocConfig.procedureConfigMap)
  }

  @Test
  def supports_null_property(): Unit = {
    val neo4jOptions = new Neo4jOptions(Map(
      "relationship.properties" -> null,
      Neo4jOptions.URL -> "bolt://localhost"
    ))

    assertThat(None).isEqualTo(neo4jOptions.relationshipMetadata.properties)
  }

  @Test
  def supports_multiple_urls(): Unit = {
    val neo4jOptions = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "neo4j://localhost, neo4j://foo.bar:7687, neo4j://foo.bar.baz:7783"
    ))

    val (baseUrl, resolvers) = neo4jOptions.connection.connectionUrls

    assertThat(baseUrl).isEqualTo(URI.create("neo4j://localhost"))
    assertThat(resolvers).isEqualTo(Set(ServerAddress.of("foo.bar", 7687), ServerAddress.of("foo.bar.baz", 7783)))
  }

  @Test
  def supports_gds_properties(): Unit = {
    val neo4jOptions = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "neo4j://localhost,neo4j://foo.bar,neo4j://foo.bar.baz:7783",
      "gds" -> "gds.pageRank.stream",
      "gds.graphName" -> "myGraph",
      "gds.configuration.concurrency" -> "2"
    ))

    assertThat(neo4jOptions.query.queryType).isEqualTo(QueryType.GDS)
    assertThat(neo4jOptions.query.value).isEqualTo("gds.pageRank.stream")
    assertThat(neo4jOptions.gdsMetadata.parameters).isEqualTo(
      Map(
        "graphName" -> "myGraph",
        "configuration" -> Map("concurrency" -> 2).asJava
      ).asJava
    )
  }

  @Test
  def supports_transaction_timeout(): Unit = {
    val neo4jOptions = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "neo4j://localhost,neo4j://foo.bar,neo4j://foo.bar.baz:7783",
      "db.transaction.timeout" -> "1000"
    ))

    val transactionConfig = neo4jOptions.toNeo4jTransactionConfig
    assertThat(transactionConfig.timeout()).isEqualTo(Duration.ofMillis(1000))
  }

  @Test
  def supports_default_transaction_timeout(): Unit = {
    val neo4jOptions = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "neo4j://localhost,neo4j://foo.bar,neo4j://foo.bar.baz:7783"
    ))

    val transactionConfig = neo4jOptions.toNeo4jTransactionConfig

    assertThat(transactionConfig.timeout()).isNull()
  }

  @Test
  def supports_transaction_metadata(): Unit = {
    val neo4jOptions = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "neo4j://localhost,neo4j://foo.bar,neo4j://foo.bar.baz:7783",
      "db.transaction.metadata.foo" -> "bar",
      "db.transaction.metadata.bar" -> "true",
      "db.transaction.metadata.qix" -> "42",
      "db.transaction.metadata.my.thing" -> "23.0",
      "db.transaction.metadata.json_array_treated_as_string" -> "[true,43]",
      "db.transaction.metadata.json_map_treated_as_string" -> """{"map": false}"""
    ))

    val transactionConfig = neo4jOptions.toNeo4jTransactionConfig

    val metadata = toScala(
      transactionConfig.metadata(),
      {
        // org.neo4j.driver.TransactionConfig#metadata() wraps all values into the Java driver's Value wrapper type
        case v: Value => v.asObject()
        case value    => value
      }
    )
    assertThat(metadata).isEqualTo(Map(
      "foo" -> "bar",
      "bar" -> true,
      "qix" -> 42L,
      "my" -> Map("thing" -> 23.0),
      "json_array_treated_as_string" -> "[true,43]",
      "json_map_treated_as_string" -> """{"map":false}"""
    ))
  }

  @Test
  def extracts_script_from_single_script_option(): Unit = {
    val neo4jOptions = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "bolt://localhost",
      Neo4jOptions.SCRIPT -> "CREATE INDEX person_surname FOR (p:Person) ON (p.surname);"
    ))

    assertThat(neo4jOptions.script).isEqualTo(Array(
      "CREATE INDEX person_surname FOR (p:Person) ON (p.surname);"
    ))
  }

  @Test
  def sorts_script_by_index_when_using_indexed_script_options(): Unit = {
    val neo4jOptions = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "bolt://localhost",
      s"${Neo4jOptions.SCRIPT_PREFIX}2" -> "CREATE CONSTRAINT product_name_sku FOR (p:Product) REQUIRE (p.name, p.sku) IS NODE KEY",
      s"${Neo4jOptions.SCRIPT_PREFIX}3" -> "RETURN 36 AS age",
      s"${Neo4jOptions.SCRIPT_PREFIX}01" -> "CREATE INDEX person_surname FOR (p:Person) ON (p.surname)"
    ))

    assertThat(neo4jOptions.script).isEqualTo(Array(
      "CREATE INDEX person_surname FOR (p:Person) ON (p.surname)",
      "CREATE CONSTRAINT product_name_sku FOR (p:Product) REQUIRE (p.name, p.sku) IS NODE KEY",
      "RETURN 36 AS age"
    ))
  }

  @Test
  def fails_when_both_script_options_provided(): Unit = {
    val options = Map(
      Neo4jOptions.URL -> "bolt://localhost",
      Neo4jOptions.SCRIPT -> "CREATE INDEX person_surname FOR (p:Person) ON (p.surname)",
      s"${Neo4jOptions.SCRIPT_PREFIX}1" -> "CREATE INDEX person_surname FOR (p:Person) ON (p.surname)"
    )

    assertThatExceptionOfType(classOf[IllegalArgumentException])
      .isThrownBy(() => new Neo4jOptions(options)).withMessage(
        "'script' and indexed script options ('script.1', 'script.2', etc.) cannot be used together."
      )
  }

  @ParameterizedTest
  @ValueSource(strings = Array("invalid", "-1", "1.5", "a1", "1a", ""))
  def fails_when_indexed_script_suffix_contains_non_digit_characters(suffix: String): Unit = {
    val options = Map(
      Neo4jOptions.URL -> "bolt://localhost",
      s"${Neo4jOptions.SCRIPT_PREFIX}$suffix" -> "CREATE INDEX person_surname FOR (p:Person) ON (p.surname)"
    )

    assertThatExceptionOfType(classOf[IllegalArgumentException])
      .isThrownBy(() => new Neo4jOptions(options)).withMessage(
        s"Script option 'script.$suffix' must have a suffix containing only digits."
      )
  }
}
