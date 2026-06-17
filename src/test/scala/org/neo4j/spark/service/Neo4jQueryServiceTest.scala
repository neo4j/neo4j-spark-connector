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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.connector.expressions.NullOrdering
import org.apache.spark.sql.connector.expressions.SortDirection
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.expressions.aggregate.Count
import org.apache.spark.sql.connector.expressions.aggregate.Max
import org.apache.spark.sql.connector.expressions.aggregate.Min
import org.apache.spark.sql.connector.expressions.aggregate.Sum
import org.apache.spark.sql.sources._
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDeploymentType.SELF_MANAGED
import org.neo4j.caniuse.Neo4jEdition
import org.neo4j.caniuse.Neo4jEdition.COMMUNITY
import org.neo4j.caniuse.Neo4jEdition.ENTERPRISE
import org.neo4j.caniuse.Neo4jVersion
import org.neo4j.spark.config.TopN
import org.neo4j.spark.cypher.CypherRenderer
import org.neo4j.spark.util.DummyNamedReference
import org.neo4j.spark.util.Neo4jImplicits.CypherImplicits
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.QueryType

import java.util.Collections

import scala.collection.immutable.HashMap

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Neo4jQueryServiceTest {

  @Nested
  @DisplayName("generates read cypher for")
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class ReadQueryServiceTest {

    import Neo4jQueryServiceTest._

    private def versions_and_prefixes() = _versions_and_prefixes()

    private def tuning_parameters() = _tuning_parameters()

    private def all_params_cross_combined() = _all_params_cross_combined()

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def labels_when_one_label_selected(neo4j: Neo4j, customCypherVersion: String, prefix: String): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.LABELS.toString.toLowerCase -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String =
        new Neo4jQueryService(neo4jOptions, plainReadStrategy(neo4j, neo4jOptions)).createQuery()

      assertThat(query).isEqualTo(s"${prefix}MATCH (n:`Person`) RETURN n")
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def labels_when_multiple_labels_selected(neo4j: Neo4j, customCypherVersion: String, prefix: String): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.LABELS.toString.toLowerCase -> ":Person:Player:Midfield",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String =
        new Neo4jQueryService(neo4jOptions, plainReadStrategy(neo4j, neo4jOptions)).createQuery()

      assertThat(query).isEqualTo(s"${prefix}MATCH (n:`Person`:`Player`:`Midfield`) RETURN n")
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def labels_when_partitioned(neo4j: Neo4j, customCypherVersion: String, prefix: String): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.LABELS.toString.toLowerCase -> ":Person:Player:Midfield",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          partitionPagination = PartitionPagination(0, 0, TopN(100))
        )
      ).createQuery()

      assertThat(query).isEqualTo(s"${prefix}MATCH (n:`Person`:`Player`:`Midfield`) RETURN n LIMIT 100")
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def labels_when_one_column_selected(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.LABELS.toString.toLowerCase -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          Array.empty[Filter],
          PartitionPagination.EMPTY,
          Seq("name")
        )
      ).createQuery()

      assertThat(query).isEqualTo(s"${prefix}MATCH (n:`Person`) RETURN n.name AS name")
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def labels_when_multiple_columns_selected(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.LABELS.toString.toLowerCase -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          Array.empty[Filter],
          PartitionPagination.EMPTY,
          List("name", "bornDate")
        )
      ).createQuery()

      assertThat(query).isEqualTo(s"${prefix}MATCH (n:`Person`) RETURN n.name AS name, n.bornDate AS bornDate")
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def labels_when_internal_id_selected(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.LABELS.toString.toLowerCase -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          Array.empty[Filter],
          PartitionPagination.EMPTY,
          List("<elementId>")
        )
      ).createQuery()

      assertThat(query).isEqualTo(s"${prefix}MATCH (n:`Person`) RETURN elementId(n) AS `<elementId>`")
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def labels_when_filtering_on_equality(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.LABELS.toString.toLowerCase -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val filters: Array[Filter] = Array[Filter](
        EqualTo("name", "John Doe")
      )

      val query: String =
        new Neo4jQueryService(
          neo4jOptions,
          new Neo4jQueryReadStrategy(neo4j, new CypherRenderer(neo4j, neo4jOptions), filters)
        ).createQuery()

      val paramName = "$" + "name".toParameterName("John Doe")

      assertThat(query).isEqualTo(s"${prefix}MATCH (n:`Person`) WHERE n.name = $paramName RETURN n")
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def labels_when_filtering_on_null_safe_equality(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.LABELS.toString.toLowerCase -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val filters: Array[Filter] = Array[Filter](
        EqualNullSafe("name", "John Doe"),
        EqualTo("age", 36)
      )

      val query: String =
        new Neo4jQueryService(
          neo4jOptions,
          new Neo4jQueryReadStrategy(neo4j, new CypherRenderer(neo4j, neo4jOptions), filters)
        ).createQuery()

      val nameParameterName = "$" + "name".toParameterName("John Doe")
      val ageParameterName = "$" + "age".toParameterName(36)

      assertThat(query).isEqualTo(
        s"""${prefix}MATCH (n:`Person`)
           | WHERE (((n.name IS NULL AND $nameParameterName IS NULL)
           | OR n.name = $nameParameterName) AND n.age = $ageParameterName)
           | RETURN n""".stripMargin.replaceAll("\n", "")
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def labels_when_filtering_on_null_safe_equality_and_value_is_null(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.LABELS.toString.toLowerCase -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val filters: Array[Filter] = Array[Filter](
        EqualNullSafe("name", null),
        EqualTo("age", 36)
      )

      val query: String =
        new Neo4jQueryService(
          neo4jOptions,
          new Neo4jQueryReadStrategy(neo4j, new CypherRenderer(neo4j, neo4jOptions), filters)
        ).createQuery()

      val nameParameterName = "$" + "name".toParameterName(null)
      val ageParameterName = "$" + "age".toParameterName(36)

      assertThat(query).isEqualTo(
        s"${prefix}MATCH (n:`Person`) WHERE (((n.name IS NULL AND $nameParameterName IS NULL) OR n.name = $nameParameterName) AND n.age = $ageParameterName) RETURN n"
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def labels_when_filtering_on_starts_or_end_with(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.LABELS.toString.toLowerCase -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val filters: Array[Filter] = Array[Filter](
        StringStartsWith("name", "Person Name"),
        StringEndsWith("name", "Person Surname")
      )

      val query: String =
        new Neo4jQueryService(
          neo4jOptions,
          new Neo4jQueryReadStrategy(neo4j, new CypherRenderer(neo4j, neo4jOptions), filters)
        ).createQuery()

      val nameOneParameterName = "$" + "name".toParameterName("Person Name")
      val nameTwoParameterName = "$" + "name".toParameterName("Person Surname")

      assertThat(query).isEqualTo(
        s"""${prefix}MATCH (n:`Person`)
           | WHERE (n.name STARTS WITH $nameOneParameterName
           | AND n.name ENDS WITH $nameTwoParameterName)
           | RETURN n""".stripMargin.replaceAll("\n", "")
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def relationship_when_one_column_selected(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "KNOWS",
        "relationship.nodes.map" -> "false",
        "relationship.source.labels" -> "Person",
        "relationship.target.labels" -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          Array.empty[Filter],
          PartitionPagination.EMPTY,
          List("source.name")
        )
      ).createQuery()

      assertThat(query).isEqualTo(
        s"${prefix}MATCH (source:`Person`) " +
          "MATCH (target:`Person`) " +
          "MATCH (source)-[rel:`KNOWS`]->(target) RETURN source.name AS `source.name`"
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def relationship_when_multiple_columns_are_selected(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "KNOWS",
        "relationship.nodes.map" -> "false",
        "relationship.source.labels" -> "Person",
        "relationship.target.labels" -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          Array.empty[Filter],
          PartitionPagination.EMPTY,
          List("source.name", "<source.elementId>")
        )
      ).createQuery()

      assertThat(query).isEqualTo(
        s"${prefix}MATCH (source:`Person`) " +
          "MATCH (target:`Person`) " +
          s"MATCH (source)-[rel:`KNOWS`]->(target) RETURN source.name AS `source.name`, elementId(source) AS `<source.elementId>`"
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def relationship_when_partitioned(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "KNOWS",
        "relationship.nodes.map" -> "false",
        "relationship.source.labels" -> "Person",
        "relationship.target.labels" -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          Array.empty[Filter],
          PartitionPagination(0, 0, TopN(limit = 100)),
          List("source.name", "<source.elementId>")
        )
      ).createQuery()

      assertThat(query).isEqualTo(
        s"""${prefix}MATCH (source:`Person`)
           |MATCH (target:`Person`)
           |MATCH (source)-[rel:`KNOWS`]->(target)
           |RETURN source.name AS `source.name`, elementId(source) AS `<source.elementId>`
           |LIMIT 100"""
          .stripMargin
          .replace(System.lineSeparator(), " ")
      )
    }

    // TODO: consider removing because of overlap
    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def relationship_when_multiple_columns_are_selected_version_2(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "KNOWS",
        "relationship.nodes.map" -> "false",
        "relationship.source.labels" -> "Person",
        "relationship.target.labels" -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          Array.empty[Filter],
          PartitionPagination.EMPTY,
          List("source.name", "source.id", "rel.someprops", "target.date")
        )
      ).createQuery()

      assertThat(query).isEqualTo(
        s"${prefix}MATCH (source:`Person`) " +
          "MATCH (target:`Person`) " +
          "MATCH (source)-[rel:`KNOWS`]->(target) RETURN source.name AS `source.name`, source.id AS `source.id`, rel.someprops AS `rel.someprops`, target.date AS `target.date`"
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def read_query_for_relationships_when_filtering_on_equality(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "KNOWS",
        "relationship.nodes.map" -> "false",
        "relationship.source.labels" -> "Person",
        "relationship.target.labels" -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val filters: Array[Filter] = Array[Filter](
        EqualTo("source.name", "John Doe")
      )

      val query: String =
        new Neo4jQueryService(
          neo4jOptions,
          new Neo4jQueryReadStrategy(neo4j, new CypherRenderer(neo4j, neo4jOptions), filters)
        ).createQuery()

      val parameterName = "$" + "source.name".toParameterName("John Doe")

      assertThat(query).isEqualTo(
        s"${prefix}MATCH (source:`Person`) " +
          "MATCH (target:`Person`) " +
          s"MATCH (source)-[rel:`KNOWS`]->(target) WHERE source.name = $parameterName RETURN rel, source AS source, target AS target"
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def relationships_when_filtering_on_equality_with_or(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "KNOWS",
        "relationship.nodes.map" -> "false",
        "relationship.source.labels" -> "Person",
        "relationship.target.labels" -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val filters: Array[Filter] = Array[Filter](
        Or(EqualTo("source.name", "John Doe"), EqualTo("target.name", "John Doe"))
      )

      val query: String =
        new Neo4jQueryService(
          neo4jOptions,
          new Neo4jQueryReadStrategy(neo4j, new CypherRenderer(neo4j, neo4jOptions), filters)
        ).createQuery()

      val paramOneName = "$" + "source.name".toParameterName("John Doe")
      val paramTwoName = "$" + "target.name".toParameterName("John Doe")

      assertThat(query).isEqualTo(
        s"${prefix}MATCH (source:`Person`) " +
          "MATCH (target:`Person`) " +
          s"MATCH (source)-[rel:`KNOWS`]->(target) WHERE (source.name = $paramOneName OR target.name = $paramTwoName) RETURN rel, source AS source, target AS target"
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def relationships_when_filtering_on_equality_with_and(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "KNOWS",
        "relationship.nodes.map" -> "true",
        "relationship.source.labels" -> "Person",
        "relationship.target.labels" -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val filters: Array[Filter] = Array[Filter](
        EqualTo("source.id", "14"),
        EqualTo("target.id", "16")
      )

      val query: String =
        new Neo4jQueryService(
          neo4jOptions,
          new Neo4jQueryReadStrategy(neo4j, new CypherRenderer(neo4j, neo4jOptions), filters)
        ).createQuery()

      val sourceIdParameterName = "$" + "source.id".toParameterName(14)
      val targetIdParameterName = "$" + "target.id".toParameterName(16)

      assertThat(query).isEqualTo(
        s"""${prefix}MATCH (source:`Person`)
           | MATCH (target:`Person`)
           | MATCH (source)-[rel:`KNOWS`]->(target)
           | WHERE (source.id = $sourceIdParameterName AND target.id = $targetIdParameterName)
           | RETURN rel, source AS source, target AS target
           |""".stripMargin.replaceAll("\n", "")
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def labels_when_complex_filtering(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "labels" -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val filters: Array[Filter] = Array[Filter](
        Or(EqualTo("name", "John Doe"), EqualTo("name", "John Scofield")),
        Or(EqualTo("age", 15), GreaterThanOrEqual("age", 18)),
        Or(Not(EqualTo("age", 22)), Not(LessThan("age", 11)))
      )

      val query: String =
        new Neo4jQueryService(
          neo4jOptions,
          new Neo4jQueryReadStrategy(neo4j, new CypherRenderer(neo4j, neo4jOptions), filters)
        ).createQuery()

      val parameterNames: Map[String, String] = HashMap(
        "name_1" -> "$".concat("name".toParameterName("John Doe")),
        "name_2" -> "$".concat("name".toParameterName("John Scofield")),
        "age_1" -> "$".concat("age".toParameterName(15)),
        "age_2" -> "$".concat("age".toParameterName(18)),
        "age_3" -> "$".concat("age".toParameterName(22)),
        "age_4" -> "$".concat("age".toParameterName(11))
      )

      assertThat(query).isEqualTo(
        s"""${prefix}MATCH (n:`Person`)
           | WHERE (((n.name = ${parameterNames("name_1")} OR n.name = ${parameterNames("name_2")})
           | AND (n.age = ${parameterNames("age_1")} OR n.age >= ${parameterNames("age_2")}))
           | AND (NOT (n.age = ${parameterNames("age_3")}) OR NOT (n.age < ${parameterNames("age_4")})))
           | RETURN n""".stripMargin.replaceAll("\n", "")
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def relationship_when_complex_filtering_with_map_mode(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "KNOWS",
        "relationship.nodes.map" -> "true",
        "relationship.source.labels" -> "Person",
        "relationship.target.labels" -> "Person:Customer",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val filters: Array[Filter] = Array[Filter](
        Or(
          Or(EqualTo("source.name", "John Doe"), EqualTo("target.name", "John Doraemon")),
          EqualTo("source.name", "Jane Doe")
        ),
        Or(EqualTo("target.age", 34), EqualTo("target.age", 18)),
        EqualTo("rel.score", 12)
      )

      val query: String =
        new Neo4jQueryService(
          neo4jOptions,
          new Neo4jQueryReadStrategy(neo4j, new CypherRenderer(neo4j, neo4jOptions), filters)
        ).createQuery()

      val parameterNames = Map(
        "source.name_1" -> "$".concat("source.name".toParameterName("John Doe")),
        "target.name_1" -> "$".concat("target.name".toParameterName("John Doraemon")),
        "source.name_2" -> "$".concat("source.name".toParameterName("Jane Doe")),
        "target.age_1" -> "$".concat("target.age".toParameterName(34)),
        "target.age_2" -> "$".concat("target.age".toParameterName(18)),
        "rel.score" -> "$".concat("rel.score".toParameterName(12))
      )

      assertThat(query).isEqualTo(
        s"""${prefix}MATCH (source:`Person`)
           | MATCH (target:`Person`:`Customer`)
           | MATCH (source)-[rel:`KNOWS`]->(target)
           | WHERE ((source.name = ${parameterNames("source.name_1")} OR target.name = ${
            parameterNames(
              "target.name_1"
            )
          } OR source.name = ${parameterNames("source.name_2")})
           | AND (target.age = ${parameterNames("target.age_1")} OR target.age = ${parameterNames("target.age_2")})
           | AND rel.score = ${parameterNames("rel.score")})
           | RETURN rel, source AS source, target AS target
           |""".stripMargin.replaceAll("\n", "")
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def relationship_when_complex_filtering_without_map_mode(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "KNOWS",
        "relationship.nodes.map" -> "false",
        "relationship.source.labels" -> "Person",
        "relationship.target.labels" -> "Person:Customer",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val filters: Array[Filter] = Array[Filter](
        Or(
          Or(EqualTo("source.name", "John Doe"), EqualTo("target.name", "John Doraemon")),
          EqualTo("source.name", "Jane Doe")
        ),
        Or(EqualTo("target.age", 34), EqualTo("target.age", 18)),
        EqualTo("rel.score", 12)
      )

      val query: String =
        new Neo4jQueryService(
          neo4jOptions,
          new Neo4jQueryReadStrategy(neo4j, new CypherRenderer(neo4j, neo4jOptions), filters)
        ).createQuery()

      val parameterNames = Map(
        "source.name_1" -> "$".concat("source.name".toParameterName("John Doe")),
        "target.name_1" -> "$".concat("target.name".toParameterName("John Doraemon")),
        "source.name_2" -> "$".concat("source.name".toParameterName("Jane Doe")),
        "target.age_1" -> "$".concat("target.age".toParameterName(34)),
        "target.age_2" -> "$".concat("target.age".toParameterName(18)),
        "rel.score" -> "$".concat("rel.score".toParameterName(12))
      )

      assertThat(query).isEqualTo(
        s"""${prefix}MATCH (source:`Person`)
           | MATCH (target:`Person`:`Customer`)
           | MATCH (source)-[rel:`KNOWS`]->(target)
           | WHERE ((source.name = ${parameterNames("source.name_1")} OR target.name = ${
            parameterNames(
              "target.name_1"
            )
          } OR source.name = ${parameterNames("source.name_2")})
           | AND (target.age = ${parameterNames("target.age_1")} OR target.age = ${parameterNames("target.age_2")})
           | AND rel.score = ${parameterNames("rel.score")})
           | RETURN rel, source AS source, target AS target""".stripMargin.replaceAll("\n", "")
      )
    }

    // todo consider breaking up or soft assertions
    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def labels_yields_sum_aggregations(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.LABELS.toString.toLowerCase -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val ageField = new DummyNamedReference("age")
      var query: String = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          Array.empty[Filter],
          PartitionPagination.EMPTY,
          Seq("name", "SUM(DISTINCT age)", "SUM(age)"),
          Array(
            new Sum(ageField, false),
            new Sum(ageField, true)
          )
        )
      ).createQuery()

      assertThat(query).isEqualTo(
        s"${prefix}MATCH (n:`Person`) RETURN n.name AS name, sum(DISTINCT n.age) AS `SUM(DISTINCT age)`, sum(n.age) AS `SUM(age)`"
      )

      val nameField = new DummyNamedReference("name")
      query = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          Array.empty[Filter],
          PartitionPagination.EMPTY,
          Seq("name", "COUNT(DISTINCT name)", "COUNT(name)"),
          Array(
            new Count(nameField, false),
            new Count(nameField, true)
          )
        )
      ).createQuery()

      assertThat(query).isEqualTo(
        s"${prefix}MATCH (n:`Person`) RETURN n.name AS name, count(DISTINCT n.name) AS `COUNT(DISTINCT name)`, count(n.name) AS `COUNT(name)`"
      )

      query = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          Array.empty[Filter],
          PartitionPagination.EMPTY,
          Seq("name", "MAX(age)", "MIN(age)"),
          Array(
            new Max(ageField),
            new Min(ageField)
          )
        )
      ).createQuery()

      assertThat(query).isEqualTo(
        s"${prefix}MATCH (n:`Person`) RETURN n.name AS name, max(n.age) AS `MAX(age)`, min(n.age) AS `MIN(age)`"
      )
    }

    // todo consider breaking up or soft assertions
    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def relationships_yields_sum_aggregations(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "BOUGHT",
        "relationship.nodes.map" -> "false",
        "relationship.source.labels" -> "Person",
        "relationship.target.labels" -> "Product",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val targetPriceField = new DummyNamedReference("`target.price`")
      var query: String = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          Array.empty,
          PartitionPagination.EMPTY,
          List("source.fullName", "SUM(DISTINCT `target.price`)", "SUM(`target.price`)"),
          Array(
            new Sum(targetPriceField, false),
            new Sum(targetPriceField, true)
          )
        )
      ).createQuery()

      assertThat(query).isEqualTo(
        s"""${prefix}MATCH (source:`Person`)
           |MATCH (target:`Product`)
           |MATCH (source)-[rel:`BOUGHT`]->(target)
           |RETURN source.fullName AS `source.fullName`, sum(DISTINCT target.price) AS `SUM(DISTINCT ``target.price``)`, sum(target.price) AS `SUM(``target.price``)`"""
          .stripMargin
          .replaceAll("\n", " ")
      )

      val targetIdField = new DummyNamedReference("`target.id`")
      query = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          Array.empty,
          PartitionPagination.EMPTY,
          List("source.fullName", "COUNT(DISTINCT `target.id`)", "COUNT(`target.id`)"),
          Array(
            new Count(targetIdField, false),
            new Count(targetIdField, true)
          )
        )
      ).createQuery()

      assertThat(query).isEqualTo(
        s"""${prefix}MATCH (source:`Person`) MATCH (target:`Product`)
           |MATCH (source)-[rel:`BOUGHT`]->(target)
           |RETURN source.fullName AS `source.fullName`, count(DISTINCT target.id) AS `COUNT(DISTINCT ``target.id``)`, count(target.id) AS `COUNT(``target.id``)`"""
          .stripMargin
          .replaceAll("\n", " ")
      )

      query = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          Array.empty,
          PartitionPagination.EMPTY,
          List("source.fullName", "MAX(`target.price`)", "MIN(`target.price`)"),
          Array(
            new Max(targetPriceField),
            new Min(targetPriceField)
          )
        )
      ).createQuery()

      assertThat(query).isEqualTo(
        s"""${prefix}MATCH (source:`Person`)
           |MATCH (target:`Product`)
           |MATCH (source)-[rel:`BOUGHT`]->(target)
           |RETURN source.fullName AS `source.fullName`, max(target.price) AS `MAX(``target.price``)`, min(target.price) AS `MIN(``target.price``)`"""
          .stripMargin
          .replaceAll("\n", " ")
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def labels_when_top_n_partition_pagination(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.LABELS.toString.toLowerCase -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          partitionPagination = PartitionPagination(
            0,
            0,
            TopN(
              42,
              Array(new SortOrder {
                override def expression(): Expression = new DummyNamedReference("name")

                override def direction(): SortDirection = SortDirection.ASCENDING

                override def nullOrdering(): NullOrdering = direction().defaultNullOrdering()
              })
            )
          )
        )
      ).createQuery()

      assertThat(query).isEqualTo(s"${prefix}MATCH (n:`Person`) RETURN n ORDER BY n.name ASC LIMIT 42")
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def labels_with_top_n_partition_pagination_and_required_column(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.LABELS.toString.toLowerCase -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          requiredColumns = Array("name").toIndexedSeq,
          partitionPagination = PartitionPagination(
            0,
            0,
            TopN(
              42,
              Array(new SortOrder {
                override def expression(): Expression = new DummyNamedReference("name")

                override def direction(): SortDirection = SortDirection.ASCENDING

                override def nullOrdering(): NullOrdering = direction().defaultNullOrdering()
              })
            )
          )
        )
      ).createQuery()

      assertThat(query).isEqualTo(s"${prefix}MATCH (n:`Person`) RETURN n.name AS name ORDER BY n.name ASC LIMIT 42")
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def relationship_when_top_n_partition_pagination(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "KNOWS",
        "relationship.nodes.map" -> "false",
        "relationship.source.labels" -> "Person",
        "relationship.target.labels" -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          Array.empty[Filter],
          PartitionPagination(
            0,
            0,
            TopN(
              24,
              Array(new SortOrder {
                override def expression(): Expression = new DummyNamedReference("rel.since")

                override def direction(): SortDirection = SortDirection.DESCENDING

                override def nullOrdering(): NullOrdering = direction().defaultNullOrdering()
              })
            )
          )
        )
      ).createQuery()

      assertThat(query).isEqualTo(
        s"${prefix}MATCH (source:`Person`) " +
          "MATCH (target:`Person`) " +
          "MATCH (source)-[rel:`KNOWS`]->(target) RETURN rel, source AS source, target AS target " +
          "ORDER BY rel.since DESC LIMIT 24"
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def relationship_when_top_n_partition_pagination_and_required_column(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "KNOWS",
        "relationship.nodes.map" -> "false",
        "relationship.source.labels" -> "Person",
        "relationship.target.labels" -> "Person",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          Array.empty[Filter],
          PartitionPagination(
            0,
            0,
            TopN(
              24,
              Array(new SortOrder {
                override def expression(): Expression = new DummyNamedReference("rel.since")

                override def direction(): SortDirection = SortDirection.DESCENDING

                override def nullOrdering(): NullOrdering = direction().defaultNullOrdering()
              })
            )
          ),
          Array("source.name").toIndexedSeq
        )
      ).createQuery()

      assertThat(query).isEqualTo(
        s"""${prefix}MATCH (source:`Person`)
           |MATCH (target:`Person`)
           |MATCH (source)-[rel:`KNOWS`]->(target) RETURN source.name AS `source.name`
           |ORDER BY rel.since DESC LIMIT 24"""
          .stripMargin
          .replaceAll("\n", " ")
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def custom_queries_when_top_n_partition_pagination_ignores_aggregations(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.QUERY.toString.toLowerCase -> "MATCH (p:Person) RETURN p",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          Array.empty[Filter],
          PartitionPagination(
            0,
            0,
            TopN(
              24,
              Array(new SortOrder {
                override def expression(): Expression = new DummyNamedReference("name")

                override def direction(): SortDirection = SortDirection.DESCENDING

                override def nullOrdering(): NullOrdering = direction().defaultNullOrdering()
              })
            )
          )
        )
      ).createQuery()

      assertThat(query).isEqualTo(s"${prefix}MATCH (p:Person) RETURN p SKIP 0 LIMIT 24")
    }

    @ParameterizedTest
    @MethodSource(Array("tuning_parameters"))
    def labels_when_tuning_preamble(tuningOptions: Map[String, String], prefix: String): Unit = {
      val optionsMap = Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.LABELS.toString.toLowerCase -> "Person"
      )

      val neo4jOptions = new Neo4jOptions(optionsMap ++ tuningOptions)

      val readService =
        new Neo4jQueryService(neo4jOptions, plainReadStrategy(neo4j(version(5, 0), COMMUNITY), neo4jOptions))

      val wantQuery = "MATCH (n:`Person`) RETURN n"
      assertThat(readService.createQuery().trim).isEqualTo(s"$prefix\n$wantQuery".trim)
    }

    @ParameterizedTest
    @MethodSource(Array("tuning_parameters"))
    def relationship_when_tuning_preamble(tuningOptions: Map[String, String], prefix: String): Unit = {
      val optionsMap = Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "KNOWS",
        "relationship.nodes.map" -> "false",
        "relationship.source.labels" -> "Person",
        "relationship.target.labels" -> "Person"
      )

      val neo4jOptions = new Neo4jOptions(optionsMap ++ tuningOptions)

      val readService =
        new Neo4jQueryService(neo4jOptions, plainReadStrategy(neo4j(version(5, 0), COMMUNITY), neo4jOptions))

      val wantQuery =
        "MATCH (source:`Person`) MATCH (target:`Person`) MATCH (source)-[rel:`KNOWS`]->(target) RETURN rel, source AS source, target AS target"

      assertThat(readService.createQuery().trim).isEqualTo(s"$prefix\n$wantQuery".trim)
    }

    @ParameterizedTest
    @MethodSource(Array("tuning_parameters"))
    def custom_query_without_preamble_options_does_not_generate_any_preamble(
      tuningOptions: Map[String, String],
      ignored: String
    ): Unit = {
      val optionsMap = Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.QUERY.toString.toLowerCase -> "MATCH (o:Object) RETURN o"
      )

      val neo4jOptions = new Neo4jOptions(optionsMap ++ tuningOptions)
      val neo4jInfo = neo4j(version(5, 0), COMMUNITY)

      val noPreambleReadService =
        new Neo4jQueryService(
          neo4jOptions,
          new Neo4jQueryReadStrategy(
            neo4jInfo,
            new CypherRenderer(neo4jInfo, neo4jOptions),
            withPreamble = false
          )
        )

      val wantQuery = "MATCH (o:Object) RETURN o"
      assertThat(noPreambleReadService.createQuery().trim).isEqualTo(wantQuery)
    }

    @ParameterizedTest
    @MethodSource(Array("all_params_cross_combined"))
    def custom_query_properly_embeds_cypher_version_and_tuning_preamble(
      neo4j: Neo4j,
      customCypherVersion: String,
      cypherVersionPreamble: String,
      tuningOptions: Map[String, String],
      tuningPrefix: String
    ): Unit = {
      val optionsMap = Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.QUERY.toString.toLowerCase -> "MATCH (o:Object) RETURN o",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      )

      val neo4jOptions = new Neo4jOptions(optionsMap ++ tuningOptions)
      val readService = new Neo4jQueryService(neo4jOptions, plainReadStrategy(neo4j, neo4jOptions))

      val wantQuery = s"$tuningPrefix\n${cypherVersionPreamble}MATCH (o:Object) RETURN o"
      assertThat(readService.createQuery().trim).isEqualTo(wantQuery.trim)
    }

    @ParameterizedTest
    @MethodSource(Array("all_params_cross_combined"))
    def custom_query_properly_embeds_script_results_when_script_is_present(
      neo4j: Neo4j,
      customCypherVersion: String,
      cypherVersionPreamble: String,
      tuningOptions: Map[String, String],
      tuningPrefix: String
    ): Unit = {
      val optionsMap = Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.QUERY.toString.toLowerCase -> "MATCH (o:Object) RETURN o",
        "script" -> "return 'foo'",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      )

      val neo4jOptions = new Neo4jOptions(optionsMap ++ tuningOptions)
      val versionedReadService = new Neo4jQueryService(neo4jOptions, plainReadStrategy(neo4j, neo4jOptions))

      val wantQuery =
        s"$tuningPrefix\n${cypherVersionPreamble}WITH $$scriptResult AS scriptResult MATCH (o:Object) RETURN o"

      assertThat(versionedReadService.createQuery().trim).isEqualTo(wantQuery.trim)
    }

    private def plainReadStrategy(neo4j: Neo4j, neo4jOptions: Neo4jOptions): Neo4jQueryReadStrategy =
      new Neo4jQueryReadStrategy(neo4j, new CypherRenderer(neo4j, neo4jOptions))
  }

  @Nested
  @DisplayName("generates write cypher for")
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class WriteQueryServiceTest {

    import Neo4jQueryServiceTest._

    private def versions_and_prefixes() = _versions_and_prefixes()

    private def tuning_parameters() = _tuning_parameters()

    private def all_params_cross_combined() = _all_params_cross_combined()

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def labels_when_multiple_labels(neo4j: Neo4j, customCypherVersion: String, prefix: String): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        QueryType.LABELS.toString.toLowerCase -> ":Person:Player:Midfield",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String =
        new Neo4jQueryService(neo4jOptions, plainWriteStrategy(neo4j, neo4jOptions, SaveMode.Append)).createQuery().trim

      assertThat(query).isEqualTo(
        s"${prefix}UNWIND $$events AS event CREATE (node:`Person`:`Player`:`Midfield`) SET node += event.properties"
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def labels_when_composite_key_is_used(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "labels" -> "Location",
        "node.keys" -> "LocationName:name,LocationType:type,FeatureID:featureId",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String =
        new Neo4jQueryService(neo4jOptions, plainWriteStrategy(neo4j, neo4jOptions)).createQuery()

      assertThat(query).isEqualTo(
        s"""${prefix}UNWIND $$events AS event
           |MERGE (node:`Location` {name: event.keys.name, type: event.keys.type, featureId: event.keys.featureId})
           |SET node += event.properties
           |""".stripMargin.trim.replaceAll("\n", " ")
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def relationship_when_composite_key_is_used(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "BOUGHT",
        "relationship.source.labels" -> ":Person",
        "relationship.source.node.keys" -> "FirstName:name,LastName:lastName",
        "relationship.target.labels" -> ":Product:Merch",
        "relationship.target.node.keys" -> "ProductPrice:price,ProductId:id",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String =
        new Neo4jQueryService(neo4jOptions, plainWriteStrategy(neo4j, neo4jOptions)).createQuery()

      assertThat(query.trim).isEqualTo(
        s"""${prefix}UNWIND $$events AS event
           |MATCH (source:`Person` {name: event.source.keys.name, lastName: event.source.keys.lastName})
           |MATCH (target:`Product`:`Merch` {price: event.target.keys.price, id: event.target.keys.id})
           |MERGE (source)-[rel:`BOUGHT`]->(target)
           |SET rel += event.rel.properties
           |""".stripMargin.replaceAll("\n", " ").trim
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def relationship_when_source_merges_and_target_matches(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "BOUGHT",
        "relationship.source.labels" -> "Person",
        "relationship.source.node.keys" -> "FirstName:name,LastName:lastName",
        "relationship.source.save.mode" -> "Overwrite",
        "relationship.target.labels" -> "Product",
        "relationship.target.node.keys" -> "ProductPrice:price,ProductId:id",
        "relationship.target.save.mode" -> "Match",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String =
        new Neo4jQueryService(neo4jOptions, plainWriteStrategy(neo4j, neo4jOptions)).createQuery()

      assertThat(query.trim).isEqualTo(
        s"""${prefix}UNWIND $$events AS event
           |MERGE (source:`Person` {name: event.source.keys.name, lastName: event.source.keys.lastName}) SET source += event.source.properties
           |WITH source, event
           |MATCH (target:`Product` {price: event.target.keys.price, id: event.target.keys.id})
           |MERGE (source)-[rel:`BOUGHT`]->(target)
           |SET rel += event.rel.properties
           |""".stripMargin.replaceAll("\n", " ").trim
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def relationship_when_source_merges_and_target_merges(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "BOUGHT",
        "relationship.source.labels" -> "Person",
        "relationship.source.node.keys" -> "FirstName:name,LastName:lastName",
        "relationship.source.save.mode" -> "Overwrite",
        "relationship.target.labels" -> "Product",
        "relationship.target.node.keys" -> "ProductPrice:price,ProductId:id",
        "relationship.target.save.mode" -> "Overwrite",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String =
        new Neo4jQueryService(neo4jOptions, plainWriteStrategy(neo4j, neo4jOptions)).createQuery()

      assertThat(query.trim).isEqualTo(
        s"""${prefix}UNWIND $$events AS event
           |MERGE (source:`Person` {name: event.source.keys.name, lastName: event.source.keys.lastName}) SET source += event.source.properties
           |MERGE (target:`Product` {price: event.target.keys.price, id: event.target.keys.id}) SET target += event.target.properties
           |MERGE (source)-[rel:`BOUGHT`]->(target)
           |SET rel += event.rel.properties
           |""".stripMargin.replaceAll("\n", " ").trim
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def relationship_when_source_creates_and_target_merges(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "BOUGHT",
        "relationship.source.labels" -> "Person",
        "relationship.source.node.keys" -> "FirstName:name,LastName:lastName",
        "relationship.source.save.mode" -> "Append",
        "relationship.target.labels" -> "Product",
        "relationship.target.node.keys" -> "ProductPrice:price,ProductId:id",
        "relationship.target.save.mode" -> "Overwrite",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String =
        new Neo4jQueryService(neo4jOptions, plainWriteStrategy(neo4j, neo4jOptions, SaveMode.Append)).createQuery()

      assertThat(query.trim).isEqualTo(
        s"""${prefix}UNWIND $$events AS event
           |CREATE (source:`Person` {name: event.source.keys.name, lastName: event.source.keys.lastName}) SET source += event.source.properties
           |MERGE (target:`Product` {price: event.target.keys.price, id: event.target.keys.id}) SET target += event.target.properties
           |CREATE (source)-[rel:`BOUGHT`]->(target)
           |SET rel += event.rel.properties
           |""".stripMargin.replaceAll("\n", " ").trim
      )
    }

    @ParameterizedTest
    @MethodSource(Array("versions_and_prefixes"))
    def relationship_when_key_save_strategy(
      neo4j: Neo4j,
      customCypherVersion: String,
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(Map(
        Neo4jOptions.URL -> "bolt://localhost",
        "relationship" -> "DID BUY",
        "relationship.save.strategy" -> "keys",
        "relationship.source.labels" -> ":Person:Customer",
        "relationship.source.save.mode" -> "Overwrite",
        "relationship.source.node.keys" -> "first name,last name",
        "relationship.target.labels" -> "Product",
        "relationship.target.save.mode" -> "Match",
        "relationship.target.node.keys" -> "article number",
        "relationship.properties" -> "number of items",
        "relationship.keys" -> "transactionId:transaction identifier",
        Neo4jOptions.CYPHER_VERSION -> customCypherVersion
      ))

      val query: String =
        new Neo4jQueryService(neo4jOptions, plainWriteStrategy(neo4j, neo4jOptions)).createQuery()

      assertThat(query.trim).isEqualTo(
        s"""|${prefix}UNWIND $$events AS event
            |MERGE (source:`Person`:`Customer` {`first name`: event.source.keys.`first name`, `last name`: event.source.keys.`last name`}) SET source += event.source.properties
            |WITH source, event
            |MATCH (target:`Product` {`article number`: event.target.keys.`article number`})
            |MERGE (source)-[rel:`DID BUY` {`transaction identifier`: event.rel.keys.transactionId}]->(target)
            |SET rel += event.rel.properties
            |""".stripMargin.replaceAll("\n", " ").trim
      )
    }

    @ParameterizedTest
    @MethodSource(Array("tuning_parameters"))
    def labels_when_tuning_parameters(tuningOptions: Map[String, String], prefix: String): Unit = {
      val neo4jOptions = new Neo4jOptions(
        Map(
          Neo4jOptions.URL -> "bolt://localhost",
          QueryType.LABELS.toString.toLowerCase -> "Person"
        ) ++ tuningOptions
      )
      val neo4jInfo = neo4j(version(5, 0), COMMUNITY)

      val writeService = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryWriteStrategy(neo4jInfo, new CypherRenderer(neo4jInfo, neo4jOptions), SaveMode.Overwrite)
      )

      val wantQuery = "UNWIND $events AS event MERGE (node:`Person`) SET node += event.properties"
      assertThat(writeService.createQuery().trim).isEqualTo(s"$prefix\n$wantQuery".trim)
    }

    @ParameterizedTest
    @MethodSource(Array("tuning_parameters"))
    def relationship_when_tuning_parameters(
      tuningOptions: Map[String, String],
      prefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(
        Map(
          Neo4jOptions.URL -> "bolt://localhost",
          "relationship" -> "KNOWS",
          "relationship.nodes.map" -> "false",
          "relationship.source.labels" -> "Person",
          "relationship.target.labels" -> "Person"
        ) ++ tuningOptions
      )
      val neo4jInfo = neo4j(version(5, 0), COMMUNITY)

      val writeService = new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryWriteStrategy(neo4jInfo, new CypherRenderer(neo4jInfo, neo4jOptions), SaveMode.Overwrite)
      )
      val wantQuery =
        """UNWIND $events AS event
          |MATCH (source:`Person`)
          |MATCH (target:`Person`)
          |MERGE (source)-[rel:`KNOWS`]->(target)
          |SET rel += event.rel.properties""".stripMargin.replaceAll("\n", " ")

      assertThat(writeService.createQuery().trim).isEqualTo(s"$prefix\n$wantQuery".trim)
    }

    @ParameterizedTest
    @MethodSource(Array("tuning_parameters"))
    def custom_query_without_preamble_options_does_not_generate_any_preamble(
      tuningOptions: Map[String, String],
      ignored: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(
        Map(
          Neo4jOptions.URL -> "bolt://localhost",
          QueryType.QUERY.toString.toLowerCase -> "MATCH (o:Object) RETURN o"
        ) ++ tuningOptions
      )
      val neo4jInfo = neo4j(version(5, 0), COMMUNITY)

      val noPreambleWriteService =
        new Neo4jQueryService(
          neo4jOptions,
          new Neo4jQueryWriteStrategy(neo4jInfo, new CypherRenderer(neo4jInfo, neo4jOptions), SaveMode.Overwrite, false)
        )

      val wantQuery = "UNWIND $events AS event MATCH (o:Object) RETURN o"
      assertThat(noPreambleWriteService.createQuery().trim).isEqualTo(wantQuery)
    }

    @ParameterizedTest
    @MethodSource(Array("all_params_cross_combined"))
    def custom_query_properly_embeds_cypher_version_and_tuning_preamble(
      neo4j: Neo4j,
      customCypherVersion: String,
      cypherVersionPreamble: String,
      tuningOptions: Map[String, String],
      tuningPrefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(
        Map(
          Neo4jOptions.URL -> "bolt://localhost",
          QueryType.QUERY.toString.toLowerCase -> "MATCH (o:Object) RETURN o",
          Neo4jOptions.CYPHER_VERSION -> customCypherVersion
        ) ++ tuningOptions
      )

      val writeService =
        new Neo4jQueryService(neo4jOptions, plainWriteStrategy(neo4j, neo4jOptions))

      val wantQuery = s"$tuningPrefix\n${cypherVersionPreamble}UNWIND $$events AS event MATCH (o:Object) RETURN o"
      assertThat(writeService.createQuery().trim).isEqualTo(wantQuery.trim)
    }

    @ParameterizedTest
    @MethodSource(Array("all_params_cross_combined"))
    def custom_query_properly_embeds_script_results_when_script_is_present(
      neo4j: Neo4j,
      customCypherVersion: String,
      cypherVersionPreamble: String,
      tuningOptions: Map[String, String],
      tuningPrefix: String
    ): Unit = {
      val neo4jOptions = new Neo4jOptions(
        Map(
          Neo4jOptions.URL -> "bolt://localhost",
          QueryType.QUERY.toString.toLowerCase -> "MATCH (o:Object) RETURN o",
          "script" -> "return 'foo'",
          Neo4jOptions.CYPHER_VERSION -> customCypherVersion
        ) ++ tuningOptions
      )

      val versionedWriteService =
        new Neo4jQueryService(neo4jOptions, plainWriteStrategy(neo4j, neo4jOptions))

      val wantQuery =
        s"$tuningPrefix\n${cypherVersionPreamble}WITH $$scriptResult AS scriptResult UNWIND $$events AS event MATCH (o:Object) RETURN o"

      assertThat(versionedWriteService.createQuery().trim).isEqualTo(wantQuery.trim)
    }

    def plainWriteStrategy(
      neo4j: Neo4j,
      neo4jOptions: Neo4jOptions,
      saveMode: SaveMode = SaveMode.Overwrite
    ): Neo4jQueryWriteStrategy =
      new Neo4jQueryWriteStrategy(neo4j, new CypherRenderer(neo4j, neo4jOptions), saveMode)
  }
}

object Neo4jQueryServiceTest {

  def neo4j(version: Neo4jVersion, edition: Neo4jEdition): Neo4j = {
    new Neo4j(version, edition, SELF_MANAGED, Collections.emptySet())
  }

  def version(major: Int, minor: Int): Neo4jVersion = {
    new Neo4jVersion(major, minor, 0)
  }

  // Parameter methods
  def _versions_and_prefixes(): Array[Array[Any]] = {
    Array(
      Array(neo4j(version(5, 0), COMMUNITY), "", ""),
      Array(neo4j(version(5, 0), ENTERPRISE), "", ""),
      Array(neo4j(version(5, 21), COMMUNITY), "", "CYPHER 5 "),
      Array(neo4j(version(5, 21), ENTERPRISE), "", "CYPHER 5 "),
      Array(neo4j(version(5, 26), COMMUNITY), "", "CYPHER 5 "),
      Array(neo4j(version(5, 26), ENTERPRISE), "", "CYPHER 5 "),
      Array(neo4j(version(2025, 1), COMMUNITY), "", "CYPHER 5 "),
      Array(neo4j(version(2025, 1), ENTERPRISE), "", "CYPHER 5 "),
      Array(neo4j(version(5, 0), COMMUNITY), "25", "CYPHER 25 "),
      Array(neo4j(version(5, 0), ENTERPRISE), "25", "CYPHER 25 "),
      Array(neo4j(version(5, 0), COMMUNITY), "5", "CYPHER 5 "),
      Array(neo4j(version(5, 0), ENTERPRISE), "5", "CYPHER 5 ")
    )
  }

  def _tuning_parameters(): Array[Array[Any]] = {
    Array(
      Array(Map.empty[String, String], ""),
      Array(Map[String, String]("cypher.tuning.withCustom" -> "set"), "CYPHER withCustom=set")
    )
  }

  def _all_params_cross_combined(): Array[Array[Any]] = {
    val tuningParameters = _tuning_parameters()
    val versionParameters = _versions_and_prefixes()

    for {
      versionParameter <- versionParameters
      tuningParameter <- tuningParameters
    } yield Array(
      versionParameter(0), // neo4j: Neo4j
      versionParameter(1), // customVersion: String
      versionParameter(2), // versionPrefix: String
      tuningParameter(0), // tuningOptions: Map[String, String]
      tuningParameter(1) // tuningPrefix: String
    )
  }
}
