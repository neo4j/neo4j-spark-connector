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
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Assumptions.assumeFalse
import org.junit.jupiter.api.Test
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
import org.neo4j.spark.util.DummyNamedReference
import org.neo4j.spark.util.Neo4jImplicits.CypherImplicits
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.Neo4jTuningOptions
import org.neo4j.spark.util.QueryType

import scala.collection.immutable.HashMap

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Neo4jQueryServiceTest {

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testNodeOneLabel(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String =
      new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(neo4j)).createQuery()

    assertEquals(s"${prefix}MATCH (n:`Person`) RETURN n", query)
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testNodeMultipleLabels(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, ":Person:Player:Midfield")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String =
      new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(neo4j)).createQuery()

    assertEquals(s"${prefix}MATCH (n:`Person`:`Player`:`Midfield`) RETURN n", query)
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testNodeMultipleLabelsWithPartitions(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, ":Person:Player:Midfield")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
        partitionPagination = PartitionPagination(0, 0, TopN(100))
      )
    ).createQuery()

    assertEquals(s"${prefix}MATCH (n:`Person`:`Player`:`Midfield`) RETURN n LIMIT 100", query)
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testNodeOneLabelWithOneSelectedColumn(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
        Array.empty[Filter],
        PartitionPagination.EMPTY,
        Seq("name")
      )
    ).createQuery()

    assertEquals(s"${prefix}MATCH (n:`Person`) RETURN n.name AS name", query)
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testNodeOneLabelWithMultipleColumnSelected(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
        Array.empty[Filter],
        PartitionPagination.EMPTY,
        List("name", "bornDate")
      )
    ).createQuery()

    assertEquals(s"${prefix}MATCH (n:`Person`) RETURN n.name AS name, n.bornDate AS bornDate", query)
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testNodeOneLabelWithInternalIdSelected(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
        Array.empty[Filter],
        PartitionPagination.EMPTY,
        List("<elementId>")
      )
    ).createQuery()

    assertEquals(s"${prefix}MATCH (n:`Person`) RETURN elementId(n) AS `<elementId>`", query)
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testNodeFilterEqualTo(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      EqualTo("name", "John Doe")
    )

    val query: String =
      new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(neo4j, filters)).createQuery()

    val paramName = "$" + "name".toParameterName("John Doe")

    assertEquals(s"${prefix}MATCH (n:`Person`) WHERE n.name = $paramName RETURN n", query)
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testNodeFilterEqualNullSafe(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      EqualNullSafe("name", "John Doe"),
      EqualTo("age", 36)
    )

    val query: String =
      new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(neo4j, filters)).createQuery()

    val nameParameterName = "$" + "name".toParameterName("John Doe")
    val ageParameterName = "$" + "age".toParameterName(36)

    assertEquals(
      s"""${prefix}MATCH (n:`Person`)
         | WHERE (((n.name IS NULL AND $nameParameterName IS NULL)
         | OR n.name = $nameParameterName) AND n.age = $ageParameterName)
         | RETURN n""".stripMargin.replaceAll("\n", ""),
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testNodeFilterEqualNullSafeWithNullValue(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      EqualNullSafe("name", null),
      EqualTo("age", 36)
    )

    val query: String =
      new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(neo4j, filters)).createQuery()

    val nameParameterName = "$" + "name".toParameterName(null)
    val ageParameterName = "$" + "age".toParameterName(36)

    assertEquals(
      s"${prefix}MATCH (n:`Person`) WHERE (((n.name IS NULL AND $nameParameterName IS NULL) OR n.name = $nameParameterName) AND n.age = $ageParameterName) RETURN n",
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testNodeFilterStartsEndsWith(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      StringStartsWith("name", "Person Name"),
      StringEndsWith("name", "Person Surname")
    )

    val query: String =
      new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(neo4j, filters)).createQuery()

    val nameOneParameterName = "$" + "name".toParameterName("Person Name")
    val nameTwoParameterName = "$" + "name".toParameterName("Person Surname")

    assertEquals(
      s"""${prefix}MATCH (n:`Person`)
         | WHERE (n.name STARTS WITH $nameOneParameterName
         | AND n.name ENDS WITH $nameTwoParameterName)
         | RETURN n""".stripMargin.replaceAll("\n", ""),
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testRelationshipWithOneColumnSelected(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
        Array.empty[Filter],
        PartitionPagination.EMPTY,
        List("source.name")
      )
    ).createQuery()

    assertEquals(
      s"${prefix}MATCH (source:`Person`) " +
        "MATCH (target:`Person`) " +
        "MATCH (source)-[rel:`KNOWS`]->(target) RETURN source.name AS `source.name`",
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testRelationshipWithMoreColumnSelected(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
        Array.empty[Filter],
        PartitionPagination.EMPTY,
        List("source.name", "<source.elementId>")
      )
    ).createQuery()

    assertEquals(
      s"${prefix}MATCH (source:`Person`) " +
        "MATCH (target:`Person`) " +
        s"MATCH (source)-[rel:`KNOWS`]->(target) RETURN source.name AS `source.name`, elementId(source) AS `<source.elementId>`",
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testRelationshipWithMoreColumnSelectedWithPartitions(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
        Array.empty[Filter],
        PartitionPagination(0, 0, TopN(limit = 100)),
        List("source.name", "<source.elementId>")
      )
    ).createQuery()

    assertEquals(
      s"""${prefix}MATCH (source:`Person`)
         |MATCH (target:`Person`)
         |MATCH (source)-[rel:`KNOWS`]->(target)
         |RETURN source.name AS `source.name`, elementId(source) AS `<source.elementId>`
         |LIMIT 100"""
        .stripMargin
        .replace(System.lineSeparator(), " "),
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testRelationshipWithMoreColumnsSelected(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
        Array.empty[Filter],
        PartitionPagination.EMPTY,
        List("source.name", "source.id", "rel.someprops", "target.date")
      )
    ).createQuery()

    assertEquals(
      s"${prefix}MATCH (source:`Person`) " +
        "MATCH (target:`Person`) " +
        "MATCH (source)-[rel:`KNOWS`]->(target) RETURN source.name AS `source.name`, source.id AS `source.id`, rel.someprops AS `rel.someprops`, target.date AS `target.date`",
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testRelationshipFilterEqualTo(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      EqualTo("source.name", "John Doe")
    )

    val query: String =
      new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(neo4j, filters)).createQuery()

    val parameterName = "$" + "source.name".toParameterName("John Doe")

    assertEquals(
      s"${prefix}MATCH (source:`Person`) " +
        "MATCH (target:`Person`) " +
        s"MATCH (source)-[rel:`KNOWS`]->(target) WHERE source.name = $parameterName RETURN rel, source AS source, target AS target",
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testRelationshipFilterNotEqualTo(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      Or(EqualTo("source.name", "John Doe"), EqualTo("target.name", "John Doe"))
    )

    val query: String =
      new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(neo4j, filters)).createQuery()

    val paramOneName = "$" + "source.name".toParameterName("John Doe")
    val paramTwoName = "$" + "target.name".toParameterName("John Doe")

    assertEquals(
      s"${prefix}MATCH (source:`Person`) " +
        "MATCH (target:`Person`) " +
        s"MATCH (source)-[rel:`KNOWS`]->(target) WHERE (source.name = $paramOneName OR target.name = $paramTwoName) RETURN rel, source AS source, target AS target",
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testRelationshipAndFilterEqualTo(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "true")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      EqualTo("source.id", "14"),
      EqualTo("target.id", "16")
    )

    val query: String =
      new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(neo4j, filters)).createQuery()

    val sourceIdParameterName = "$" + "source.id".toParameterName(14)
    val targetIdParameterName = "$" + "target.id".toParameterName(16)

    assertEquals(
      s"""${prefix}MATCH (source:`Person`)
         | MATCH (target:`Person`)
         | MATCH (source)-[rel:`KNOWS`]->(target)
         | WHERE (source.id = $sourceIdParameterName AND target.id = $targetIdParameterName)
         | RETURN rel, source AS source, target AS target
         |""".stripMargin.replaceAll("\n", ""),
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testComplexNodeConditions(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      Or(EqualTo("name", "John Doe"), EqualTo("name", "John Scofield")),
      Or(EqualTo("age", 15), GreaterThanOrEqual("age", 18)),
      Or(Not(EqualTo("age", 22)), Not(LessThan("age", 11)))
    )

    val query: String =
      new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(neo4j, filters)).createQuery()

    val parameterNames: Map[String, String] = HashMap(
      "name_1" -> "$".concat("name".toParameterName("John Doe")),
      "name_2" -> "$".concat("name".toParameterName("John Scofield")),
      "age_1" -> "$".concat("age".toParameterName(15)),
      "age_2" -> "$".concat("age".toParameterName(18)),
      "age_3" -> "$".concat("age".toParameterName(22)),
      "age_4" -> "$".concat("age".toParameterName(11))
    )

    assertEquals(
      s"""${prefix}MATCH (n:`Person`)
         | WHERE (((n.name = ${parameterNames("name_1")} OR n.name = ${parameterNames("name_2")})
         | AND (n.age = ${parameterNames("age_1")} OR n.age >= ${parameterNames("age_2")}))
         | AND (NOT (n.age = ${parameterNames("age_3")}) OR NOT (n.age < ${parameterNames("age_4")})))
         | RETURN n""".stripMargin.replaceAll("\n", ""),
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testRelationshipFilterComplexConditionsNoMap(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person:Customer")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      Or(
        Or(EqualTo("source.name", "John Doe"), EqualTo("target.name", "John Doraemon")),
        EqualTo("source.name", "Jane Doe")
      ),
      Or(EqualTo("target.age", 34), EqualTo("target.age", 18)),
      EqualTo("rel.score", 12)
    )

    val query: String =
      new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(neo4j, filters)).createQuery()

    val parameterNames = Map(
      "source.name_1" -> "$".concat("source.name".toParameterName("John Doe")),
      "target.name_1" -> "$".concat("target.name".toParameterName("John Doraemon")),
      "source.name_2" -> "$".concat("source.name".toParameterName("Jane Doe")),
      "target.age_1" -> "$".concat("target.age".toParameterName(34)),
      "target.age_2" -> "$".concat("target.age".toParameterName(18)),
      "rel.score" -> "$".concat("rel.score".toParameterName(12))
    )

    assertEquals(
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
         | RETURN rel, source AS source, target AS target""".stripMargin.replaceAll("\n", ""),
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testRelationshipFilterComplexConditionsWithMap(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "true")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person:Customer")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      Or(
        Or(EqualTo("source.name", "John Doe"), EqualTo("target.name", "John Doraemon")),
        EqualTo("source.name", "Jane Doe")
      ),
      Or(EqualTo("target.age", 34), EqualTo("target.age", 18)),
      EqualTo("rel.score", 12)
    )

    val query: String =
      new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(neo4j, filters)).createQuery()

    val parameterNames = Map(
      "source.name_1" -> "$".concat("source.name".toParameterName("John Doe")),
      "target.name_1" -> "$".concat("target.name".toParameterName("John Doraemon")),
      "source.name_2" -> "$".concat("source.name".toParameterName("Jane Doe")),
      "target.age_1" -> "$".concat("target.age".toParameterName(34)),
      "target.age_2" -> "$".concat("target.age".toParameterName(18)),
      "rel.score" -> "$".concat("rel.score".toParameterName(12))
    )

    assertEquals(
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
         |""".stripMargin.replaceAll("\n", ""),
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testCompoundKeysForNodes(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("labels", "Location")
    options.put("node.keys", "LocationName:name,LocationType:type,FeatureID:featureId")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String =
      new Neo4jQueryService(neo4jOptions, new Neo4jQueryWriteStrategy(neo4j, SaveMode.Overwrite)).createQuery()

    assertEquals(
      s"""${prefix}UNWIND $$events AS event
         |MERGE (node:Location {name: event.keys.name, type: event.keys.type, featureId: event.keys.featureId})
         |SET node += event.properties
         |""".stripMargin,
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testCompoundKeysForRelationship(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "BOUGHT")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.source.node.keys", "FirstName:name,LastName:lastName")
    options.put("relationship.target.labels", "Product")
    options.put("relationship.target.node.keys", "ProductPrice:price,ProductId:id")

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String =
      new Neo4jQueryService(neo4jOptions, new Neo4jQueryWriteStrategy(neo4j, SaveMode.Overwrite)).createQuery()

    assertEquals(
      s"""${prefix}UNWIND $$events AS event
         |MATCH (source:Person {name: event.source.keys.name, lastName: event.source.keys.lastName})
         |MATCH (target:Product {price: event.target.keys.price, id: event.target.keys.id})
         |MERGE (source)-[rel:BOUGHT]->(target)
         |SET rel += event.rel.properties
         |""".stripMargin,
      query.stripMargin
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testCompoundKeysForRelationshipMergeMatch(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "BOUGHT")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.source.node.keys", "FirstName:name,LastName:lastName")
    options.put("relationship.source.save.mode", "Overwrite")
    options.put("relationship.target.labels", "Product")
    options.put("relationship.target.node.keys", "ProductPrice:price,ProductId:id")
    options.put("relationship.target.save.mode", "match")

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String =
      new Neo4jQueryService(neo4jOptions, new Neo4jQueryWriteStrategy(neo4j, SaveMode.Overwrite)).createQuery()

    assertEquals(
      s"""${prefix}UNWIND $$events AS event
         |MERGE (source:Person {name: event.source.keys.name, lastName: event.source.keys.lastName}) SET source += event.source.properties
         |WITH source, event
         |MATCH (target:Product {price: event.target.keys.price, id: event.target.keys.id})
         |MERGE (source)-[rel:BOUGHT]->(target)
         |SET rel += event.rel.properties
         |""".stripMargin,
      query.stripMargin
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testRelationshipWithKeySaveStrategy(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "DID BUY")
    options.put("relationship.save.strategy", "keys")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.source.save.mode", "Overwrite")
    options.put("relationship.source.node.keys", "first name,last name")
    options.put("relationship.target.labels", "Product")
    options.put("relationship.target.save.mode", "Match")
    options.put("relationship.target.node.keys", "article number")
    options.put("relationship.properties", "number of items")
    options.put("relationship.keys", "transactionId:transaction identifier")

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String =
      new Neo4jQueryService(neo4jOptions, new Neo4jQueryWriteStrategy(neo4j, SaveMode.Overwrite)).createQuery()

    assertEquals(
      s"""|${prefix}UNWIND $$events AS event
          |MERGE (source:Person {`first name`: event.source.keys.`first name`, `last name`: event.source.keys.`last name`}) SET source += event.source.properties
          |WITH source, event
          |MATCH (target:Product {`article number`: event.target.keys.`article number`})
          |MERGE (source)-[rel:`DID BUY`{`transaction identifier`: event.rel.keys.transactionId}]->(target)
          |SET rel += event.rel.properties
          |""".stripMargin,
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testShouldDoSumAggregationOnLabels(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val ageField = new DummyNamedReference("age")
    var query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
        Array.empty[Filter],
        PartitionPagination.EMPTY,
        Seq("name", "SUM(DISTINCT age)", "SUM(age)"),
        Array(
          new Sum(ageField, false),
          new Sum(ageField, true)
        )
      )
    ).createQuery()

    assertEquals(
      s"${prefix}MATCH (n:`Person`) RETURN n.name AS name, sum(DISTINCT n.age) AS `SUM(DISTINCT age)`, sum(n.age) AS `SUM(age)`",
      query
    )

    val nameField = new DummyNamedReference("name")
    query = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
        Array.empty[Filter],
        PartitionPagination.EMPTY,
        Seq("name", "COUNT(DISTINCT name)", "COUNT(name)"),
        Array(
          new Count(nameField, false),
          new Count(nameField, true)
        )
      )
    ).createQuery()

    assertEquals(
      s"${prefix}MATCH (n:`Person`) RETURN n.name AS name, count(DISTINCT n.name) AS `COUNT(DISTINCT name)`, count(n.name) AS `COUNT(name)`",
      query
    )

    query = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
        Array.empty[Filter],
        PartitionPagination.EMPTY,
        Seq("name", "MAX(age)", "MIN(age)"),
        Array(
          new Max(ageField),
          new Min(ageField)
        )
      )
    ).createQuery()

    assertEquals(
      s"${prefix}MATCH (n:`Person`) RETURN n.name AS name, max(n.age) AS `MAX(age)`, min(n.age) AS `MIN(age)`",
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testShouldDoSumAggregationOnRelationships(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "BOUGHT")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Product")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val targetPriceField = new DummyNamedReference("`target.price`")
    var query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
        Array.empty,
        PartitionPagination.EMPTY,
        List("source.fullName", "SUM(DISTINCT `target.price`)", "SUM(`target.price`)"),
        Array(
          new Sum(targetPriceField, false),
          new Sum(targetPriceField, true)
        )
      )
    ).createQuery()

    assertEquals(
      s"""${prefix}MATCH (source:`Person`)
         |MATCH (target:`Product`)
         |MATCH (source)-[rel:`BOUGHT`]->(target)
         |RETURN source.fullName AS `source.fullName`, sum(DISTINCT target.price) AS `SUM(DISTINCT ``target.price``)`, sum(target.price) AS `SUM(``target.price``)`"""
        .stripMargin
        .replaceAll("\n", " "),
      query
    )

    val targetIdField = new DummyNamedReference("`target.id`")
    query = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
        Array.empty,
        PartitionPagination.EMPTY,
        List("source.fullName", "COUNT(DISTINCT `target.id`)", "COUNT(`target.id`)"),
        Array(
          new Count(targetIdField, false),
          new Count(targetIdField, true)
        )
      )
    ).createQuery()

    assertEquals(
      s"""${prefix}MATCH (source:`Person`) MATCH (target:`Product`)
         |MATCH (source)-[rel:`BOUGHT`]->(target)
         |RETURN source.fullName AS `source.fullName`, count(DISTINCT target.id) AS `COUNT(DISTINCT ``target.id``)`, count(target.id) AS `COUNT(``target.id``)`"""
        .stripMargin
        .replaceAll("\n", " "),
      query
    )

    query = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
        Array.empty,
        PartitionPagination.EMPTY,
        List("source.fullName", "MAX(`target.price`)", "MIN(`target.price`)"),
        Array(
          new Max(targetPriceField),
          new Min(targetPriceField)
        )
      )
    ).createQuery()

    assertEquals(
      s"""${prefix}MATCH (source:`Person`)
         |MATCH (target:`Product`)
         |MATCH (source)-[rel:`BOUGHT`]->(target)
         |RETURN source.fullName AS `source.fullName`, max(target.price) AS `MAX(``target.price``)`, min(target.price) AS `MIN(``target.price``)`"""
        .stripMargin
        .replaceAll("\n", " "),
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testTopNForLabels(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
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

    assertEquals(s"${prefix}MATCH (n:`Person`) RETURN n ORDER BY n.name ASC LIMIT 42", query)
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testTopNForLabelsWithRequiredColumn(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
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

    assertEquals(s"${prefix}MATCH (n:`Person`) RETURN n.name AS name ORDER BY n.name ASC LIMIT 42", query)
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testTopNForRelationships(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
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

    assertEquals(
      s"${prefix}MATCH (source:`Person`) " +
        "MATCH (target:`Person`) " +
        "MATCH (source)-[rel:`KNOWS`]->(target) RETURN rel, source AS source, target AS target " +
        "ORDER BY rel.since DESC LIMIT 24",
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testTopNForRelationshipWithOneRequiredColumn(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
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

    assertEquals(
      s"""${prefix}MATCH (source:`Person`)
         |MATCH (target:`Person`)
         |MATCH (source)-[rel:`KNOWS`]->(target) RETURN source.name AS `source.name`
         |ORDER BY rel.since DESC LIMIT 24"""
        .stripMargin
        .replaceAll("\n", " "),
      query
    )
  }

  @ParameterizedTest
  @MethodSource(Array("versions_and_prefixes"))
  def testTopNForCustomQueryIgnoresAggregation(neo4j: Neo4j, prefix: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.QUERY.toString.toLowerCase, "MATCH (p:Person) RETURN p")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
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

    assertEquals(s"${prefix}MATCH (p:Person) RETURN p SKIP 0 LIMIT 24", query)
  }

  @ParameterizedTest
  @MethodSource(Array("tuning_parameters"))
  def testTuningPreambleForLabels(tuningOptions: Neo4jTuningOptions, prefix: String, mode: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(withTuning(options, tuningOptions))

    val (strategy, wantQuery) = mode match {
      case "READ" => (
          new Neo4jQueryReadStrategy(
            neo4j(version(5, 0), COMMUNITY)
          ),
          "MATCH (n:`Person`) RETURN n"
        )
      case "WRITE" => (
          new Neo4jQueryWriteStrategy(
            neo4j(version(5, 0), COMMUNITY),
            SaveMode.Overwrite
          ),
          "UNWIND $events AS event\nMERGE (node:Person )\nSET node += event.properties"
        )
      case _ => throw new IllegalArgumentException(s"Invalid mode: $mode")
    }

    val gotQuery = new Neo4jQueryService(neo4jOptions, strategy).createQuery().trim

    assertEquals(s"$prefix\n$wantQuery".trim, gotQuery)
  }

  @ParameterizedTest
  @MethodSource(Array("tuning_parameters"))
  def testTuningPreambleForRelationship(tuningOptions: Neo4jTuningOptions, prefix: String, mode: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(withTuning(options, tuningOptions))

    val (strategy, wantQuery) = mode match {
      case "READ" => (
          new Neo4jQueryReadStrategy(
            neo4j(version(5, 0), COMMUNITY)
          ),
          "MATCH (source:`Person`) MATCH (target:`Person`) MATCH (source)-[rel:`KNOWS`]->(target) RETURN rel, source AS source, target AS target"
        )
      case "WRITE" => (
          new Neo4jQueryWriteStrategy(
            neo4j(version(5, 0), COMMUNITY),
            SaveMode.Overwrite
          ),
          "UNWIND $events AS event\nMATCH (source:Person )\nMATCH (target:Person )\nMERGE (source)-[rel:KNOWS]->(target)\nSET rel += event.rel.properties"
        )
    }

    val gotQuery = new Neo4jQueryService(neo4jOptions, strategy).createQuery().trim

    assertEquals(s"$prefix\n$wantQuery".trim, gotQuery)
  }

  @ParameterizedTest
  @MethodSource(Array("tuning_parameters"))
  def testTuningPreambleForCustomQuery(tuningOptions: Neo4jTuningOptions, prefix: String, mode: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.QUERY.toString.toLowerCase, "MATCH (o:Object) RETURN o")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(withTuning(options, tuningOptions))

    val (strategy, wantQuery) = mode match {
      case "READ" => (
          new Neo4jQueryReadStrategy(
            neo4j(version(5, 0), COMMUNITY)
          ),
          "MATCH (o:Object) RETURN o"
        )
      case "WRITE" => (
          new Neo4jQueryWriteStrategy(
            neo4j(version(5, 0), COMMUNITY),
            SaveMode.Overwrite
          ),
          "UNWIND $events AS event\nMATCH (o:Object) RETURN o"
        )
    }

    val gotQuery = new Neo4jQueryService(neo4jOptions, strategy).createQuery().trim

    assertEquals(s"$prefix\n$wantQuery".trim, gotQuery)
  }

  @ParameterizedTest
  @MethodSource(Array("tuning_parameters"))
  def testCanSkipPreamble(tuningOptions: Neo4jTuningOptions, ignored: String, mode: String): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.QUERY.toString.toLowerCase, "MATCH (o:Object) RETURN o")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(withTuning(options, tuningOptions))

    val (strategy, wantQuery) = mode match {
      case "READ" => (
          new Neo4jQueryReadStrategy(neo4j(version(5, 0), COMMUNITY), withPreamble = false),
          "MATCH (o:Object) RETURN o"
        )
      case "WRITE" => (
          new Neo4jQueryWriteStrategy(neo4j(version(5, 0), COMMUNITY), SaveMode.Overwrite, withPreamble = false),
          "UNWIND $events AS event\nMATCH (o:Object) RETURN o"
        )
    }

    val gotQuery = new Neo4jQueryService(neo4jOptions, strategy).createQuery().trim

    assertEquals(wantQuery, gotQuery)
  }

  @Test
  def testTuningAndVersionInclusionInCustomReadQuery(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.QUERY.toString.toLowerCase, "MATCH (o:Object) RETURN o")

    val tuningOptions = Neo4jTuningOptions.empty.copy(replan = "force", operatorEngine = "compiled")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(withTuning(options, tuningOptions))

    val actual = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(neo4j(version(2025, 1), ENTERPRISE))
    ).createQuery().trim

    assertEquals(
      "CYPHER replan=force operatorEngine=compiled\nCYPHER 5 MATCH (o:Object) RETURN o",
      actual
    )
  }

  @Test
  def testTuningAndVersionInclusionInCustomWriteQuery(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.QUERY.toString.toLowerCase, "MATCH (o:Object) RETURN o")

    val tuningOptions = Neo4jTuningOptions.empty.copy(replan = "force", operatorEngine = "compiled")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(withTuning(options, tuningOptions))

    val actual = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryWriteStrategy(neo4j(version(2025, 1), ENTERPRISE), SaveMode.Overwrite)
    ).createQuery().trim

    assertEquals(
      "CYPHER replan=force operatorEngine=compiled\nCYPHER 5 UNWIND $events AS event\nMATCH (o:Object) RETURN o",
      actual
    )
  }

  @Test
  def testScriptResultInclusionInCustomReadQuery(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.QUERY.toString.toLowerCase, "MATCH (o:Object) RETURN o")
    options.put("script", "return 'foo'")

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val actual = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(neo4j(version(5, 0), COMMUNITY))
    ).createQuery().trim

    assertEquals(
      "WITH $scriptResult AS scriptResult MATCH (o:Object) RETURN o",
      actual
    )
  }

  @Test
  def testScriptResultInclusionInCustomWriteQuery(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.QUERY.toString.toLowerCase, "MATCH (o:Object) RETURN o")
    options.put("script", "return 'foo'")

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val actual = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryWriteStrategy(neo4j(version(5, 0), COMMUNITY), SaveMode.Overwrite)
    ).createQuery().trim

    assertEquals(
      "WITH $scriptResult AS scriptResult UNWIND $events AS event\nMATCH (o:Object) RETURN o",
      actual
    )
  }

  def versions_and_prefixes(): Array[Array[Any]] = {
    Array(
      Array(neo4j(version(5, 0), COMMUNITY), ""),
      Array(neo4j(version(5, 0), ENTERPRISE), ""),
      Array(neo4j(version(5, 21), COMMUNITY), "CYPHER 5 "),
      Array(neo4j(version(5, 21), ENTERPRISE), "CYPHER 5 "),
      Array(neo4j(version(5, 26), COMMUNITY), "CYPHER 5 "),
      Array(neo4j(version(5, 26), ENTERPRISE), "CYPHER 5 "),
      Array(neo4j(version(2025, 1), COMMUNITY), "CYPHER 5 "),
      Array(neo4j(version(2025, 1), ENTERPRISE), "CYPHER 5 ")
    )
  }

  def neo4j(version: Neo4jVersion, edition: Neo4jEdition): Neo4j = {
    new Neo4j(version, edition, SELF_MANAGED)
  }

  def version(major: Int, minor: Int): Neo4jVersion = {
    new Neo4jVersion(major, minor, 0)
  }

  def tuning_parameters(): Array[Array[Any]] = {
    Array(
      Array(Neo4jTuningOptions.empty, "", "READ"),
      Array(Neo4jTuningOptions.empty, "", "WRITE"),
      Array(Neo4jTuningOptions.empty.copy(runtime = "parallel"), "CYPHER runtime=parallel", "READ"),
      Array(Neo4jTuningOptions.empty.copy(runtime = "parallel"), "CYPHER runtime=parallel", "WRITE"),
      Array(
        Neo4jTuningOptions.empty.copy(replan = "force", operatorEngine = "compiled"),
        "CYPHER replan=force operatorEngine=compiled",
        "READ"
      ),
      Array(
        Neo4jTuningOptions.empty.copy(replan = "force", operatorEngine = "compiled"),
        "CYPHER replan=force operatorEngine=compiled",
        "WRITE"
      )
    )
  }

  private def withTuning(
    options: java.util.Map[String, String],
    tuning: Neo4jTuningOptions
  ): java.util.Map[String, String] = {
    tuning.toMap.foreach {
      case (key, value) => {
        val dotCasedKey = key.replaceAll("([A-Z])", ".$1").toLowerCase
        options.put(s"cypher.$dotCasedKey", value)
      }
    }
    options
  }

}
