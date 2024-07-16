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

import org.junit.Assert._
import org.junit.Test
import org.neo4j.driver.AccessMode
import org.neo4j.driver.net.ServerAddress

import java.net.URI

import scala.annotation.meta.getter
import scala.collection.JavaConverters._

class Neo4jOptionsTest {

  import org.junit.Rule
  import org.junit.rules.ExpectedException

  @(Rule @getter)
  val _expectedException: ExpectedException = ExpectedException.none

  @Test
  def testUrlIsRequired(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(QueryType.QUERY.toString.toLowerCase, "Person")

    _expectedException.expect(classOf[IllegalArgumentException])
    _expectedException.expectMessage("Parameter 'url' is required")

    new Neo4jOptions(options)
  }

  @Test
  def testRelationshipTableName(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.RELATIONSHIP.toString.toLowerCase, "KNOWS")
    options.put(Neo4jOptions.RELATIONSHIP_SOURCE_LABELS, "Person")
    options.put(Neo4jOptions.RELATIONSHIP_TARGET_LABELS, "Answer")

    val neo4jOptions = new Neo4jOptions(options)

    assertEquals("table_Person_KNOWS_Answer", neo4jOptions.getTableName)
  }

  @Test
  def testLabelsTableName(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("labels", "Person:Admin")

    val neo4jOptions = new Neo4jOptions(options)

    assertEquals("table_Person-Admin", neo4jOptions.getTableName)
  }

  @Test
  def testRelationshipNodeModesAreCaseInsensitive(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.RELATIONSHIP.toString.toLowerCase, "KNOWS")
    options.put(Neo4jOptions.RELATIONSHIP_SAVE_STRATEGY, "nAtIve")
    options.put(Neo4jOptions.RELATIONSHIP_SOURCE_SAVE_MODE, "Errorifexists")
    options.put(Neo4jOptions.RELATIONSHIP_TARGET_SAVE_MODE, "overwrite")

    val neo4jOptions = new Neo4jOptions(options)

    assertEquals(RelationshipSaveStrategy.NATIVE, neo4jOptions.relationshipMetadata.saveStrategy)
    assertEquals(NodeSaveMode.ErrorIfExists, neo4jOptions.relationshipMetadata.sourceSaveMode)
    assertEquals(NodeSaveMode.Overwrite, neo4jOptions.relationshipMetadata.targetSaveMode)
  }

  @Test
  def testRelationshipWriteStrategyIsNotPresentShouldThrowException(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "PERSON")
    options.put("relationship.save.strategy", "nope")

    _expectedException.expect(classOf[NoSuchElementException])
    _expectedException.expectMessage("No value found for 'NOPE'")

    new Neo4jOptions(options)
  }

  @Test
  def testQueryShouldHaveQueryType(): Unit = {
    val query: String = "MATCH n RETURN n"
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.QUERY.toString.toLowerCase, query)

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    assertEquals(QueryType.QUERY, neo4jOptions.query.queryType)
    assertEquals(query, neo4jOptions.query.value)
  }

  @Test
  def testNodeShouldHaveLabelType(): Unit = {
    val label: String = "Person"
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, label)

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    assertEquals(QueryType.LABELS, neo4jOptions.query.queryType)
    assertEquals(label, neo4jOptions.query.value)
  }

  @Test
  def testRelationshipShouldHaveRelationshipType(): Unit = {
    val relationship: String = "KNOWS"
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, relationship)

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    assertEquals(QueryType.LABELS, neo4jOptions.query.queryType)
    assertEquals(relationship, neo4jOptions.query.value)
  }

  @Test
  def testPushDownColumnIsDisabled(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("pushdown.columns.enabled", "false")

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    assertFalse(neo4jOptions.pushdownColumnsEnabled)
  }

  @Test
  def testDriverDefaults(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.QUERY.toString.toLowerCase, "MATCH n RETURN n")

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    assertEquals("", neo4jOptions.session.database)
    assertEquals(AccessMode.READ, neo4jOptions.session.accessMode)

    assertEquals("basic", neo4jOptions.connection.auth)
    assertEquals("", neo4jOptions.connection.username)
    assertEquals("", neo4jOptions.connection.password)
    assertEquals(false, neo4jOptions.connection.encryption)

    assertEquals(None, neo4jOptions.connection.trustStrategy)

    assertEquals("", neo4jOptions.connection.certificatePath)
    assertEquals("", neo4jOptions.connection.ticket)
    assertEquals("", neo4jOptions.connection.principal)
    assertEquals("", neo4jOptions.connection.credentials)
    assertEquals("", neo4jOptions.connection.realm)
    assertEquals("", neo4jOptions.connection.schema)

    assertEquals(Neo4jOptions.DEFAULT_CONNECTION_MAX_LIFETIME_MSECS, neo4jOptions.connection.lifetime)
    assertEquals(-1, neo4jOptions.connection.acquisitionTimeout)
    assertEquals(-1, neo4jOptions.connection.connectionTimeout)
    assertEquals(
      Neo4jOptions.DEFAULT_CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS,
      neo4jOptions.connection.livenessCheckTimeout
    )
    assertEquals(RelationshipSaveStrategy.NATIVE, neo4jOptions.relationshipMetadata.saveStrategy)

    assertTrue(neo4jOptions.pushdownFiltersEnabled)
  }

  @Test
  def testApocConfiguration(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put("apoc.meta.nodeTypeProperties", """{"nodeLabels": ["Label"], "mandatory": false}""")
    options.put(Neo4jOptions.URL, "bolt://localhost")

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val expected = Map("apoc.meta.nodeTypeProperties" -> Map(
      "nodeLabels" -> Seq("Label").asJava,
      "mandatory" -> false
    ))

    assertEquals(neo4jOptions.apocConfig.procedureConfigMap, expected)
  }

  @Test
  def testUnexistingProperty(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put("relationship.properties", null)
    options.put(Neo4jOptions.URL, "bolt://localhost")

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    assertEquals(neo4jOptions.relationshipMetadata.properties, Map.empty)
  }

  @Test
  def testUrls(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "neo4j://localhost, neo4j://foo.bar:7687, neo4j://foo.bar.baz:7783")

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)
    val (baseUrl, resolvers) = neo4jOptions.connection.connectionUrls

    assertEquals(URI.create("neo4j://localhost"), baseUrl)
    assertEquals(Set(ServerAddress.of("foo.bar", 7687), ServerAddress.of("foo.bar.baz", 7783)), resolvers)
  }

  @Test
  def testGdsProperties(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "neo4j://localhost,neo4j://foo.bar,neo4j://foo.bar.baz:7783")
    options.put("gds", "gds.pageRank.stream")
    options.put("gds.graphName", "myGraph")
    options.put("gds.configuration.concurrency", "2")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)
    assertEquals(QueryType.GDS, neo4jOptions.query.queryType)
    assertEquals("gds.pageRank.stream", neo4jOptions.query.value)
    assertEquals(
      Map(
        "graphName" -> "myGraph",
        "configuration" -> Map("concurrency" -> 2).asJava
      ).asJava,
      neo4jOptions.gdsMetadata.parameters
    )
  }
}
