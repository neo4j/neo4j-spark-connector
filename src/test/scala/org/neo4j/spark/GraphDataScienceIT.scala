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
package org.neo4j.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, DisplayName, Test}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.neo4j.spark.testsupport.Closeables.use
import org.neo4j.spark.testsupport.SparkConnectorScalaSuiteWithGdsBase
import org.neo4j.spark.testsupport.TestUtil
import org.neo4j.spark.testsupport.Versions

import scala.math.Ordering.Implicits.infixOrderingOps

@DisplayName("graph data science")
class GraphDataScienceIT extends SparkConnectorScalaSuiteWithGdsBase {

  private val dataSourceFormat = classOf[DataSource].getName
  private val boltUrl = SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl

  @AfterEach
  def cleanData(): Unit = {
    use(SparkConnectorScalaSuiteWithGdsBase.session("system")) { session =>
      session.run("CREATE OR REPLACE DATABASE neo4j WAIT 30 seconds").consume()
    }

    use(SparkConnectorScalaSuiteWithGdsBase.session()) { session =>
      session
        .executeWrite(tx =>
          tx.run(
            """
              |CALL gds.graph.list() YIELD graphName
              |WITH graphName AS g
              |CALL gds.graph.drop(g) YIELD graphName
              |RETURN *
              |""".stripMargin
          ).consume()
        )
    }
  }

  @Test
  def returns_the_page_rank(): Unit = {
    initForPageRank()

    val df = read(
      "gds" -> "gds.pageRank.stream",
      "gds.graphName" -> "myGraph",
      "gds.configuration.concurrency" -> "2"
    )
    assertEquals(df.count(), 8)

    assertEquals(StructType(Array(StructField("nodeId", LongType), StructField("score", DoubleType))), df.schema)

    val dfEstimate = read(
      "gds" -> "gds.pageRank.stream.estimate",
      "gds.graphNameOrConfiguration" -> "myGraph",
      "gds.algoConfiguration.concurrency" -> "2"
    )
    assertEquals(dfEstimate.count(), 1)
    dfEstimate.show(false)

    assertEquals(
      StructType(
        Array(
          StructField("requiredMemory", StringType),
          StructField("treeView", StringType),
          StructField("mapView", MapType(StringType, StringType)),
          StructField("bytesMin", LongType),
          StructField("bytesMax", LongType),
          StructField("nodeCount", LongType),
          StructField("relationshipCount", LongType),
          StructField("heapPercentageMin", DoubleType),
          StructField("heapPercentageMax", DoubleType)
        )
      ),
      dfEstimate.schema
    )
  }

  @Test
  def fails_with_unsupported_options(): Unit = {
    initForPageRank()

    def run(options: Map[String, String], error: String): Unit = {
      try {
        ss.read.format(dataSourceFormat)
          .option("url", boltUrl)
          .options(options)
          .load()
          .show(false)
        fail("Expected to throw an exception")
      } catch {
        case iae: IllegalArgumentException =>
          assertTrue(iae.getMessage.equals(error))
        case _: Throwable =>
          fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}")
      }
    }

    run(
      Map(
        "gds" -> "gds.pageRank.stream",
        "gds.graphName" -> "myGraph",
        "gds.configuration.concurrency" -> "2",
        "partitions" -> "2"
      ),
      "For GDS queries we support only one partition"
    )

    run(
      Map(
        "gds" -> "gds.pageRank.write",
        "gds.graphName" -> "myGraph",
        "gds.configuration.concurrency" -> "2"
      ),
      "You cannot execute GDS mutate or write procedure in a read query"
    )

    run(
      Map(
        "gds" -> "gds.pageRank.mutate",
        "gds.graphName" -> "myGraph",
        "gds.configuration.concurrency" -> "2"
      ),
      "You cannot execute GDS mutate or write procedure in a read query"
    )
  }

  @Test
  def works_with_map_return(): Unit = {
    initForHits()

    val procName = if (TestUtil.gdsVersion(SparkConnectorScalaSuiteWithGdsBase.session()) >= Versions.GDS_2_5)
      "gds.hits.stream"
    else "gds.alpha.hits.stream"
    val df = read(
      "gds" -> procName,
      "gds.graphName" -> "myGraph",
      "gds.configuration.hitsIterations" -> "20"
    )
    assertEquals(df.count(), 9)

    assertEquals(
      StructType(Array(StructField("nodeId", LongType), StructField("values", MapType(StringType, StringType)))),
      df.schema
    )
  }

  @Test
  def works_with_path_return(): Unit = {
    initForYens()

    val sourceTargetNodes = read("labels" -> "Location")
      .where("name IN ('A', 'F')")
      .orderBy("name")
      .collect()

    // TODO temporary: GDS sourceNode/targetNode rejects elementId strings, so we parse the
    // numeric tail of the elementId
    val (sourceId, targetId) =
      (
        sourceTargetNodes(0).getAs[String]("<elementId>").split(":").last.toLong,
        sourceTargetNodes(1).getAs[String]("<elementId>").split(":").last.toLong
      )

    val df = read(
      "gds" -> "gds.shortestPath.yens.stream",
      "gds.graphName" -> "myGraph",
      "gds.configuration.sourceNode" -> sourceId,
      "gds.configuration.targetNode" -> targetId,
      "gds.configuration.k" -> 3,
      "gds.configuration.relationshipWeightProperty" -> "cost"
    )
    assertEquals(df.count(), 3)

    assertEquals(
      StructType(
        Array(
          StructField("index", LongType),
          StructField("sourceNode", LongType),
          StructField("targetNode", LongType),
          StructField("totalCost", DoubleType),
          StructField("nodeIds", ArrayType(LongType)),
          StructField("costs", ArrayType(DoubleType)),
          StructField("path", StringType)
        )
      ),
      df.schema
    )

    val (graphNameParam, algoConfigurationParam) =
      if (TestUtil.gdsVersion(SparkConnectorScalaSuiteWithGdsBase.session()) >= Versions.GDS_2_4)
        ("graphName", "configuration")
      else ("graphNameOrConfiguration", "algoConfiguration")
    val dfEstimate = read(
      "gds" -> "gds.shortestPath.yens.stream.estimate",
      s"gds.$graphNameParam" -> "myGraph",
      s"gds.$algoConfigurationParam.sourceNode" -> sourceId,
      s"gds.$algoConfigurationParam.targetNode" -> targetId,
      s"gds.$algoConfigurationParam.k" -> 3,
      s"gds.$algoConfigurationParam.relationshipWeightProperty" -> "cost"
    )
    assertEquals(dfEstimate.count(), 1)
    dfEstimate.show(false)

    assertEquals(
      StructType(
        Array(
          StructField("requiredMemory", StringType),
          StructField("treeView", StringType),
          StructField("mapView", MapType(StringType, StringType)),
          StructField("bytesMin", LongType),
          StructField("bytesMax", LongType),
          StructField("nodeCount", LongType),
          StructField("relationshipCount", LongType),
          StructField("heapPercentageMin", DoubleType),
          StructField("heapPercentageMax", DoubleType)
        )
      ),
      dfEstimate.schema
    )
  }


  @Test
  def works_with_k_nearest(): Unit = {
    SparkConnectorScalaSuiteWithGdsBase.session()
      .executeWrite(tx =>
        tx.run(
          """
            |CREATE (alice:Person {name: 'Alice', age: 24, lotteryNumbers: [1, 3], embedding: [1.0, 3.0]})
            |CREATE (bob:Person {name: 'Bob', age: 73, lotteryNumbers: [1, 2, 3], embedding: [2.1, 1.6]})
            |CREATE (carol:Person {name: 'Carol', age: 24, lotteryNumbers: [3], embedding: [1.5, 3.1]})
            |CREATE (dave:Person {name: 'Dave', age: 48, lotteryNumbers: [2, 4], embedding: [0.6, 0.2]})
            |CREATE (eve:Person {name: 'Eve', age: 67, lotteryNumbers: [1, 5], embedding: [1.8, 2.7]});
            |""".stripMargin
        ).consume()
      )

    read(
      "gds" -> "gds.graph.project",
      "gds.graphName" -> "myGraph",
      "gds.nodeProjection.Person.properties" -> "['age','lotteryNumbers','embedding']",
      "gds.relationshipProjection" -> "*"
    ).show(false)

    val df = read(
      "gds" -> "gds.knn.stream",
      "gds.graphName" -> "myGraph",
      "gds.configuration.topK" -> 1,
      "gds.configuration.nodeProperties" -> "['age']",
      "gds.configuration.randomSeed" -> 1337,
      "gds.configuration.concurrency" -> 1,
      "gds.configuration.sampleRate" -> 1.0,
      "gds.configuration.deltaThreshold" -> 0.0
    )

    assertEquals(df.count(), 5)

    assertEquals(
      StructType(
        Array(
          StructField("node1", LongType),
          StructField("node2", LongType),
          StructField("similarity", DoubleType)
        )
      ),
      df.schema
    )

    val dfEstimate = read(
      "gds" -> "gds.knn.stream.estimate",
      "gds.graphNameOrConfiguration" -> "myGraph",
      "gds.algoConfiguration.topK" -> 1,
      "gds.algoConfiguration.nodeProperties" -> "['age']",
      "gds.algoConfiguration.randomSeed" -> 1337,
      "gds.algoConfiguration.concurrency" -> 1,
      "gds.algoConfiguration.sampleRate" -> 1.0,
      "gds.algoConfiguration.deltaThreshold" -> 0.0
    )
    assertEquals(dfEstimate.count(), 1)
    dfEstimate.show(false)

    assertEquals(
      StructType(
        Array(
          StructField("requiredMemory", StringType),
          StructField("treeView", StringType),
          StructField("mapView", MapType(StringType, StringType)),
          StructField("bytesMin", LongType),
          StructField("bytesMax", LongType),
          StructField("nodeCount", LongType),
          StructField("relationshipCount", LongType),
          StructField("heapPercentageMin", DoubleType),
          StructField("heapPercentageMax", DoubleType)
        )
      ),
      dfEstimate.schema
    )
  }

  private def initForPageRank(): Unit = {
    SparkConnectorScalaSuiteWithGdsBase.session()
      .executeWrite(tx =>
        tx.run(
          """
            |CREATE
            |  (home:Page {name:'Home'}),
            |  (about:Page {name:'About'}),
            |  (product:Page {name:'Product'}),
            |  (links:Page {name:'Links'}),
            |  (a:Page {name:'Site A'}),
            |  (b:Page {name:'Site B'}),
            |  (c:Page {name:'Site C'}),
            |  (d:Page {name:'Site D'}),
            |
            |  (home)-[:LINKS {weight: 0.2}]->(about),
            |  (home)-[:LINKS {weight: 0.2}]->(links),
            |  (home)-[:LINKS {weight: 0.6}]->(product),
            |  (about)-[:LINKS {weight: 1.0}]->(home),
            |  (product)-[:LINKS {weight: 1.0}]->(home),
            |  (a)-[:LINKS {weight: 1.0}]->(home),
            |  (b)-[:LINKS {weight: 1.0}]->(home),
            |  (c)-[:LINKS {weight: 1.0}]->(home),
            |  (d)-[:LINKS {weight: 1.0}]->(home),
            |  (links)-[:LINKS {weight: 0.8}]->(home),
            |  (links)-[:LINKS {weight: 0.05}]->(a),
            |  (links)-[:LINKS {weight: 0.05}]->(b),
            |  (links)-[:LINKS {weight: 0.05}]->(c),
            |  (links)-[:LINKS {weight: 0.05}]->(d);
            |""".stripMargin
        ).consume()
      )
    read(
      "gds" -> "gds.graph.project",
      "gds.graphName" -> "myGraph",
      "gds.nodeProjection" -> "Page",
      "gds.relationshipProjection" -> "LINKS",
      "gds.configuration.relationshipProperties" -> "weight"
    ).show(false)
  }

  private def initForHits(): Unit = {
    assumeTrue(TestUtil.neo4jVersion(SparkConnectorScalaSuiteWithGdsBase.session()) >= Versions.NEO4J_5)
    SparkConnectorScalaSuiteWithGdsBase.session()
      .executeWrite(tx =>
        tx.run(
          """
            |CREATE
            |  (a:Website {name: 'A'}),
            |  (b:Website {name: 'B'}),
            |  (c:Website {name: 'C'}),
            |  (d:Website {name: 'D'}),
            |  (e:Website {name: 'E'}),
            |  (f:Website {name: 'F'}),
            |  (g:Website {name: 'G'}),
            |  (h:Website {name: 'H'}),
            |  (i:Website {name: 'I'}),
            |
            |  (a)-[:LINK]->(b),
            |  (a)-[:LINK]->(c),
            |  (a)-[:LINK]->(d),
            |  (b)-[:LINK]->(c),
            |  (b)-[:LINK]->(d),
            |  (c)-[:LINK]->(d),
            |
            |  (e)-[:LINK]->(b),
            |  (e)-[:LINK]->(d),
            |  (e)-[:LINK]->(f),
            |  (e)-[:LINK]->(h),
            |
            |  (f)-[:LINK]->(g),
            |  (f)-[:LINK]->(i),
            |  (f)-[:LINK]->(h),
            |  (g)-[:LINK]->(h),
            |  (g)-[:LINK]->(i),
            |  (h)-[:LINK]->(i);
            |""".stripMargin
        ).consume()
      )
    read(
      "gds" -> "gds.graph.project",
      "gds.graphName" -> "myGraph",
      "gds.nodeProjection" -> "Website",
      "gds.relationshipProjection.LINK.indexInverse" -> "true"
    ).show(false)
  }

  private def initForYens(): Unit = {
    SparkConnectorScalaSuiteWithGdsBase.session()
      .executeWrite(tx =>
        tx.run(
          """
            |CREATE (a:Location {name: 'A'}),
            |       (b:Location {name: 'B'}),
            |       (c:Location {name: 'C'}),
            |       (d:Location {name: 'D'}),
            |       (e:Location {name: 'E'}),
            |       (f:Location {name: 'F'}),
            |       (a)-[:ROAD {cost: 50}]->(b),
            |       (a)-[:ROAD {cost: 50}]->(c),
            |       (a)-[:ROAD {cost: 100}]->(d),
            |       (b)-[:ROAD {cost: 40}]->(d),
            |       (c)-[:ROAD {cost: 40}]->(d),
            |       (c)-[:ROAD {cost: 80}]->(e),
            |       (d)-[:ROAD {cost: 30}]->(e),
            |       (d)-[:ROAD {cost: 80}]->(f),
            |       (e)-[:ROAD {cost: 40}]->(f);
            |""".stripMargin
        ).consume()
      )
    read(
      "gds" -> "gds.graph.project",
      "gds.graphName" -> "myGraph",
      "gds.nodeProjection" -> "Location",
      "gds.relationshipProjection" -> "ROAD",
      "gds.configuration.relationshipProperties" -> "cost"
    ).show(false)
  }

  private def read(options: (String, Any)*): DataFrame =
    options
      .foldLeft(ss.read.format(dataSourceFormat).option("url", boltUrl)) {
        case (reader, (key, value)) => reader.option(key, value.toString)
      }
      .load()
}
