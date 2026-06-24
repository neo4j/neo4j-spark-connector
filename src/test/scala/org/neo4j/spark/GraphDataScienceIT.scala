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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.aggregate.Count
import org.apache.spark.sql.connector.expressions.aggregate.Max
import org.apache.spark.sql.connector.expressions.aggregate.Min
import org.apache.spark.sql.connector.expressions.aggregate.Sum
import org.apache.spark.sql.types._
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.Parameter
import org.junit.jupiter.params.ParameterizedClass
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ArgumentsSource
import org.junit.jupiter.params.provider.MethodSource
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDetector
import org.neo4j.driver.Driver
import org.neo4j.spark.cypher.CypherRenderer
import org.neo4j.spark.cypher.QueryEmbedder
import org.neo4j.spark.service.Neo4jQueryReadStrategy
import org.neo4j.spark.service.Neo4jQueryService
import org.neo4j.spark.service.PartitionPagination
import org.neo4j.spark.testsupport.Closeables.use
import org.neo4j.spark.testsupport.Neo4jContainerProvider
import org.neo4j.spark.testsupport.Neo4jExtensions.DriverExtensions
import org.neo4j.spark.testsupport.Neo4jExtensions.Neo4jContainerExtensions
import org.neo4j.spark.testsupport.TestUtil
import org.neo4j.spark.testsupport.Versions
import org.neo4j.spark.util.DummyNamedReference
import org.neo4j.spark.util.Neo4jOptions
import org.testcontainers.neo4j.Neo4jContainer

import scala.math.Ordering.Implicits.infixOrderingOps

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ParameterizedClass(name = "{argumentSetName}")
@ArgumentsSource(classOf[Neo4jContainerProvider])
@DisplayName("graph data science")
class GraphDataScienceIT {

  @Parameter
  var neo4jContainer: Neo4jContainer = _

  var driver: Driver = _

  var spark: SparkSession = _

  var neo4j: Neo4j = _

  @BeforeEach
  def prepare(): Unit = {
    if (!neo4jContainer.isRunning) {
      neo4jContainer.start()
    }
    driver = neo4jContainer.driver()
    Assumptions.assumeTrue(driver.serverSupportsGds())
    driver.createOrReplaceDatabase("neo4j")
    spark = neo4jContainer.spark()
    neo4j = Neo4jDetector.INSTANCE.detect(driver)
  }

  @AfterEach
  def cleanUp(): Unit = {
    Option(driver).filter(_.serverSupportsGds()).foreach { d =>
      d.executableQuery("CALL gds.graph.drop('myGraph', false)").execute()
    }
    Option(spark).foreach(_.close())
    Option(driver).foreach(_.close())
  }

  @Test
  def runs_page_rank(): Unit = {
    initForPageRank()

    val df = spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.pageRank.stream")
      .option("gds.graphName", "myGraph")
      .option("gds.configuration.concurrency", "2")
      .load()
    assertThat(df.count()).isEqualTo(8)

    assertThat(df.schema)
      .isEqualTo(StructType(Array(StructField("nodeId", LongType), StructField("score", DoubleType))))

    val dfEstimate = spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.pageRank.stream.estimate")
      .option("gds.graphNameOrConfiguration", "myGraph")
      .option("gds.algoConfiguration.concurrency", "2")
      .load()
    assertThat(dfEstimate.count()).isEqualTo(1)

    assertThat(dfEstimate.schema).isEqualTo(
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
      )
    )
  }

  @ParameterizedTest
  @MethodSource(Array("unsupportedOptionCases"))
  def fails_with_unsupported_options(testCase: UnsupportedOptionCase): Unit = {
    assertThatExceptionOfType(classOf[IllegalArgumentException])
      .isThrownBy(() => {
        spark.read.format(classOf[DataSource].getName)
          .options(testCase.options)
          .load()
          .show(false)
      })
      .withMessage(testCase.error)
  }

  @Test
  def hits_supports_map_results(): Unit = {
    assumeTrue(use(driver.session())(s => TestUtil.neo4jVersion(s)) >= Versions.NEO4J_5)
    initForHits()

    val df = spark.read.format(classOf[DataSource].getName)
      .option("gds", hitsStreamProc)
      .option("gds.graphName", "myGraph")
      .option("gds.configuration.hitsIterations", "20")
      .load()
    assertThat(df.count()).isEqualTo(9)

    assertThat(df.schema).isEqualTo(
      StructType(Array(StructField("nodeId", LongType), StructField("values", MapType(StringType, StringType))))
    )
  }

  @Test
  def yens_shortest_path_supports_path_results(): Unit = {
    initForYens()

    val sourceTargetNodes = spark.read.format(classOf[DataSource].getName)
      .option("labels", "Location")
      .load()
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

    val df = spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.shortestPath.yens.stream")
      .option("gds.graphName", "myGraph")
      .option("gds.configuration.sourceNode", sourceId)
      .option("gds.configuration.targetNode", targetId)
      .option("gds.configuration.k", 3)
      .option("gds.configuration.relationshipWeightProperty", "cost")
      .load()
    assertThat(df.count()).isEqualTo(3)

    assertThat(df.schema).isEqualTo(
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
      )
    )

    val (graphNameParam, algoConfigurationParam) =
      if (use(driver.session())(s => TestUtil.gdsVersion(s)) >= Versions.GDS_2_4)
        ("graphName", "configuration")
      else ("graphNameOrConfiguration", "algoConfiguration")
    val dfEstimate = spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.shortestPath.yens.stream.estimate")
      .option(s"gds.$graphNameParam", "myGraph")
      .option(s"gds.$algoConfigurationParam.sourceNode", sourceId)
      .option(s"gds.$algoConfigurationParam.targetNode", targetId)
      .option(s"gds.$algoConfigurationParam.k", 3)
      .option(s"gds.$algoConfigurationParam.relationshipWeightProperty", "cost")
      .load()
    assertThat(dfEstimate.count()).isEqualTo(1)

    assertThat(dfEstimate.schema).isEqualTo(
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
      )
    )
  }

  @Test
  def runs_k_nearest(): Unit = {
    driver.executableQuery(
      """
        |CREATE (alice:Person {name: 'Alice', age: 24, lotteryNumbers: [1, 3], embedding: [1.0, 3.0]})
        |CREATE (bob:Person {name: 'Bob', age: 73, lotteryNumbers: [1, 2, 3], embedding: [2.1, 1.6]})
        |CREATE (carol:Person {name: 'Carol', age: 24, lotteryNumbers: [3], embedding: [1.5, 3.1]})
        |CREATE (dave:Person {name: 'Dave', age: 48, lotteryNumbers: [2, 4], embedding: [0.6, 0.2]})
        |CREATE (eve:Person {name: 'Eve', age: 67, lotteryNumbers: [1, 5], embedding: [1.8, 2.7]});
        |""".stripMargin
    ).execute()

    spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.graph.project")
      .option("gds.graphName", "myGraph")
      .option("gds.nodeProjection.Person.properties", "['age','lotteryNumbers','embedding']")
      .option("gds.relationshipProjection", "*")
      .load()
      .show(false)

    val df = spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.knn.stream")
      .option("gds.graphName", "myGraph")
      .option("gds.configuration.topK", 1)
      .option("gds.configuration.nodeProperties", "['age']")
      .option("gds.configuration.randomSeed", 1337)
      .option("gds.configuration.concurrency", 1)
      .option("gds.configuration.sampleRate", 1.0)
      .option("gds.configuration.deltaThreshold", 0.0)
      .load()

    assertThat(df.count()).isEqualTo(5)

    assertThat(df.schema).isEqualTo(
      StructType(
        Array(
          StructField("node1", LongType),
          StructField("node2", LongType),
          StructField("similarity", DoubleType)
        )
      )
    )

    val dfEstimate = spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.knn.stream.estimate")
      .option("gds.graphNameOrConfiguration", "myGraph")
      .option("gds.algoConfiguration.topK", 1)
      .option("gds.algoConfiguration.nodeProperties", "['age']")
      .option("gds.algoConfiguration.randomSeed", 1337)
      .option("gds.algoConfiguration.concurrency", 1)
      .option("gds.algoConfiguration.sampleRate", 1.0)
      .option("gds.algoConfiguration.deltaThreshold", 0.0)
      .load()
    assertThat(dfEstimate.count()).isEqualTo(1)

    assertThat(dfEstimate.schema).isEqualTo(
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
      )
    )
  }

  @Test
  def generates_read_query_that_aggregates(): Unit = {
    val neo4jOptions = new Neo4jOptions(neo4jContainer.authenticatedOptions() ++ Map("gds" -> "gds.pageRank.stream"))

    val field = new DummyNamedReference("score")
    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(
        neo4j,
        new CypherRenderer(neo4j, neo4jOptions),
        new QueryEmbedder(),
        Array.empty,
        PartitionPagination.EMPTY,
        List(
          "nodeId",
          "MAX(score)",
          "MIN(score)",
          "COUNT(score)",
          "COUNT(DISTINCT score)",
          "SUM(score)",
          "SUM(DISTINCT score)"
        ),
        Array(
          new Max(field),
          new Min(field),
          new Sum(field, false),
          new Count(field, false),
          new Count(field, true),
          new Sum(field, false),
          new Sum(field, true)
        )
      )
    ).createQuery()

    assertThat(query).endsWith(
      """CALL gds.pageRank.stream($graphName)
        |YIELD nodeId, score
        |RETURN nodeId AS nodeId, max(score) AS `MAX(score)`, min(score) AS `MIN(score)`, count(score) AS `COUNT(score)`, count(DISTINCT score) AS `COUNT(DISTINCT score)`, sum(score) AS `SUM(score)`, sum(DISTINCT score) AS `SUM(DISTINCT score)`"""
        .stripMargin
        .replaceAll("\n", " ")
    )
  }

  @Test
  def refuses_read_query_with_cypher_preamble(): Unit = {
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(
      neo4jContainer.authenticatedOptions() ++ Map(
        "gds" -> "gds.pageRank.stream",
        "cypher.tuning.expressionEngine" -> "compiled"
      )
    )

    val field = new DummyNamedReference("score")

    assertThatThrownBy(() => {
      new Neo4jQueryService(
        neo4jOptions,
        new Neo4jQueryReadStrategy(
          neo4j,
          new CypherRenderer(neo4j, neo4jOptions),
          new QueryEmbedder(),
          Array.empty,
          PartitionPagination.EMPTY,
          List(
            "nodeId",
            "MAX(score)",
            "MIN(score)",
            "COUNT(score)",
            "COUNT(DISTINCT score)",
            "SUM(score)",
            "SUM(DISTINCT score)"
          ),
          Array(
            new Max(field),
            new Min(field),
            new Sum(field, false),
            new Count(field, false),
            new Count(field, true),
            new Sum(field, false),
            new Sum(field, true)
          )
        )
      ).createQuery()
    })
      .isInstanceOf(classOf[UnsupportedOperationException])
      .hasMessageContaining("Query tuning parameters are not supported for GDS queries")
  }

  private def hitsStreamProc: String =
    if (use(driver.session())(s => TestUtil.gdsVersion(s)) >= Versions.GDS_2_5) "gds.hits.stream"
    else "gds.alpha.hits.stream"

  private def initForPageRank(): Unit = {
    driver.executableQuery(
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
    ).execute()
    spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.graph.project")
      .option("gds.graphName", "myGraph")
      .option("gds.nodeProjection", "Page")
      .option("gds.relationshipProjection", "LINKS")
      .option("gds.configuration.relationshipProperties", "weight")
      .load()
      .show(false)
  }

  private def initForHits(): Unit = {
    driver.executableQuery(
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
    ).execute()
    spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.graph.project")
      .option("gds.graphName", "myGraph")
      .option("gds.nodeProjection", "Website")
      .option("gds.relationshipProjection.LINK.indexInverse", "true")
      .load()
      .show(false)
  }

  private def initForYens(): Unit = {
    driver.executableQuery(
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
    ).execute()
    spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.graph.project")
      .option("gds.graphName", "myGraph")
      .option("gds.nodeProjection", "Location")
      .option("gds.relationshipProjection", "ROAD")
      .option("gds.configuration.relationshipProperties", "cost")
      .load()
      .show(false)
  }
}

object GraphDataScienceIT {

  def unsupportedOptionCases: java.util.stream.Stream[UnsupportedOptionCase] =
    java.util.stream.Stream.of(
      UnsupportedOptionCase(
        Map(
          "gds" -> "gds.pageRank.stream",
          "gds.graphName" -> "myGraph",
          "gds.configuration.concurrency" -> "2",
          "partitions" -> "2"
        ),
        "For GDS queries we support only one partition"
      ),
      UnsupportedOptionCase(
        Map(
          "gds" -> "gds.pageRank.write",
          "gds.graphName" -> "myGraph",
          "gds.configuration.concurrency" -> "2"
        ),
        "You cannot execute GDS mutate or write procedure in a read query"
      ),
      UnsupportedOptionCase(
        Map(
          "gds" -> "gds.pageRank.mutate",
          "gds.graphName" -> "myGraph",
          "gds.configuration.concurrency" -> "2"
        ),
        "You cannot execute GDS mutate or write procedure in a read query"
      )
    )
}

case class UnsupportedOptionCase(options: Map[String, String], error: String)
