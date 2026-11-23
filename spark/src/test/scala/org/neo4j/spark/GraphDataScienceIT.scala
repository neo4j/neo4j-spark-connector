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
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.Parameter
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.neo4j.caniuse.Neo4j
import org.neo4j.driver.Driver
import org.neo4j.spark.cypher.CypherRenderer
import org.neo4j.spark.cypher.QueryEmbedder
import org.neo4j.spark.service.Neo4jQueryReadStrategy
import org.neo4j.spark.service.Neo4jQueryService
import org.neo4j.spark.service.PartitionPagination
import org.neo4j.spark.testsupport.Closeables.use
import org.neo4j.spark.testsupport.InjectNeo4jContainerParameter
import org.neo4j.spark.testsupport.Neo4jExtensions.DriverExtensions
import org.neo4j.spark.testsupport.Neo4jExtensions.Neo4jContainerExtensions
import org.neo4j.spark.testsupport.TestUtil
import org.neo4j.spark.testsupport.Versions
import org.neo4j.spark.util.DummyNamedReference
import org.neo4j.spark.util.Neo4jOptions
import org.testcontainers.neo4j.Neo4jContainer

import java.util.UUID

import scala.math.Ordering.Implicits.infixOrderingOps

@InjectNeo4jContainerParameter
@DisplayName("graph data science")
class GraphDataScienceIT {

  @Parameter
  var neo4jContainer: Neo4jContainer = _

  @BeforeEach
  def prepare(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
    assumeTrue(driver.serverSupportsGds())
  }

  @Test
  def runs_page_rank(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
    val graphName = unique("pagerank")
    val nodeLabel = unique("Page")
    val relationshipType = unique("LINKS")
    initForPageRank(driver, spark, graphName, nodeLabel, relationshipType)

    val df = spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.pageRank.stream")
      .option("gds.graphName", graphName)
      .option("gds.configuration.concurrency", "2")
      .load()
    assertThat(df.count()).isEqualTo(8)

    assertThat(df.schema)
      .isEqualTo(StructType(Array(StructField("nodeId", LongType), StructField("score", DoubleType))))

    val dfEstimate = spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.pageRank.stream.estimate")
      .option("gds.graphNameOrConfiguration", graphName)
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
  def fails_with_unsupported_options(
    testCase: UnsupportedOptionCase,
    driver: Driver,
    spark: SparkSession,
    neo4j: Neo4j
  ): Unit = {
    assertThatExceptionOfType(classOf[IllegalArgumentException])
      .isThrownBy(() => {
        spark.read.format(classOf[DataSource].getName)
          .options(testCase.options)
          .load()
          .count()
      })
      .withMessage(testCase.error)
  }

  @Test
  def hits_supports_map_results(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
    assumeTrue(use(driver.session())(s => TestUtil.neo4jVersion(s)) >= Versions.NEO4J_5)
    val graphName = unique("hits")
    val nodeLabel = unique("Website")
    val relationshipType = unique("LINK")
    initForHits(driver, spark, neo4j, graphName, nodeLabel, relationshipType)

    val df = spark.read.format(classOf[DataSource].getName)
      .option("gds", hitsStreamProc(driver))
      .option("gds.graphName", graphName)
      .option("gds.configuration.hitsIterations", "20")
      .load()
    assertThat(df.count()).isEqualTo(9)

    assertThat(df.schema).isEqualTo(
      StructType(Array(StructField("nodeId", LongType), StructField("values", MapType(StringType, StringType))))
    )
  }

  @Test
  def yens_shortest_path_supports_path_results(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
    val graphName = unique("yens")
    val nodeLabel = unique("Location")
    val relationshipType = unique("ROAD")
    initForYens(driver, spark, neo4j, graphName, nodeLabel, relationshipType)

    val sourceTargetNodes = spark.read.format(classOf[DataSource].getName)
      .option("labels", nodeLabel)
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
      .option("gds.graphName", graphName)
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
      .option(s"gds.$graphNameParam", graphName)
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
  def runs_k_nearest(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
    val graphName = unique("knn")
    val nodeLabel = unique("Person")
    driver.executableQuery(
      s"""
         |CREATE (alice:`$nodeLabel` {name: 'Alice', age: 24, lotteryNumbers: [1, 3], embedding: [1.0, 3.0]})
         |CREATE (bob:`$nodeLabel` {name: 'Bob', age: 73, lotteryNumbers: [1, 2, 3], embedding: [2.1, 1.6]})
         |CREATE (carol:`$nodeLabel` {name: 'Carol', age: 24, lotteryNumbers: [3], embedding: [1.5, 3.1]})
         |CREATE (dave:`$nodeLabel` {name: 'Dave', age: 48, lotteryNumbers: [2, 4], embedding: [0.6, 0.2]})
         |CREATE (eve:`$nodeLabel` {name: 'Eve', age: 67, lotteryNumbers: [1, 5], embedding: [1.8, 2.7]});
         |""".stripMargin
    ).execute()

    spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.graph.project")
      .option("gds.graphName", graphName)
      .option(s"gds.nodeProjection.$nodeLabel.properties", "['age','lotteryNumbers','embedding']")
      .option("gds.relationshipProjection", "*")
      .load()
      .count()

    val df = spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.knn.stream")
      .option("gds.graphName", graphName)
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
      .option("gds.graphNameOrConfiguration", graphName)
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
  def generates_read_query_that_aggregates(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
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
  def refuses_read_query_with_cypher_preamble(driver: Driver, spark: SparkSession, neo4j: Neo4j): Unit = {
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

  private def hitsStreamProc(driver: Driver): String =
    if (use(driver.session())(s => TestUtil.gdsVersion(s)) >= Versions.GDS_2_5) "gds.hits.stream"
    else "gds.alpha.hits.stream"

  private def unique(prefix: String): String =
    s"${prefix}_${UUID.randomUUID().toString.replace("-", "")}"

  private def initForPageRank(
    driver: Driver,
    spark: SparkSession,
    graphName: String,
    nodeLabel: String,
    relationshipType: String
  ): Unit = {
    driver.executableQuery(
      s"""
         |CREATE
         |  (home:`$nodeLabel` {name:'Home'}),
         |  (about:`$nodeLabel` {name:'About'}),
         |  (product:`$nodeLabel` {name:'Product'}),
         |  (links:`$nodeLabel` {name:'Links'}),
         |  (a:`$nodeLabel` {name:'Site A'}),
         |  (b:`$nodeLabel` {name:'Site B'}),
         |  (c:`$nodeLabel` {name:'Site C'}),
         |  (d:`$nodeLabel` {name:'Site D'}),
         |
         |  (home)-[:`$relationshipType` {weight: 0.2}]->(about),
         |  (home)-[:`$relationshipType` {weight: 0.2}]->(links),
         |  (home)-[:`$relationshipType` {weight: 0.6}]->(product),
         |  (about)-[:`$relationshipType` {weight: 1.0}]->(home),
         |  (product)-[:`$relationshipType` {weight: 1.0}]->(home),
         |  (a)-[:`$relationshipType` {weight: 1.0}]->(home),
         |  (b)-[:`$relationshipType` {weight: 1.0}]->(home),
         |  (c)-[:`$relationshipType` {weight: 1.0}]->(home),
         |  (d)-[:`$relationshipType` {weight: 1.0}]->(home),
         |  (links)-[:`$relationshipType` {weight: 0.8}]->(home),
         |  (links)-[:`$relationshipType` {weight: 0.05}]->(a),
         |  (links)-[:`$relationshipType` {weight: 0.05}]->(b),
         |  (links)-[:`$relationshipType` {weight: 0.05}]->(c),
         |  (links)-[:`$relationshipType` {weight: 0.05}]->(d);
         |""".stripMargin
    ).execute()
    spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.graph.project")
      .option("gds.graphName", graphName)
      .option("gds.nodeProjection", nodeLabel)
      .option("gds.relationshipProjection", relationshipType)
      .option("gds.configuration.relationshipProperties", "weight")
      .load()
      .count()
  }

  private def initForHits(
    driver: Driver,
    spark: SparkSession,
    neo4j: Neo4j,
    graphName: String,
    nodeLabel: String,
    relationshipType: String
  ): Unit = {
    driver.executableQuery(
      s"""
         |CREATE
         |  (a:`$nodeLabel` {name: 'A'}),
         |  (b:`$nodeLabel` {name: 'B'}),
         |  (c:`$nodeLabel` {name: 'C'}),
         |  (d:`$nodeLabel` {name: 'D'}),
         |  (e:`$nodeLabel` {name: 'E'}),
         |  (f:`$nodeLabel` {name: 'F'}),
         |  (g:`$nodeLabel` {name: 'G'}),
         |  (h:`$nodeLabel` {name: 'H'}),
         |  (i:`$nodeLabel` {name: 'I'}),
         |
         |  (a)-[:`$relationshipType`]->(b),
         |  (a)-[:`$relationshipType`]->(c),
         |  (a)-[:`$relationshipType`]->(d),
         |  (b)-[:`$relationshipType`]->(c),
         |  (b)-[:`$relationshipType`]->(d),
         |  (c)-[:`$relationshipType`]->(d),
         |
         |  (e)-[:`$relationshipType`]->(b),
         |  (e)-[:`$relationshipType`]->(d),
         |  (e)-[:`$relationshipType`]->(f),
         |  (e)-[:`$relationshipType`]->(h),
         |
         |  (f)-[:`$relationshipType`]->(g),
         |  (f)-[:`$relationshipType`]->(i),
         |  (f)-[:`$relationshipType`]->(h),
         |  (g)-[:`$relationshipType`]->(h),
         |  (g)-[:`$relationshipType`]->(i),
         |  (h)-[:`$relationshipType`]->(i);
         |""".stripMargin
    ).execute()
    spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.graph.project")
      .option("gds.graphName", graphName)
      .option("gds.nodeProjection", nodeLabel)
      .option(s"gds.relationshipProjection.$relationshipType.indexInverse", "true")
      .load()
      .count()
  }

  private def initForYens(
    driver: Driver,
    spark: SparkSession,
    neo4j: Neo4j,
    graphName: String,
    nodeLabel: String,
    relationshipType: String
  ): Unit = {
    driver.executableQuery(
      s"""
         |CREATE (a:`$nodeLabel` {name: 'A'}),
         |       (b:`$nodeLabel` {name: 'B'}),
         |       (c:`$nodeLabel` {name: 'C'}),
         |       (d:`$nodeLabel` {name: 'D'}),
         |       (e:`$nodeLabel` {name: 'E'}),
         |       (f:`$nodeLabel` {name: 'F'}),
         |       (a)-[:`$relationshipType` {cost: 50}]->(b),
         |       (a)-[:`$relationshipType` {cost: 50}]->(c),
         |       (a)-[:`$relationshipType` {cost: 100}]->(d),
         |       (b)-[:`$relationshipType` {cost: 40}]->(d),
         |       (c)-[:`$relationshipType` {cost: 40}]->(d),
         |       (c)-[:`$relationshipType` {cost: 80}]->(e),
         |       (d)-[:`$relationshipType` {cost: 30}]->(e),
         |       (d)-[:`$relationshipType` {cost: 80}]->(f),
         |       (e)-[:`$relationshipType` {cost: 40}]->(f);
         |""".stripMargin
    ).execute()
    spark.read.format(classOf[DataSource].getName)
      .option("gds", "gds.graph.project")
      .option("gds.graphName", graphName)
      .option("gds.nodeProjection", nodeLabel)
      .option("gds.relationshipProjection", relationshipType)
      .option("gds.configuration.relationshipProperties", "cost")
      .load()
      .count()
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
