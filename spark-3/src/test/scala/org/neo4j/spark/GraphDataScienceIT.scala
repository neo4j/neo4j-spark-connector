package org.neo4j.spark

import org.apache.spark.sql.types.{ArrayType, DoubleType, LongType, MapType, StringType, StructField, StructType}
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.{After, Assume, Test}
import org.neo4j.driver.Transaction


class GraphDataScienceIT extends SparkConnectorScalaSuiteWithGdsBase {

  @After
  def cleanData(): Unit = {
    SparkConnectorScalaSuiteWithGdsBase.session()
      .writeTransaction(
        (tx: Transaction) => {
          tx.run("MATCH (n) DETACH DELETE n").consume()
        })
    SparkConnectorScalaSuiteWithGdsBase.session()
      .writeTransaction(
        (tx: Transaction) => {
          tx.run(
            """
              |CALL gds.graph.list() YIELD graphName
              |WITH graphName AS g
              |CALL gds.graph.drop(g) YIELD graphName
              |RETURN *
              |""".stripMargin).consume()
        })
  }

  @Test
  def shouldReturnThePageRank(): Unit = {
    initForPageRank()

    val df = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
      .option("gds", "gds.pageRank.stream")
      .option("gds.graphName", "myGraph")
      .option("gds.configuration.concurrency", "2")
      .load()
    assertEquals(df.count(), 8)
    df.show(false)

    assertEquals(StructType(Array(StructField("nodeId", LongType), StructField("score", DoubleType))), df.schema)

    val dfEstimate = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
      .option("gds", "gds.pageRank.stream.estimate")
      .option("gds.graphNameOrConfiguration", "myGraph")
      .option("gds.algoConfiguration.concurrency", "2")
      .load()
    assertEquals(dfEstimate.count(), 1)
    dfEstimate.show(false)

    assertEquals(StructType(
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
    ), dfEstimate.schema)
  }

  @Test
  def shouldFailWithUnsupportedOptions(): Unit = {
    initForPageRank()

    def run(options: Map[String, String], error: String): Unit = {
      try {
        ss.read.format(classOf[DataSource].getName)
          .option("url", SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
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
        "gds.configuration.concurrency"-> "2",
        "partitions"-> "2"
      ),
      "For GDS queries we support only one partition"
    )

    run(
      Map(
        "gds" -> "gds.pageRank.write",
        "gds.graphName" -> "myGraph",
        "gds.configuration.concurrency" -> "2",
      ),
      "You cannot execute GDS mutate or write procedure in a read query"
    )

    run(
      Map(
        "gds" -> "gds.pageRank.mutate",
        "gds.graphName" -> "myGraph",
        "gds.configuration.concurrency" -> "2",
      ),
      "You cannot execute GDS mutate or write procedure in a read query"
    )
  }

  @Test
  def shouldWorkWithMapReturn(): Unit = {
    initForHits()

    val procName = if (TestUtil.neo4jVersionAsDouble() >= 5.13) "gds.hits.stream" else "gds.alpha.hits.stream"
    val df = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
      .option("gds", procName)
      .option("gds.graphName", "myGraph")
      .option("gds.configuration.hitsIterations", "20")
      .load()
    assertEquals(df.count(), 9)
    df.show(false)

    assertEquals(StructType(Array(StructField("nodeId", LongType),
      StructField("values", MapType(StringType, StringType)))), df.schema)
  }

  @Test
  def shouldWorkWithPathReturn(): Unit = {
    initForYens()

    val sourceTargetNodes = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
      .option("labels", "Location")
      .load()
      .where("name IN ('A', 'F')")
      .orderBy("name")
      .collect()

    val (sourceId, targetId) = (sourceTargetNodes(0).getAs[Long]("<id>"),
      sourceTargetNodes(1).getAs[Long]("<id>"))

    val df = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
      .option("gds", "gds.shortestPath.yens.stream")
      .option("gds.graphName", "myGraph")
      .option("gds.configuration.sourceNode", sourceId)
      .option("gds.configuration.targetNode", targetId)
      .option("gds.configuration.k", 3)
      .option("gds.configuration.relationshipWeightProperty", "cost")
      .load()
    assertEquals(df.count(), 3)
    df.show(false)

    assertEquals(StructType(
      Array(
        StructField("index", LongType),
        StructField("sourceNode", LongType),
        StructField("targetNode", LongType),
        StructField("totalCost", DoubleType),
        StructField("nodeIds", ArrayType(LongType)),
        StructField("costs", ArrayType(DoubleType)),
        StructField("path", StringType)
      )
    ), df.schema)

    val (graphNameParam, algoConfigurationParam) = if (TestUtil.neo4jVersionAsDouble() >= 5.13)
      ("graphName", "configuration") else ("graphNameOrConfiguration", "algoConfiguration")
    val dfEstimate = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
      .option("gds", "gds.shortestPath.yens.stream.estimate")
      .option(s"gds.$graphNameParam", "myGraph")
      .option(s"gds.$algoConfigurationParam.sourceNode", sourceId)
      .option(s"gds.$algoConfigurationParam.targetNode", targetId)
      .option(s"gds.$algoConfigurationParam.k", 3)
      .option(s"gds.$algoConfigurationParam.relationshipWeightProperty", "cost")
      .load()
    assertEquals(dfEstimate.count(), 1)
    dfEstimate.show(false)

    assertEquals(StructType(
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
    ), dfEstimate.schema)
  }

  private def initForYens(): Unit = {
    SparkConnectorScalaSuiteWithGdsBase.session()
      .writeTransaction(
        (tx: Transaction) => {
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
              |""".stripMargin).consume()
        })
    ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
      .option("gds", "gds.graph.project")
      .option("gds.graphName", "myGraph")
      .option("gds.nodeProjection", "Location")
      .option("gds.relationshipProjection", "ROAD")
      .option("gds.configuration.relationshipProperties", "cost")
      .load()
      .show(false)
  }

  @Test
  def shouldWorkWithKNearest(): Unit = {
    SparkConnectorScalaSuiteWithGdsBase.session()
      .writeTransaction(
        (tx: Transaction) => {
          tx.run(
            """
              |CREATE (alice:Person {name: 'Alice', age: 24, lotteryNumbers: [1, 3], embedding: [1.0, 3.0]})
              |CREATE (bob:Person {name: 'Bob', age: 73, lotteryNumbers: [1, 2, 3], embedding: [2.1, 1.6]})
              |CREATE (carol:Person {name: 'Carol', age: 24, lotteryNumbers: [3], embedding: [1.5, 3.1]})
              |CREATE (dave:Person {name: 'Dave', age: 48, lotteryNumbers: [2, 4], embedding: [0.6, 0.2]})
              |CREATE (eve:Person {name: 'Eve', age: 67, lotteryNumbers: [1, 5], embedding: [1.8, 2.7]});
              |""".stripMargin).consume()
        })

    ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
      .option("gds", "gds.graph.project")
      .option("gds.graphName", "myGraph")
      .option("gds.nodeProjection.Person.properties", "['age','lotteryNumbers','embedding']")
      .option("gds.relationshipProjection", "*")
      .load()
      .show(false)

    val df = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
      .option("gds", "gds.knn.stream")
      .option("gds.graphName", "myGraph")
      .option("gds.configuration.topK", 1)
      .option("gds.configuration.nodeProperties", "['age']")
      .option("gds.configuration.randomSeed", 1337)
      .option("gds.configuration.concurrency", 1)
      .option("gds.configuration.sampleRate", 1.0)
      .option("gds.configuration.deltaThreshold", 0.0)
      .load()

    assertEquals(df.count(), 5)
    df.show(false)

    assertEquals(StructType(
      Array(
        StructField("node1", LongType),
        StructField("node2", LongType),
        StructField("similarity", DoubleType),
      )
    ), df.schema)

    val dfEstimate = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
      .option("gds", "gds.knn.stream.estimate")
      .option("gds.graphNameOrConfiguration", "myGraph")
      .option("gds.algoConfiguration.topK", 1)
      .option("gds.algoConfiguration.nodeProperties", "['age']")
      .option("gds.algoConfiguration.randomSeed", 1337)
      .option("gds.algoConfiguration.concurrency", 1)
      .option("gds.algoConfiguration.sampleRate", 1.0)
      .option("gds.algoConfiguration.deltaThreshold", 0.0)
      .load()
    assertEquals(dfEstimate.count(), 1)
    dfEstimate.show(false)

    assertEquals(StructType(
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
    ), dfEstimate.schema)
  }

  private def initForPageRank(): Unit = {
    SparkConnectorScalaSuiteWithGdsBase.session()
      .writeTransaction(
        (tx: Transaction) => {
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
              |""".stripMargin).consume()
        })
    ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
      .option("gds", "gds.graph.project")
      .option("gds.graphName", "myGraph")
      .option("gds.nodeProjection", "Page")
      .option("gds.relationshipProjection", "LINKS")
      .option("gds.configuration.relationshipProperties", "weight")
      .load()
      .show(false)
  }

  private def initForHits(): Unit = {
    val isNeo4j4: Boolean = SparkConnectorScalaSuiteWithGdsBase.session()
      .writeTransaction(
        (tx: Transaction) => {
          tx.run(
            """
              |CALL dbms.components() YIELD versions
              |RETURN versions[0] starts with "4";
              |""".stripMargin)
            .single()
            .get(0)
            .asBoolean()
        })
    Assume.assumeFalse(isNeo4j4)
    SparkConnectorScalaSuiteWithGdsBase.session()
      .writeTransaction(
        (tx: Transaction) => {
          tx.run(
            """
              CREATE
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
              |""".stripMargin).consume()
        })
    ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
      .option("gds", "gds.graph.project")
      .option("gds.graphName", "myGraph")
      .option("gds.nodeProjection", "Website")
      .option("gds.relationshipProjection.LINK.indexInverse", "true")
      .load()
      .show(false)
  }
}

