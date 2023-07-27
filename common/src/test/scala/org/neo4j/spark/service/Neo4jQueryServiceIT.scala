package org.neo4j.spark.service

import org.apache.spark.sql.connector.expressions.aggregate.{Count, Max, Min, Sum}
import org.junit.runners.MethodSorters
import org.junit.{After, Assert, FixMethodOrder, Test}
import org.neo4j.spark.SparkConnectorScalaSuiteWithGdsBase
import org.neo4j.spark.util.{DriverCache, DummyNamedReference, Neo4jOptions}

@FixMethodOrder(MethodSorters.JVM)
class Neo4jQueryServiceIT extends SparkConnectorScalaSuiteWithGdsBase {

  @After
  def cleanUp(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)
    new DriverCache(neo4jOptions.connection, "").close()
  }

  @Test
  def testShouldDoAggregationOnGDS(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, SparkConnectorScalaSuiteWithGdsBase.server.getBoltUrl)
    options.put("gds", "gds.pageRank.stream")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val field = new DummyNamedReference("score")
    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(
      Array.empty,
      PartitionPagination.EMPTY,
      List("nodeId",
        "MAX(score)",
        "MIN(score)",
        "COUNT(score)",
        "COUNT(DISTINCT score)",
        "SUM(score)",
        "SUM(DISTINCT score)"),
      Array(
        new Max(field),
        new Min(field),
        new Sum(field, false),
        new Count(field, false),
        new Count(field, true),
        new Sum(field, false),
        new Sum(field, true)
      )
    )).createQuery()

    Assert.assertEquals(
      """CALL gds.pageRank.stream($graphName)
        |YIELD nodeId, score
        |RETURN nodeId AS nodeId, max(score) AS `MAX(score)`, min(score) AS `MIN(score)`, count(score) AS `COUNT(score)`, count(DISTINCT score) AS `COUNT(DISTINCT score)`, sum(score) AS `SUM(score)`, sum(DISTINCT score) AS `SUM(DISTINCT score)`"""
        .stripMargin
        .replaceAll("\n", " "), query)
  }

}
