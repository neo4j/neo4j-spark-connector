package org.neo4j.spark.service

import org.junit.Assert.assertEquals
import org.junit.Test
import org.mockito.Mockito.{RETURNS_DEEP_STUBS, mock}
import org.neo4j.caniuse.{Neo4j, Neo4jDeploymentType, Neo4jEdition, Neo4jVersion}
import org.neo4j.spark.config.TopN
import org.neo4j.spark.util.{DriverCache, Neo4jOptions}

import scala.jdk.javaapi.CollectionConverters


class SchemaServiceTest {

  @Test
  def does_not_overflow_when_partition_size_is_over_max_value_of_32bit_integers(): Unit = {
    val opts = options(
      "url" -> "bolt://example.com",
      "partitions" -> 2.toString,
      "query.count" -> (2L * 2_147_483_648L).toString, // 2 * (Integer.MAX_VALUE + 1)
      "query" -> "MERGE (:Node)",
    )
    val schemaService = new SchemaService(neo4j(), opts, mock(classOf[DriverCache], RETURNS_DEEP_STUBS))

    val pages = schemaService.skipLimitFromPartition(Some(TopN(1024)))

    assertEquals(List(0, 1), pages.map(_.partitionNumber).toList)
    assertEquals(List(0, 2_147_483_648L), pages.map(_.skip).toList)
    assertEquals(List(2_147_483_648L, 2_147_483_648L), pages.map(_.topN.limit).toList)
    assertEquals(List(0, 0), pages.map(_.topN.orders.size).toList)
  }

  private def options(kv: (String, String)*): Neo4jOptions = {
    new Neo4jOptions(
      CollectionConverters.asJava(kv.toMap)
    )
  }

  private def neo4j(): Neo4j = {
    new Neo4j(new Neo4jVersion(2025, 1, 0), Neo4jEdition.COMMUNITY, Neo4jDeploymentType.SELF_MANAGED)
  }
}
