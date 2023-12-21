package org.neo4j.spark.streaming

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.neo4j.spark.reader.BasePartitionReader
import org.neo4j.spark.service.{Neo4jQueryStrategy, PartitionPagination}
import org.neo4j.spark.util.Neo4jImplicits._
import org.neo4j.spark.util.{Neo4jOptions, Neo4jUtil, StreamingFrom}

import java.util
import java.util.Collections

class BaseStreamingPartitionReader(private val options: Neo4jOptions,
                                   private val filters: Array[Filter],
                                   private val schema: StructType,
                                   private val jobId: String,
                                   private val partitionSkipLimit: PartitionPagination,
                                   private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
                                   private val requiredColumns: StructType,
                                   private val aggregateColumns: Array[AggregateFunc]) extends BasePartitionReader(options,
    filters,
    schema,
    jobId,
    partitionSkipLimit,
    scriptResult,
    requiredColumns,
    aggregateColumns) {

  private val streamingPropertyName = Neo4jUtil.getStreamingPropertyName(options)

  private val streamingField = filters.find(f => f.getAttribute.contains(streamingPropertyName))

  logInfo(s"Creating Streaming Partition reader $name")

  private lazy val values = {
    val map = new util.HashMap[String, Any](super.getQueryParameters)
    val value = streamingField
      .flatMap(_.getValue)
      .get
    map.put(Neo4jQueryStrategy.VARIABLE_STREAM, Collections.singletonMap("offset", value))
    map
  }


  override protected def getQueryParameters: util.Map[String, Any] = values

}
