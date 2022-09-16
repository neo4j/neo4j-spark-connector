package org.neo4j.spark.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.service.PartitionSkipLimit
import org.neo4j.spark.util.Neo4jOptions

class Neo4jPartitionReaderFactory(private val neo4jOptions: Neo4jOptions,
                                  private val filters: Array[Filter],
                                  private val schema: StructType,
                                  private val jobId: String,
                                  private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
                                  private val requiredColumns: StructType,
                                  private val aggregateColumns: Array[AggregateFunc]) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new Neo4jPartitionReader(
    neo4jOptions,
    filters,
    schema,
    jobId,
    partition.asInstanceOf[Neo4jPartition].partitionSkipLimit,
    scriptResult,
    requiredColumns,
    aggregateColumns
  )
}
