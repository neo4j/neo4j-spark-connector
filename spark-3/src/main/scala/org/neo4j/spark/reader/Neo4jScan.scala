package org.neo4j.spark.reader

import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.config.TopN
import org.neo4j.spark.service.PartitionPagination
import org.neo4j.spark.streaming.Neo4jMicroBatchReader
import org.neo4j.spark.util.{Neo4jOptions, Neo4jUtil, StorageType, ValidateReadNotStreaming, ValidateReadStreaming, Validations}

import java.util.Optional

case class Neo4jPartition(partitionSkipLimit: PartitionPagination) extends InputPartition

class Neo4jScan(neo4jOptions: Neo4jOptions,
                jobId: String,
                schema: StructType,
                filters: Array[Filter],
                requiredColumns: StructType,
                aggregateColumns: Array[AggregateFunc],
                topN: Option[TopN]) extends Scan with Batch {

  override def toBatch: Batch = this

  var scriptResult: java.util.List[java.util.Map[String, AnyRef]] = _

  private def createPartitions() = {
    Validations.validate(ValidateReadNotStreaming(neo4jOptions, jobId))
    // we get the skip/limit for each partition and execute the "script"
    val (partitionSkipLimitList, scriptResult) = Neo4jUtil.callSchemaService(neo4jOptions, jobId, filters, { schemaService =>
      (schemaService.skipLimitFromPartition(topN), schemaService.execute(neo4jOptions.script))
    })
    // we generate a partition for each element
    this.scriptResult = scriptResult
    partitionSkipLimitList
      .map(partitionSkipLimit => Neo4jPartition(partitionSkipLimit))
  }

  override def planInputPartitions(): Array[InputPartition] = {
    val neo4jPartitions: Seq[Neo4jPartition] = createPartitions()
    neo4jPartitions.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new Neo4jPartitionReaderFactory(
      neo4jOptions, filters, schema, jobId, scriptResult, requiredColumns, aggregateColumns
    )
  }

  override def readSchema(): StructType = schema

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    // we hardcode the SparkAccumulator as Spark 3.x
    // support Accumulators from DataSourceV2
    val optsMap = neo4jOptions.asMap()
    optsMap.put(Neo4jOptions.STREAMING_METADATA_STORAGE, StorageType.SPARK.toString)
    val newOpts = new Neo4jOptions(optsMap)
    Validations.validate(ValidateReadStreaming(newOpts, jobId))
    new Neo4jMicroBatchReader(schema, newOpts, jobId, aggregateColumns)
  }
}
