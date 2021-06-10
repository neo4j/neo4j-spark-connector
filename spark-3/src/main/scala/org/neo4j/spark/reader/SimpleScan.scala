package org.neo4j.spark.reader

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.service.{PartitionSkipLimit, SchemaService}
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Neo4jUtil}

import scala.collection.JavaConverters.seqAsJavaListConverter

case class Neo4jPartition(partitionSkipLimit: PartitionSkipLimit) extends InputPartition

class SimpleScan(
                  neo4jOptions: Neo4jOptions,
                  jobId: String,
                  schema: StructType,
                  filters: Array[Filter],
                  requiredColumns: StructType
                ) extends Scan with Batch {

  override def toBatch: Batch = this

  var scriptResult: java.util.List[java.util.Map[String, AnyRef]] = _

  private def createPartitions() = {
    // we get the skip/limit for each partition and execute the "script"
    val (partitionSkipLimitList, scriptResult) = Neo4jUtil.callSchemaService(neo4jOptions, jobId, filters, { schemaService =>
      (schemaService.skipLimitFromPartition(), schemaService.execute(neo4jOptions.script))
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
    new SimplePartitionReaderFactory(
      neo4jOptions, filters, schema, jobId, scriptResult, requiredColumns
    )
  }

  override def readSchema(): StructType = schema
}
