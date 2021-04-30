package org.neo4j.spark.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.service._
import org.neo4j.spark.util.Neo4jOptions

class Neo4jInputPartition(private val options: Neo4jOptions,
                          private val filters: Array[Filter],
                          private val schema: StructType,
                          private val jobId: String,
                          private val partitionSkipLimit: PartitionSkipLimit,
                          private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
                          private val requiredColumns: StructType)
    extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = new Neo4jInputPartitionReader(options, filters, schema,
    jobId, partitionSkipLimit, scriptResult, requiredColumns)
}