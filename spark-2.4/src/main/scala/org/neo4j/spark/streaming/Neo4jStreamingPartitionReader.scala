package org.neo4j.spark.streaming

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.AccumulatorUtils
import org.neo4j.spark.service.PartitionSkipLimit
import org.neo4j.spark.streaming.BaseStreamingPartitionReader
import org.neo4j.spark.util.Neo4jOptions

class Neo4jStreamingPartitionReader(private val options: Neo4jOptions,
                                    private val filters: Array[Filter],
                                    private val schema: StructType,
                                    private val jobId: String,
                                    private val partitionSkipLimit: PartitionSkipLimit,
                                    private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
                                    private val offsetAccumulator: OffsetAccumulator,
                                    private val requiredColumns: StructType)
  extends BaseStreamingPartitionReader(options, filters, schema, jobId, partitionSkipLimit, scriptResult, offsetAccumulator, requiredColumns)
    with InputPartitionReader[InternalRow] {

  // workaround for Spark 2.4 as Accumulators are not managed properly into DatasourceV2
  override protected def getAccumulator(): OffsetAccumulator = AccumulatorUtils.get(offsetAccumulator.id)
}