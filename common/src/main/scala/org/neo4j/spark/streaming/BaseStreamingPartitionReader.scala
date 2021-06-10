package org.neo4j.spark.streaming

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.neo4j.spark.reader.BasePartitionReader
import org.neo4j.spark.service.{Neo4jQueryStrategy, PartitionSkipLimit}
import org.neo4j.spark.util.Neo4jImplicits._
import org.neo4j.spark.util.{Neo4jOptions, Neo4jUtil, StreamingFrom}

import java.util
import java.util.Collections

class BaseStreamingPartitionReader(private val options: Neo4jOptions,
                                   private val filters: Array[Filter],
                                   private val schema: StructType,
                                   private val jobId: String,
                                   private val partitionSkipLimit: PartitionSkipLimit,
                                   private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
                                   private val requiredColumns: StructType) extends BasePartitionReader(options,
    filters,
    schema,
    jobId,
    partitionSkipLimit,
    scriptResult,
    requiredColumns) {

  private val prop = Neo4jUtil.getStreamingPropertyName(options)

  private val field = filters.find(f => f.getAttribute
      .map(name => name == prop).getOrElse(false))


  private lazy val values = {
    val map = new util.HashMap[String, AnyRef](super.getQueryParameters)
    val value: Long = field
      .flatMap(f => f.getValue)
      .getOrElse(StreamingFrom.ALL.value())
      .asInstanceOf[Long]
    map.put(Neo4jQueryStrategy.VARIABLE_STREAM, Collections.singletonMap("offset", value))
    map
  }

  override def get: InternalRow = {
    val row = super.get
    updateOffset(row)
    row
  }


  private def updateOffset(row: InternalRow) = {
    val fieldIndex = schema.fieldIndex(prop)
    val timestamp = schema(fieldIndex).dataType match {
        case DataTypes.LongType => row.getLong(fieldIndex)
        case _ => row.getUTF8String(fieldIndex).toString.toLong
      }
    OffsetStorage.setLastOffset(jobId, timestamp)
  }

  override protected def getQueryParameters: util.Map[String, AnyRef] = values

}
