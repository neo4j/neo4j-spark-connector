/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.spark.streaming

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.reader.BasePartitionReader
import org.neo4j.spark.service.Neo4jQueryStrategy
import org.neo4j.spark.service.PartitionPagination
import org.neo4j.spark.util.Neo4jImplicits._
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.Neo4jUtil
import org.neo4j.spark.util.StreamingFrom

import java.util
import java.util.Collections

class BaseStreamingPartitionReader(
  private val options: Neo4jOptions,
  private val filters: Array[Filter],
  private val schema: StructType,
  private val jobId: String,
  private val partitionSkipLimit: PartitionPagination,
  private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
  private val offsetAccumulator: OffsetStorage[java.lang.Long, java.lang.Long],
  private val requiredColumns: StructType,
  private val aggregateColumns: Array[AggregateFunc]
) extends BasePartitionReader(
      options,
      filters,
      schema,
      jobId,
      partitionSkipLimit,
      scriptResult,
      requiredColumns,
      aggregateColumns
    ) {

  private val streamingPropertyName = Neo4jUtil.getStreamingPropertyName(options)

  private val streamingField = filters.find(f =>
    f.getAttribute
      .map(name => name == streamingPropertyName).getOrElse(false)
  )

  @volatile
  private var lastTimestamp: java.lang.Long = _

  logInfo(s"Creating Streaming Partition reader $name")

  private lazy val values = {
    val map = new util.HashMap[String, Any](super.getQueryParameters)
    val value: Long = streamingField
      .flatMap(f => f.getValue)
      .getOrElse(StreamingFrom.ALL.value())
      .asInstanceOf[Long]
    map.put(Neo4jQueryStrategy.VARIABLE_STREAM, Collections.singletonMap("offset", value))
    map
  }

  override def next: Boolean = {
    val hasNext = super.next
    if (hasNext) {
      updateLastTimestamp(super.get)
    }
    hasNext
  }

  override def close(): Unit = {
    if (!hasError()) {
      offsetAccumulator.add(getLastTimestamp())
    }
    logInfo(s"Closing Partition reader $name ${if (hasError()) "with error " else ""}")
    super.close()
  }

  private def updateLastTimestamp(row: InternalRow) = synchronized {
    try {
      val fieldIndex = schema.fieldIndex(streamingPropertyName)
      val currentTimestamp: java.lang.Long = schema(fieldIndex).dataType match {
        case DataTypes.LongType => row.getLong(fieldIndex)
        case _                  => row.getUTF8String(fieldIndex).toString.toLong
      }
      if (lastTimestamp == null || lastTimestamp < currentTimestamp) {
        lastTimestamp = currentTimestamp
      }
    } catch {
      case t: Throwable => logInfo(
          s"""
             |Cannot extract the last timestamp for schema $schema and property $streamingPropertyName,
             |because of the following exception:
             |""".stripMargin,
          t
        )
    }
  }

  def getLastTimestamp() = synchronized { lastTimestamp }

  override protected def getQueryParameters: util.Map[String, Any] = values

}
