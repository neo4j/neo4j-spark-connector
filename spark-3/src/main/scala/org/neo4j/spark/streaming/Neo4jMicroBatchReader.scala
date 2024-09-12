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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.connector.read.streaming.Offset
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.GreaterThan
import org.apache.spark.sql.sources.LessThanOrEqual
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util._

class Neo4jMicroBatchReader(
  private val schema: StructType,
  private val neo4jOptions: Neo4jOptions,
  private val jobId: String,
  private val aggregateColumns: Array[AggregateFunc]
) extends MicroBatchStream
    with Logging {

  private val driverCache = new DriverCache(neo4jOptions.connection, jobId)

  private lazy val scriptResult = {
    val schemaService = new SchemaService(neo4jOptions, driverCache)
    schemaService.createOptimizations(schema)
    val scriptResult = schemaService.execute(neo4jOptions.script)
    schemaService.close()
    scriptResult
  }

  private var filters: Array[Filter] = Array.empty[Filter]

  override def deserializeOffset(json: String): Offset = Neo4jOffset(json.toLong)

  override def commit(end: Offset): Unit = {}

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    logDebug(s"start and end offset: $start - $end")

    val prop = Neo4jUtil.getStreamingPropertyName(neo4jOptions)
    this.filters =
      Array(
        GreaterThan(prop, start.asInstanceOf[Neo4jOffset].offset),
        LessThanOrEqual(prop, end.asInstanceOf[Neo4jOffset].offset)
      )

    val partitions = Neo4jUtil.callSchemaService(
      neo4jOptions,
      jobId,
      filters,
      { schemaService => schemaService.skipLimitFromPartition(None) }
    )

    partitions
      .map(p => Neo4jStreamingPartition(p, filters))
      .toArray
  }

  override def stop(): Unit = {
    driverCache.close()
  }

  override def latestOffset(): Offset = {
    val offset = Neo4jOffset(Neo4jUtil.callSchemaService[Long](
      neo4jOptions,
      jobId,
      filters,
      {
        schemaService =>
          try {
            schemaService.lastOffset()
          } catch {
            case _: Throwable => -1L
          }
      }
    ))

    offset
  }

  override def initialOffset(): Offset = Neo4jOffset(neo4jOptions.streamingOptions.from.value())

  override def createReaderFactory(): PartitionReaderFactory = {
    new Neo4jStreamingPartitionReaderFactory(
      neo4jOptions,
      schema,
      jobId,
      scriptResult,
      aggregateColumns
    )
  }
}
