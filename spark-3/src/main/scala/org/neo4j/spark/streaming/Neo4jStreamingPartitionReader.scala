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
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.util.LongAccumulator
import org.neo4j.spark.service.PartitionPagination
import org.neo4j.spark.util.Neo4jOptions

class Neo4jStreamingPartitionReader(
  private val options: Neo4jOptions,
  private val filters: Array[Filter],
  private val schema: StructType,
  private val jobId: String,
  private val partitionSkipLimit: PartitionPagination,
  private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
  private val requiredColumns: StructType,
  private val aggregateColumns: Array[AggregateFunc]
) extends BaseStreamingPartitionReader(
      options,
      filters,
      schema,
      jobId,
      partitionSkipLimit,
      scriptResult,
      requiredColumns,
      aggregateColumns
    )
    with PartitionReader[InternalRow] {}
