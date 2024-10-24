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
package org.neo4j.spark.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.service.PartitionPagination
import org.neo4j.spark.util.Neo4jOptions

class Neo4jPartitionReaderFactory(
  private val neo4jOptions: Neo4jOptions,
  private val filters: Array[Filter],
  private val schema: StructType,
  private val jobId: String,
  private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
  private val requiredColumns: StructType,
  private val aggregateColumns: Array[AggregateFunc]
) extends PartitionReaderFactory {

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
