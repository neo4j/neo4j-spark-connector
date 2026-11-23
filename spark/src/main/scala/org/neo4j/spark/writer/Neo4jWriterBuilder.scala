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
package org.neo4j.spark.writer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.caniuse.Neo4j
import org.neo4j.spark.streaming.Neo4jStreamingWriter
import org.neo4j.spark.util._

class Neo4jWriterBuilder(
  neo4j: Neo4j,
  queryId: String,
  schema: StructType,
  saveMode: SaveMode,
  neo4jOptions: Neo4jOptions,
  sparkSession: Option[SparkSession]
) extends WriteBuilder
    with SupportsOverwrite
    with SupportsTruncate {

  override def build(): Write = new Write {
    override def description(): String = "Neo4j Writer"

    override def toBatch: BatchWrite = buildForBatch()

    override def toStreaming: StreamingWrite = buildForStreaming()

    override def supportedCustomMetrics(): Array[CustomMetric] = DataWriterMetrics.metricDeclarations()
  }

  private def validOptions(actualSaveMode: SaveMode): Neo4jOptions = {
    Validations.validate(
      ValidateSaveMode(neo4jOptions, actualSaveMode),
      ValidateWrite(
        neo4j,
        neo4jOptions,
        queryId,
        actualSaveMode
      )
    )

    neo4jOptions
  }

  override def buildForBatch(): BatchWrite =
    new Neo4jBatchWriter(neo4j, queryId, schema, saveMode, validOptions(saveMode))

  @volatile
  private var streamWriter: Neo4jStreamingWriter = _

  private def isNewInstance(queryId: String, schema: StructType, options: Neo4jOptions): Boolean =
    streamWriter == null ||
      streamWriter.queryId != queryId ||
      streamWriter.schema != schema ||
      streamWriter.neo4jOptions != options

  override def buildForStreaming(): StreamingWrite = {
    if (isNewInstance(queryId, schema, neo4jOptions)) {
      Validations.validate(ValidateSaveMode(neo4jOptions, null))
      val saveMode = SaveMode.valueOf(neo4jOptions.saveMode)

      streamWriter = new Neo4jStreamingWriter(
        neo4j,
        queryId,
        schema,
        saveMode,
        validOptions(saveMode),
        sparkSession
      )
    }

    streamWriter
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    new Neo4jWriterBuilder(neo4j, queryId, schema, SaveMode.Overwrite, neo4jOptions, sparkSession)
  }
}
