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
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.metric.CustomSumMetric
import org.apache.spark.sql.connector.write.BatchWrite
import org.apache.spark.sql.connector.write.SupportsOverwrite
import org.apache.spark.sql.connector.write.SupportsTruncate
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.streaming.Neo4jStreamingWriter
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.NodeSaveMode
import org.neo4j.spark.util.ValidateSaveMode
import org.neo4j.spark.util.ValidateWrite
import org.neo4j.spark.util.ValidationUtil
import org.neo4j.spark.util.Validations

class Neo4jWriterBuilder(queryId: String, schema: StructType, saveMode: SaveMode, neo4jOptions: Neo4jOptions)
    extends WriteBuilder
    with SupportsOverwrite
    with SupportsTruncate {

  override def build(): Write = new Write {
    override def description(): String = "Neo4j Writer"

    override def toBatch: BatchWrite = buildForBatch()

    override def toStreaming: StreamingWrite = buildForStreaming()

    override def supportedCustomMetrics(): Array[CustomMetric] = DataWriterMetrics.metricDeclarations()
  }

  def validOptions(actualSaveMode: SaveMode): Neo4jOptions = {
    Validations.validate(ValidateWrite(
      neo4jOptions,
      queryId,
      actualSaveMode,
      (o: Neo4jOptions) => {
        ValidationUtil.isFalse(
          o.relationshipMetadata.sourceSaveMode.equals(NodeSaveMode.ErrorIfExists)
            && o.relationshipMetadata.targetSaveMode.equals(NodeSaveMode.ErrorIfExists),
          "Save mode 'ErrorIfExists' is not supported on Spark 3.0, use 'Append' instead."
        )
      }
    ))
    neo4jOptions
  }

  override def buildForBatch(): BatchWrite = new Neo4jBatchWriter(queryId, schema, saveMode, validOptions(saveMode))

  @volatile
  private var streamWriter: Neo4jStreamingWriter = _

  def isNewInstance(queryId: String, schema: StructType, options: Neo4jOptions): Boolean =
    streamWriter == null ||
      streamWriter.queryId != queryId ||
      streamWriter.schema != schema ||
      streamWriter.neo4jOptions != options

  override def buildForStreaming(): StreamingWrite = {
    if (isNewInstance(queryId, schema, neo4jOptions)) {
      val streamingSaveMode = neo4jOptions.saveMode
      Validations.validate(ValidateSaveMode(streamingSaveMode))

      val saveMode = SaveMode.valueOf(streamingSaveMode)
      streamWriter = new Neo4jStreamingWriter(
        queryId,
        schema,
        saveMode,
        validOptions(saveMode)
      )
    }

    streamWriter
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    new Neo4jWriterBuilder(queryId, schema, SaveMode.Overwrite, neo4jOptions)
  }
}
