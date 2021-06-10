package org.neo4j.spark.writer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsOverwrite, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.AccessMode
import org.neo4j.spark.streaming.Neo4jStreamingWriter
import org.neo4j.spark.util.{Neo4jOptions, NodeSaveMode, ValidationUtil, Validations}

class Neo4jWriterBuilder(queryId: String,
                         schema: StructType,
                         saveMode: SaveMode,
                         neo4jOptions: Neo4jOptions) extends WriteBuilder
  with SupportsOverwrite
  with SupportsTruncate {

  def validOptions(actualSaveMode: SaveMode): Neo4jOptions = {
    neo4jOptions.validate(neo4jOptions =>
      Validations.writer(neo4jOptions, queryId, actualSaveMode, (o: Neo4jOptions) => {
        ValidationUtil.isFalse(
          o.relationshipMetadata.sourceSaveMode.equals(NodeSaveMode.ErrorIfExists)
            && o.relationshipMetadata.targetSaveMode.equals(NodeSaveMode.ErrorIfExists),
          "Save mode 'ErrorIfExists' is not supported on Spark 3.0, use 'Append' instead.")
      }))
  }

  override def buildForBatch(): BatchWrite = new Neo4jBatchWriter(queryId,
    schema,
    saveMode,
    validOptions(saveMode)
  )

  @volatile
  private var streamWriter: Neo4jStreamingWriter = _

  def isNewInstance(queryId: String,
                    schema: StructType,
                    options: Neo4jOptions): Boolean =
    streamWriter == null ||
      streamWriter.queryId != queryId ||
      streamWriter.schema != schema ||
      streamWriter.neo4jOptions != options

  override def buildForStreaming(): StreamingWrite = {
    if (isNewInstance(queryId, schema, neo4jOptions)) {
      val streamingSaveMode = neo4jOptions.saveMode
      Validations.supportedSaveMode(streamingSaveMode)

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
