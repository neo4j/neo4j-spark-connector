package org.neo4j.spark.writer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.{DriverCache, Neo4jOptions}

class Neo4jDataSourceWriter(jobId: String,
                            structType: StructType,
                            saveMode: SaveMode,
                            options: DataSourceOptions) extends DataSourceWriter {

  private val neo4jOptions: Neo4jOptions = new Neo4jOptions(options.asMap())
    .validate(neo4jOptions => {
      saveMode match {
        case SaveMode.Overwrite => {
          if (neo4jOptions.nodeMetadata.nodeKeys.isEmpty) {
            throw new IllegalArgumentException(s"${Neo4jOptions.NODE_KEYS} is required when Save Mode is Overwrite")
          }
        }
        case _ => Unit
      }
    })

  private val driverCache = new DriverCache(neo4jOptions.connection, jobId)

  override def createWriterFactory(): DataWriterFactory[InternalRow] = new Neo4jDataWriterFactory(jobId, structType, saveMode, neo4jOptions)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    driverCache.close()
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    driverCache.close()
  }
}