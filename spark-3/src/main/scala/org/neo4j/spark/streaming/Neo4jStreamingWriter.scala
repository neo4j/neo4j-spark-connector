package org.neo4j.spark.streaming

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.connector.write.{PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Neo4jUtil}

class Neo4jStreamingWriter(val queryId: String,
                           val schema: StructType,
                           saveMode: SaveMode,
                           val neo4jOptions: Neo4jOptions)
  extends StreamingWrite {

  private val self = this

  private val listener = new StreamingQueryListener {
    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {}

    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
      if (event.id.toString == queryId) {
        self.close()
        SparkSession.active.streams.removeListener(this)
      }
    }
  }
  SparkSession.active.streams.addListener(listener)

  private val driverCache = new DriverCache(neo4jOptions.connection, queryId)

  private lazy val scriptResult = {
    val schemaService = new SchemaService(neo4jOptions, driverCache)
    schemaService.createOptimizations()
    val scriptResult = schemaService.execute(neo4jOptions.script)
    schemaService.close()
    scriptResult
  }

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    new Neo4jStreamingDataWriterFactory(
      queryId,
      schema,
      saveMode,
      neo4jOptions,
      scriptResult
    )
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = { }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = { }

  def close(): Unit = Neo4jUtil.closeSafety(driverCache)
}