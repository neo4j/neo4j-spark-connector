package org.neo4j.spark.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.AccessMode
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Neo4jUtil, ValidateWrite, Validations}
import org.neo4j.spark.writer.Neo4jDataWriterFactory

class Neo4jDataSourceStreamWriter(val queryId: String,
                                  val schema: StructType,
                                  val options: DataSourceOptions,
                                  val streamingSaveMode: SaveMode)
  extends StreamWriter
    with Logging {

  private val self = this

  private val listener = new StreamingQueryListener {
    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {}

    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
      if (event.runId.toString == queryId) {
        self.close()
        SparkSession.active.streams.removeListener(this)
      }
    }
  }
  SparkSession.active.streams.addListener(listener)

  private val optionsMap = {
    val map = options.asMap()
    map.put(Neo4jOptions.ACCESS_MODE, AccessMode.WRITE.toString)
    map
  }
  private val neo4jOptions = new Neo4jOptions(optionsMap)
  Validations.validate(ValidateWrite(neo4jOptions, queryId, streamingSaveMode))

  private val driverCache = new DriverCache(neo4jOptions.connection, queryId)

  private lazy val scriptResult = {
    val schemaService = new SchemaService(neo4jOptions, driverCache)
    schemaService.createOptimizations()
    val scriptResult = schemaService.execute(neo4jOptions.script)
    schemaService.close()
    scriptResult
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = Unit

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = Unit

  override def createWriterFactory(): DataWriterFactory[InternalRow] = new Neo4jDataWriterFactory(queryId, schema, streamingSaveMode, neo4jOptions, scriptResult)

  def close(): Unit = Neo4jUtil.closeSafety(driverCache)
}
