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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.PhysicalWriteInfo
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.DriverCache
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.Neo4jUtil

import java.util.Optional

class Neo4jStreamingWriter(
  val queryId: String,
  val schema: StructType,
  saveMode: SaveMode,
  val neo4jOptions: Neo4jOptions
) extends StreamingWrite {

  private val self = this

  private val listener = new StreamingQueryListener {
    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = ()

    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = ()

    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
      if (event.id.toString == queryId) {
        self.close()
        SparkSession.getDefaultSession.get.streams.removeListener(this)
      }
    }
  }
  SparkSession.getDefaultSession.get.streams.addListener(listener)

  private val driverCache = new DriverCache(neo4jOptions.connection, queryId)

  private lazy val scriptResult = {
    val schemaService = new SchemaService(neo4jOptions, driverCache)
    schemaService.createOptimizations(schema)
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

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  def close(): Unit = Neo4jUtil.closeSafely(driverCache)
}
