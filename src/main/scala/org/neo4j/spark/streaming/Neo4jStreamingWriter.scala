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
import org.apache.spark.sql.connector.write.PhysicalWriteInfo
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.types.StructType
import org.neo4j.caniuse.Neo4j
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.DriverCache
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.Neo4jUtil

class Neo4jStreamingWriter(
  val neo4j: Neo4j,
  val queryId: String,
  val schema: StructType,
  saveMode: SaveMode,
  val neo4jOptions: Neo4jOptions
) extends StreamingWrite {

  private val driverCache = new DriverCache(neo4jOptions.connection)

  private lazy val scriptResult = {
    val schemaService = new SchemaService(neo4j, neo4jOptions, driverCache)
    schemaService.createOptimizations(schema)
    val scriptResult = schemaService.execute(neo4jOptions.script.toIndexedSeq)
    schemaService.close()
    scriptResult
  }

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    try {
      new Neo4jStreamingDataWriterFactory(
        neo4j,
        queryId,
        schema,
        saveMode,
        neo4jOptions,
        scriptResult
      )
    } finally {
      close()
    }
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  def close(): Unit = Neo4jUtil.closeSafely(driverCache)
}
