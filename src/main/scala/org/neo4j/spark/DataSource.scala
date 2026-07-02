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
package org.neo4j.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDetector
import org.neo4j.spark.util._

import java.util.UUID

import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Using

class DataSource extends TableProvider
    with DataSourceRegister {

  Validations.validate(ValidateSparkMinVersion("4.0.0"))

  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    inferSchema(caseInsensitiveStringMap, UUID.randomUUID().toString)
  }

  private def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap, jobId: String): StructType = {
    val neo4jOpts = getNeo4jOptions(caseInsensitiveStringMap)
    Validations.validate(ValidateConnection(neo4jOpts, jobId))
    val neo4j = getNeo4jInfo(neo4jOpts.connection)
    Neo4jUtil.callSchemaService(
      neo4j,
      neo4jOpts,
      jobId,
      Array.empty[Filter],
      { schemaService =>
        schemaService.struct()
      }
    )
  }

  private def getNeo4jOptions(caseInsensitiveStringMap: CaseInsensitiveStringMap) = {
    val session = SparkSession.getActiveSession
    val externalOptions = caseInsensitiveStringMap.asCaseSensitiveMap().asScala.toMap
    val neo4jOptions = Neo4jOptions.fromSession(session, externalOptions)
    ValidateNeo4jOptionsConsistency(getNeo4jInfo(neo4jOptions.connection), neo4jOptions).validate()
    neo4jOptions
  }

  private def getNeo4jInfo(options: Neo4jDriverOptions): Neo4j = {
    val driverCache = new DriverCache(options)
    Using.resource(driverCache)(cache => Neo4jDetector.INSTANCE.detect(cache.getOrCreate()))
  }

  override def getTable(
    structType: StructType,
    transforms: Array[Transform],
    map: java.util.Map[String, String]
  ): Table = {
    val jobId = UUID.randomUUID().toString
    val caseInsensitiveStringMapNeo4jOptions = new CaseInsensitiveStringMap(map)
    val schema = if (structType != null) {
      structType
    } else {
      inferSchema(caseInsensitiveStringMapNeo4jOptions, jobId)
    }
    val neo4jOpts = getNeo4jOptions(caseInsensitiveStringMapNeo4jOptions)
    val neo4jInfo = getNeo4jInfo(neo4jOpts.connection)
    new Neo4jTable(neo4jInfo, schema, neo4jOpts, jobId)
  }

  override def shortName(): String = "neo4j"
}
