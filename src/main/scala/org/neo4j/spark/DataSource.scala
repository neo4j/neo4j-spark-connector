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

class DataSource extends TableProvider
    with DataSourceRegister {

  Validations.validate(ValidateSparkMinVersion("4.0.0"))

  private val jobId: String = UUID.randomUUID().toString

  private var schema: StructType = _

  private var neo4jOptions: Neo4jOptions = _

  private var neo4j: Neo4j = _

  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    if (schema == null) {
      val neo4jOpts = getNeo4jOptions(caseInsensitiveStringMap)
      Validations.validate(ValidateConnection(neo4jOpts, jobId))
      val neo4j = getNeo4jInfo(neo4jOpts.connection)
      schema =
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

    schema
  }

  private def getNeo4jInfo(options: Neo4jDriverOptions): Neo4j = {
    if (neo4j == null) {
      val driver = new DriverCache(options).getOrCreate()
      neo4j = Neo4jDetector.INSTANCE.detect(driver)
    }
    neo4j
  }

  private def getNeo4jOptions(caseInsensitiveStringMap: CaseInsensitiveStringMap) = {
    if (neo4jOptions == null) {
      val session = SparkSession.getActiveSession
      val externalOptions = caseInsensitiveStringMap.asCaseSensitiveMap().asScala.toMap
      neo4jOptions = Neo4jOptions.fromSession(session, externalOptions)
    }

    ValidateNeo4jOptionsConsistency(getNeo4jInfo(neo4jOptions.connection), neo4jOptions).validate()
    neo4jOptions
  }

  override def getTable(
    structType: StructType,
    transforms: Array[Transform],
    map: java.util.Map[String, String]
  ): Table = {
    val caseInsensitiveStringMapNeo4jOptions = new CaseInsensitiveStringMap(map)
    val schema = if (structType != null) {
      structType
    } else {
      inferSchema(caseInsensitiveStringMapNeo4jOptions)
    }
    val neo4jOpts = getNeo4jOptions(caseInsensitiveStringMapNeo4jOptions)
    val neo4jInfo = getNeo4jInfo(neo4jOpts.connection)
    new Neo4jTable(neo4jInfo, schema, neo4jOpts, jobId)
  }

  override def shortName(): String = "neo4j"
}
