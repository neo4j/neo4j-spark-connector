package org.neo4j.spark

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.neo4j.spark.util.{Neo4jOptions, Neo4jUtil, ValidateConnection, ValidateSparkVersion, Validations}

import java.util.UUID

class DataSource extends TableProvider
  with DataSourceRegister {

  Validations.validate(ValidateSparkVersion("3.2.*"))

  private val jobId: String = UUID.randomUUID().toString

  private var schema: StructType = null

  private var neo4jOptions: Neo4jOptions = null

  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    if (schema == null) {
      val neo4jOpts = getNeo4jOptions(caseInsensitiveStringMap)
      Validations.validate(ValidateConnection(neo4jOpts, jobId))
      schema = Neo4jUtil.callSchemaService(neo4jOpts, jobId, Array.empty[Filter], { schemaService => schemaService.struct() })
    }

    schema
  }

  private def getNeo4jOptions(caseInsensitiveStringMap: CaseInsensitiveStringMap) = {
    if(neo4jOptions == null) {
      neo4jOptions = new Neo4jOptions(caseInsensitiveStringMap.asCaseSensitiveMap())
    }

    neo4jOptions
  }

  override def getTable(structType: StructType, transforms: Array[Transform], map: java.util.Map[String, String]): Table = {
    val caseInsensitiveStringMapNeo4jOptions = new CaseInsensitiveStringMap(map)
    val schema = if (structType != null) {
      structType
    } else {
      inferSchema(caseInsensitiveStringMapNeo4jOptions)
    }
    new Neo4jTable(schema, map, jobId)
  }

  override def shortName(): String = "neo4j"
}
