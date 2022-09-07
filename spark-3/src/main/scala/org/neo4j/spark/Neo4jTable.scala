package org.neo4j.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.neo4j.driver.AccessMode
import org.neo4j.spark.reader.Neo4jScanBuilder
import org.neo4j.spark.util.{Neo4jOptions, ValidateRead, Validations}
import org.neo4j.spark.writer.Neo4jWriterBuilder

import scala.collection.JavaConverters._

class Neo4jTable(schema: StructType, options: java.util.Map[String, String], jobId: String) extends Table
  with SupportsRead
  with SupportsWrite
  with Logging {

  private val neo4jOptions = new Neo4jOptions(options)

  override def name(): String = neo4jOptions.getTableName

  override def schema(): StructType = schema

  override def capabilities(): java.util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.ACCEPT_ANY_SCHEMA,
    TableCapability.OVERWRITE_BY_FILTER,
    TableCapability.OVERWRITE_DYNAMIC,
    TableCapability.STREAMING_WRITE,
    TableCapability.MICRO_BATCH_READ
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): Neo4jScanBuilder = {
    Validations.validate(ValidateRead(neo4jOptions, jobId))
    new Neo4jScanBuilder(neo4jOptions, jobId, schema())
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val mapOptions = new java.util.HashMap[String, String](options)
    mapOptions.put(Neo4jOptions.ACCESS_MODE, AccessMode.WRITE.toString)
    val writeNeo4jOptions = new Neo4jOptions(mapOptions)
    new Neo4jWriterBuilder(info.queryId(), info.schema(), SaveMode.Append, writeNeo4jOptions)
  }
}
