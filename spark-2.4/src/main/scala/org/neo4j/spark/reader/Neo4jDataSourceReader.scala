package org.neo4j.spark.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.util.{Neo4jOptions, Neo4jUtil, ValidateRead, ValidateReadNotStreaming, Validations}

import java.util
import scala.collection.JavaConverters._

class Neo4jDataSourceReader(private val options: DataSourceOptions, private val jobId: String, private val userDefinedSchema: StructType = null) extends DataSourceReader
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private var filters: Array[Filter] = Array[Filter]()

  private var requiredColumns: StructType = new StructType()

  private val neo4jOptions: Neo4jOptions = new Neo4jOptions(options.asMap())
  Validations.validate(ValidateRead(neo4jOptions, jobId), ValidateReadNotStreaming(neo4jOptions, jobId))

  private val structType = if (userDefinedSchema != null) {
    userDefinedSchema
  } else {
    Neo4jUtil.callSchemaService(neo4jOptions, jobId, filters, { schemaService => schemaService
      .struct() })
  }

  override def readSchema(): StructType = structType

  override def planInputPartitions: util.ArrayList[InputPartition[InternalRow]] = {
    // we retrieve the schema in order to parse the data correctly
    val schema = readSchema()
    val neo4jPartitions: Seq[Neo4jInputPartition] = createPartitions(schema)
    new util.ArrayList[InputPartition[InternalRow]](neo4jPartitions.asJava)
  }

  private def createPartitions(schema: StructType) = {
    // we get the skip/limit for each partition and execute the "script"
    val (partitionSkipLimitList, scriptResult) = Neo4jUtil.callSchemaService(neo4jOptions, jobId, filters, { schemaService =>
      (schemaService.skipLimitFromPartition(), schemaService.execute(neo4jOptions.script)) })
    // we generate a partition for each element
    partitionSkipLimitList
      .map(partitionSkipLimit => new Neo4jInputPartition(neo4jOptions, filters, schema, jobId,
        partitionSkipLimit, scriptResult, requiredColumns))
  }

  override def pushFilters(filtersArray: Array[Filter]): Array[Filter] = {
    if (neo4jOptions.pushdownFiltersEnabled) {
      filters = filtersArray
    }

    filtersArray
  }

  override def pushedFilters(): Array[Filter] = filters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    requiredColumns = if (!neo4jOptions.pushdownColumnsEnabled || neo4jOptions.relationshipMetadata.nodeMap) {
      new StructType()
    } else {
      requiredSchema
    }
  }
}