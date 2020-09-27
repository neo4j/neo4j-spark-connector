package org.neo4j.spark.reader

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.{DriverCache, Neo4jOptions}
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.Validations

import scala.collection.JavaConverters._

class Neo4jDataSourceReader(private val options: DataSourceOptions, private val jobId: String) extends DataSourceReader
  with Logging
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private var filters: Array[Filter] = Array[Filter]()

  private var requiredColumns: StructType = _

  private val neo4jOptions: Neo4jOptions = new Neo4jOptions(options.asMap())
    .validate(options => Validations.read(options, jobId))


  override def readSchema(): StructType = callSchemaService { schemaService =>
    schemaService
      .struct()
  }

  private def callSchemaService[T](function: SchemaService => T): T = {
    val driverCache = new DriverCache(neo4jOptions.connection, jobId)
    val schemaService = new SchemaService(neo4jOptions, driverCache)
    var hasError = false
    try {
      function(schemaService)
    } catch {
      case e: Throwable => {
        hasError = true
        throw e
      }
    } finally {
      schemaService.close()
      if (hasError) {
        driverCache.close()
      }
    }
  }

  override def planInputPartitions: util.ArrayList[InputPartition[InternalRow]] = {
    // we retrieve the schema in order to parse the data correctly
    val schema = readSchema()
    val neo4jPartitions: Seq[Neo4jInputPartitionReader] = createPartitions(schema)
    new util.ArrayList[InputPartition[InternalRow]](neo4jPartitions.asJava)
  }

  private def createPartitions(schema: StructType) = {
    // we get the skip/limit for each partition
    val partitionSkipLimitList = callSchemaService { schemaService =>
      schemaService
        .skipLimitFromPartition()
    }
    // we generate a partition for each element
    partitionSkipLimitList
      .map(partitionSkipLimit => new Neo4jInputPartitionReader(
        neo4jOptions,
        filters,
        schema,
        jobId,
        partitionSkipLimit,
        requiredColumns)
      )
  }

  override def pushFilters(filtersArray: Array[Filter]): Array[Filter] = {
    if (neo4jOptions.pushdownFiltersEnabled) {
      filters = filtersArray
    }

    filtersArray
  }

  override def pushedFilters(): Array[Filter] = filters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    if (requiredSchema.nonEmpty) {
      log.info(s"Using requiredSchema: ${requiredSchema.toString()}")
    }
    else {
      log.info(s"No pruned columns")
    }

    requiredColumns = requiredSchema
  }
}