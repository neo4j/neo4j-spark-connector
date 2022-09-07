package org.neo4j.spark.reader

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.util.Neo4jOptions

class Neo4jScanBuilder(neo4jOptions: Neo4jOptions, jobId: String, schema: StructType) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private var filters: Array[Filter] = Array[Filter]()

  private var requiredColumns: StructType = new StructType()

  override def build(): Scan = {
    new Neo4jScan(neo4jOptions, jobId, schema, filters, requiredColumns)
  }

  override def pushFilters(filtersArray: Array[Filter]): Array[Filter] = {
    if (neo4jOptions.pushdownFiltersEnabled) {
      filters = filtersArray
    }

    filtersArray
  }

  override def pushedFilters(): Array[Filter] = filters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    requiredColumns = if (
      !neo4jOptions.pushdownColumnsEnabled || neo4jOptions.relationshipMetadata.nodeMap
      || requiredSchema == schema
    ) {
      new StructType()
    } else {
      requiredSchema
    }
  }
}
