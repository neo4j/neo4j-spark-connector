package org.neo4j.spark.reader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.{Record, Session, Transaction, Values}
import org.neo4j.spark.service.{MappingService, Neo4jQueryReadStrategy, Neo4jQueryService, Neo4jQueryStrategy, Neo4jReadMappingStrategy, PartitionSkipLimit}
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Neo4jUtil}
import org.neo4j.spark.util.Neo4jImplicits.StructTypeImplicit

import scala.collection.JavaConverters._

abstract class BasePartitionReader(private val options: Neo4jOptions,
                          private val filters: Array[Filter],
                          private val schema: StructType,
                          private val jobId: String,
                          private val partitionSkipLimit: PartitionSkipLimit,
                          private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
                          private val requiredColumns: StructType) extends Logging {
  private var result: Iterator[Record] = _
  private var session: Session = _
  private var transaction: Transaction = _
  private val driverCache: DriverCache = new DriverCache(options.connection,
    if (partitionSkipLimit.partitionNumber > 0) s"$jobId-${partitionSkipLimit.partitionNumber}" else jobId)

  private val query: String = new Neo4jQueryService(options, new Neo4jQueryReadStrategy(filters, partitionSkipLimit, requiredColumns.fieldNames))
    .createQuery()

  private val mappingService = new MappingService(new Neo4jReadMappingStrategy(options, requiredColumns), options)

  def next: Boolean = {
    if (result == null) {
      session = driverCache.getOrCreate().session(options.session.toNeo4jSession)
      transaction = session.beginTransaction()
      log.info(s"Running the following query on Neo4j: $query")
      result = transaction.run(query, Values
        .value(Map[String, AnyRef](Neo4jQueryStrategy.VARIABLE_SCRIPT_RESULT -> scriptResult).asJava))
        .asScala
    }

    result.hasNext
  }

  def get: InternalRow = mappingService.convert(result.next(), schema)

  def close(): Unit = {
    Neo4jUtil.closeSafety(transaction, log)
    Neo4jUtil.closeSafety(session, log)
    driverCache.close()
  }

}
