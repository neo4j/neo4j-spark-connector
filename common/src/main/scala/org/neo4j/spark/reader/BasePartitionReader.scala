package org.neo4j.spark.reader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.Record
import org.neo4j.driver.Session
import org.neo4j.driver.Transaction
import org.neo4j.driver.Values
import org.neo4j.spark.service.MappingService
import org.neo4j.spark.service.Neo4jQueryReadStrategy
import org.neo4j.spark.service.Neo4jQueryService
import org.neo4j.spark.service.Neo4jQueryStrategy
import org.neo4j.spark.service.Neo4jReadMappingStrategy
import org.neo4j.spark.service.PartitionPagination
import org.neo4j.spark.util.DriverCache
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.Neo4jUtil
import org.neo4j.spark.util.QueryType

import java.io.IOException
import java.util

import scala.collection.JavaConverters._

abstract class BasePartitionReader(
  private val options: Neo4jOptions,
  private val filters: Array[Filter],
  private val schema: StructType,
  private val jobId: String,
  private val partitionSkipLimit: PartitionPagination,
  private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
  private val requiredColumns: StructType,
  private val aggregateColumns: Array[AggregateFunc]
) extends Logging {
  private var result: Iterator[Record] = _
  private var session: Session = _
  private var transaction: Transaction = _

  protected val name: String =
    if (partitionSkipLimit.partitionNumber > 0) s"$jobId-${partitionSkipLimit.partitionNumber}" else jobId
  protected val driverCache: DriverCache = new DriverCache(options.connection, name)

  private val query: String = new Neo4jQueryService(
    options,
    new Neo4jQueryReadStrategy(filters, partitionSkipLimit, requiredColumns.fieldNames, aggregateColumns, jobId)
  )
    .createQuery()

  private var nextRow: InternalRow = _

  private lazy val values = {
    val params = new java.util.HashMap[String, Any]()
    params.put(Neo4jQueryStrategy.VARIABLE_SCRIPT_RESULT, scriptResult)
    Neo4jUtil.paramsFromFilters(filters)
      .foreach(p => params.put(p._1, p._2))

    if (options.query.queryType == QueryType.GDS) {
      params.putAll(options.gdsMetadata.parameters)
    }

    params
  }

  private val mappingService = new MappingService(new Neo4jReadMappingStrategy(options, requiredColumns), options)

  @volatile
  private var error: Boolean = false

  @throws(classOf[IOException])
  def next: Boolean =
    try {
      if (result == null) {
        session = driverCache.getOrCreate().session(options.session.toNeo4jSession())
        transaction = session.beginTransaction()

        val queryParams = getQueryParameters

        logInfo(s"Running the following query on Neo4j: $query")
        logDebug(s"with parameters $queryParams")

        result = transaction.run(query, Values.value(queryParams))
          .asScala
      }

      if (result.hasNext) {
        nextRow = mappingService.convert(result.next(), schema)
        true
      } else {
        false
      }
    } catch {
      case t: Throwable => {
        error = true
        logInfo("Error while invoking next:", t)
        throw new IOException(t)
      }
    }

  def get: InternalRow = nextRow

  def close(): Unit = {
    Neo4jUtil.closeSafely(transaction, log)
    Neo4jUtil.closeSafely(session, log)
    driverCache.close()
  }

  def hasError(): Boolean = error

  protected def getQueryParameters: util.Map[String, Any] = values
}
