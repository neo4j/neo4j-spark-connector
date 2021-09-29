package org.neo4j.spark.reader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.{Record, Session, Transaction, Values}
import org.neo4j.spark.service.{MappingService, Neo4jQueryReadStrategy, Neo4jQueryService, Neo4jQueryStrategy, Neo4jReadMappingStrategy, PartitionSkipLimit}
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Neo4jUtil}

import java.io.IOException
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

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
  protected val name: String = if (partitionSkipLimit.partitionNumber > 0) s"$jobId-${partitionSkipLimit.partitionNumber}" else jobId
  protected val driverCache: DriverCache = new DriverCache(options.connection, name)

  private val query: String = new Neo4jQueryService(options, new Neo4jQueryReadStrategy(filters, partitionSkipLimit, requiredColumns.fieldNames))
    .createQuery()

  private var nextRow: InternalRow = _

  private lazy val values = {
    val params = mutable.HashMap[String, Any]()
    params.put(Neo4jQueryStrategy.VARIABLE_SCRIPT_RESULT, scriptResult)
    Neo4jUtil.paramsFromFilters(filters)
      .foreach(p => params.put(p._1, p._2))

    params.asJava
  }

  private val mappingService = new MappingService(new Neo4jReadMappingStrategy(options, requiredColumns), options)

  @volatile
  private var error: Boolean = false

  @throws(classOf[IOException])
  def next: Boolean = try {
    if (result == null) {
      session = driverCache.getOrCreate().session(options.session.toNeo4jSession())
      transaction = session.beginTransaction()

      val queryParams = getQueryParameters

      logDebug(s"Running the following query on Neo4j: $query\nwith parameters $queryParams")
      logInfo(s"Running the following query on Neo4j: $query")

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
    Neo4jUtil.closeSafety(transaction, log)
    Neo4jUtil.closeSafety(session, log)
    driverCache.close()
  }

  def hasError(): Boolean = error

  protected def getQueryParameters: util.Map[String, Any] = values
}
