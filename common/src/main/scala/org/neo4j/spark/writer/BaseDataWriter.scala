package org.neo4j.spark.writer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.exceptions.{ClientException, Neo4jException, ServiceUnavailableException, SessionExpiredException, TransientException}
import org.neo4j.driver.{Bookmark, Session, Transaction, Values}
import org.neo4j.spark.service._
import org.neo4j.spark.util.Neo4jUtil.{closeSafety, isRetryableException}
import org.neo4j.spark.util.{DriverCache, Neo4jOptions}

import java.time.Duration
import java.util
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.LockSupport
import scala.collection.JavaConverters._

abstract class BaseDataWriter(jobId: String,
                              partitionId: Int,
                              structType: StructType,
                              saveMode: SaveMode,
                              options: Neo4jOptions,
                              scriptResult: java.util.List[java.util.Map[String, AnyRef]]) extends Logging {
  private val STOPPED_THREAD_EXCEPTION_MESSAGE = "Connection to the database terminated. Thread interrupted while committing the transaction"

  private val driverCache: DriverCache = new DriverCache(options.connection, jobId)

  private var transaction: Transaction = _
  private var session: Session = _

  private val mappingService = new MappingService(new Neo4jWriteMappingStrategy(options), options)

  private val batch: util.List[java.util.Map[String, Object]] = new util.ArrayList[util.Map[String, Object]]()

  private val retries = new CountDownLatch(options.transactionMetadata.retries)

  private val query: String = new Neo4jQueryService(options, new Neo4jQueryWriteStrategy(saveMode)).createQuery()

  private val bookmarks: scala.collection.mutable.Set[Bookmark] = new scala.collection.mutable.LinkedHashSet[Bookmark]()

  def write(record: InternalRow): Unit = {
    batch.add(mappingService.convert(record, structType))
    if (batch.size() == options.transactionMetadata.batchSize) {
      writeBatch()
    }
  }

  private def writeBatch(): Unit = {
    try {
      if (session == null || !session.isOpen) {
        session = driverCache.getOrCreate().session(options.session.toNeo4jSession(bookmarks.toSeq))
      }
      if (transaction == null || !transaction.isOpen) {
        transaction = session.beginTransaction()
      }
      log.info(
        s"""Writing a batch of ${batch.size()} elements to Neo4j,
           |for jobId=$jobId and partitionId=$partitionId
           |with query: $query
           |""".stripMargin)
      val result = transaction.run(query,
        Values.value(Map[String, AnyRef](Neo4jQueryStrategy.VARIABLE_EVENTS -> batch,
          Neo4jQueryStrategy.VARIABLE_SCRIPT_RESULT -> scriptResult).asJava))
      if (log.isDebugEnabled) {
        val summary = result.consume()
        val counters = summary.counters()
        log.debug(
          s"""Batch saved into Neo4j data with:
             | - nodes created: ${counters.nodesCreated()}
             | - nodes deleted: ${counters.nodesDeleted()}
             | - relationships created: ${counters.relationshipsCreated()}
             | - relationships deleted: ${counters.relationshipsDeleted()}
             | - properties set: ${counters.propertiesSet()}
             | - labels added: ${counters.labelsAdded()}
             | - labels removed: ${counters.labelsRemoved()}
             |""".stripMargin)
      }
      transaction.commit()
      bookmarks.add(session.lastBookmark())
      closeSafety(transaction)
      batch.clear()
    } catch {
      case neo4jTransientException: Neo4jException =>
        val code = neo4jTransientException.code()
        if (isRetryableException(neo4jTransientException)
          && !options.transactionMetadata.failOnTransactionCodes.contains(code)
          && retries.getCount > 0) {
          retries.countDown()
          log.info(s"Matched Neo4j transient exception next retry is ${options.transactionMetadata.retries - retries.getCount}")
          close()
          LockSupport.parkNanos(Duration.ofMillis(options.transactionMetadata.retryTimeout).toNanos)
          writeBatch()
        } else {
          logAndThrowException(neo4jTransientException)
        }
      case e: Exception => logAndThrowException(e)
    }
    Unit
  }

  /**
   * df: we check if the thrown exception is STOPPED_THREAD_EXCEPTION. This is the
   * exception that is thrown when the streaming query is interrupted, we don't want to cause
   * any error in this case. The transaction are rolled back automatically.
   */
  private def logAndThrowException(e: Exception): Unit = {
    if(e.isInstanceOf[ServiceUnavailableException] && e.getMessage == STOPPED_THREAD_EXCEPTION_MESSAGE) {
      logWarning(e.getMessage)
    }
    else {
      if (e.isInstanceOf[ClientException]) {
        log.error(s"Cannot commit the transaction because: ${e.getMessage}")
      }
      else {
        log.error("Cannot commit the transaction because the following exception", e)
      }

      throw e
    }
  }

  def commit(): Null = {
    writeBatch()
    close()
    null
  }

  def abort(): Unit = {
    if (transaction != null && transaction.isOpen) {
      try {
        transaction.rollback()
      } catch {
        case e: Throwable => log.warn("Cannot rollback the transaction because of the following exception", e)
      }
    }
    close()
    Unit
  }

  protected def close(): Unit = {
    closeSafety(transaction, log)
    closeSafety(session, log)
  }
}
