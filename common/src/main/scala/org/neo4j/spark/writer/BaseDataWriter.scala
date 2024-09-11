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
package org.neo4j.spark.writer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.Session
import org.neo4j.driver.Transaction
import org.neo4j.driver.Values
import org.neo4j.driver.exceptions.ServiceUnavailableException
import org.neo4j.spark.service._
import org.neo4j.spark.util.DriverCache
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.Neo4jUtil.closeSafely
import org.neo4j.spark.util.Neo4jUtil.isRetryableException

import java.io.Closeable
import java.time.Duration
import java.util
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.LockSupport

import scala.annotation.tailrec
import scala.collection.JavaConverters._

abstract class BaseDataWriter(
  jobId: String,
  partitionId: Int,
  structType: StructType,
  saveMode: SaveMode,
  options: Neo4jOptions,
  scriptResult: java.util.List[java.util.Map[String, AnyRef]]
) extends Logging with Closeable with DataWriter[InternalRow] {

  private val STOPPED_THREAD_EXCEPTION_MESSAGE =
    "Connection to the database terminated. Thread interrupted while committing the transaction"

  private val driverCache: DriverCache = new DriverCache(options.connection, jobId)

  private var transaction: Transaction = _
  private var session: Session = _

  private val mappingService = new MappingService(new Neo4jWriteMappingStrategy(options), options)

  private val batch: util.List[java.util.Map[String, Object]] = new util.ArrayList[util.Map[String, Object]]()

  private val retries = new CountDownLatch(options.transactionSettings.retries)

  private val query: String = new Neo4jQueryService(options, new Neo4jQueryWriteStrategy(saveMode)).createQuery()

  private val metrics = DataWriterMetrics()

  def write(record: InternalRow): Unit = {
    batch.add(mappingService.convert(record, structType))
    if (batch.size() == options.transactionSettings.batchSize) {
      writeBatch()
    }
  }

  @tailrec
  private def writeBatch(): Unit = {
    try {
      if (session == null || !session.isOpen) {
        session = driverCache.getOrCreate().session(options.session.toNeo4jSession())
      }
      if (transaction == null || !transaction.isOpen) {
        transaction = session.beginTransaction()
      }
      log.info(
        s"""Writing a batch of ${batch.size()} elements to Neo4j,
           |for jobId=$jobId and partitionId=$partitionId
           |with query: $query
           |""".stripMargin
      )
      val result = transaction.run(
        query,
        Values.value(Map[String, AnyRef](
          Neo4jQueryStrategy.VARIABLE_EVENTS -> batch,
          Neo4jQueryStrategy.VARIABLE_SCRIPT_RESULT -> scriptResult
        ).asJava)
      )
      val summary = result.consume()
      val counters = summary.counters()
      if (log.isDebugEnabled) {
        log.debug(
          s"""Batch saved into Neo4j data with:
             | - nodes created: ${counters.nodesCreated()}
             | - nodes deleted: ${counters.nodesDeleted()}
             | - relationships created: ${counters.relationshipsCreated()}
             | - relationships deleted: ${counters.relationshipsDeleted()}
             | - properties set: ${counters.propertiesSet()}
             | - labels added: ${counters.labelsAdded()}
             | - labels removed: ${counters.labelsRemoved()}
             |""".stripMargin
        )
      }
      transaction.commit()

      // update metrics
      metrics.applyCounters(batch.size(), counters)

      closeSafely(transaction)
      batch.clear()
    } catch {
      case e: Throwable =>
        if (options.transactionSettings.shouldFailOn(e)) {
          log.error("unable to write batch due to explicitly configured failure condition", e)
          throw e
        }

        if (isRetryableException(e) && retries.getCount > 0) {
          retries.countDown()
          log.info(
            s"encountered a transient exception while writing batch, retrying ${options.transactionSettings.retries - retries.getCount} time",
            e
          )
          close()
          LockSupport.parkNanos(Duration.ofMillis(options.transactionSettings.retryTimeout).toNanos)
          writeBatch()
        } else {
          logAndThrowException(e)
        }
    }
  }

  /**
   * df: we check if the thrown exception is STOPPED_THREAD_EXCEPTION. This is the
   * exception that is thrown when the streaming query is interrupted, we don't want to cause
   * any error in this case. The transaction are rolled back automatically.
   */
  private def logAndThrowException(e: Throwable): Unit = {
    if (e.isInstanceOf[ServiceUnavailableException] && e.getMessage == STOPPED_THREAD_EXCEPTION_MESSAGE) {
      logWarning(e.getMessage)
    } else {
      logError("unable to write batch", e)
    }

    throw e
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
  }

  def close(): Unit = {
    closeSafely(transaction, log)
    closeSafely(session, log)
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = metrics.metricValues()
}
