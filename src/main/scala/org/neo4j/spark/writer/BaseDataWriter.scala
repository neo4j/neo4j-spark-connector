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
import org.neo4j.caniuse.Neo4j
import org.neo4j.driver.Session
import org.neo4j.driver.Transaction
import org.neo4j.driver.Values
import org.neo4j.driver.exceptions.ServiceUnavailableException
import org.neo4j.spark.cypher.CypherRenderer
import org.neo4j.spark.cypher.QueryEmbedder
import org.neo4j.spark.service._
import org.neo4j.spark.util.DriverCache
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.Neo4jUnknownCommitOutcomeException
import org.neo4j.spark.util.Neo4jUtil.closeSafely
import org.neo4j.spark.util.Neo4jUtil.isConnectionFailure
import org.neo4j.spark.util.Neo4jUtil.isRetryableException
import org.neo4j.spark.util.UnknownCommitOutcome

import java.io.Closeable
import java.time.Duration
import java.util
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.LockSupport

import scala.jdk.CollectionConverters.MapHasAsJava

abstract class BaseDataWriter(
  neo4j: Neo4j,
  jobId: String,
  partitionId: Int,
  structType: StructType,
  saveMode: SaveMode,
  options: Neo4jOptions,
  scriptResult: java.util.List[java.util.Map[String, AnyRef]]
) extends Logging with Closeable with DataWriter[InternalRow] {

  import BaseDataWriter._

  private val STOPPED_THREAD_EXCEPTION_MESSAGE =
    "Connection to the database terminated. Thread interrupted while committing the transaction"

  private val driverCache: DriverCache = new DriverCache(options.connection)

  private var transaction: Transaction = _
  private var session: Session = _

  private val mappingService = new MappingService(new Neo4jWriteMappingStrategy(options), options)

  private val batch: util.List[java.util.Map[String, Object]] = new util.ArrayList[util.Map[String, Object]]()

  private val retries = new CountDownLatch(options.transactionSettings.retries)

  private val query: String =
    new Neo4jQueryService(
      options,
      new Neo4jQueryWriteStrategy(neo4j, new CypherRenderer(neo4j, options), new QueryEmbedder(), saveMode)
    ).createQuery()

  private val metrics = DataWriterMetrics()

  private var skipped = 0

  def write(record: InternalRow): Unit = {
    val mapped = mappingService.convert(record, structType)
    mapped match {
      case Some(m) => batch.add(m)
      case None =>
        skipped += 1
    }
    if (batch.size() == options.transactionSettings.batchSize) {
      writeBatch()
    }
  }

  private def writeBatch(): Unit = {
    var retry = true
    while (retry) {
      retry = false
      try {
        attemptBatch()
      } catch {
        case failure: BatchAttemptFailure => retry = handleFailure(failure)
      }
    }
  }

  /**
   * Runs the batch once, tracking how far it got so that a failure can be interpreted. Any failure is wrapped in a
   * [[BatchAttemptFailure]] carrying that phase, which is what makes the difference between "the server never
   * applied this" and "the server may have applied this" visible to the caller.
   */
  private def attemptBatch(): Unit = {
    var phase: CommitPhase = BeforeCommit
    try {
      if (session == null || !session.isOpen) {
        session = driverCache.getOrCreate().session(options.session.toNeo4jSession())
      }
      if (transaction == null || !transaction.isOpen) {
        transaction = session.beginTransaction(options.toNeo4jTransactionConfig)
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

      phase = Committing
      transaction.commit()
      phase = Committed

      logSkipped()

      // update metrics
      metrics.applyCounters(batch.size(), counters)

      closeSafely(transaction)
      batch.clear()
    } catch {
      case e: Throwable => throw new BatchAttemptFailure(phase, e)
    }
  }

  /**
   * @return `true` if the batch should be attempted again, `false` if it is done. Throws if the task must fail.
   */
  private def handleFailure(failure: BatchAttemptFailure): Boolean = {
    val e = failure.cause

    if (failure.phase == Committed) {
      // The commit returned normally, so the batch is in the database whatever failed afterwards. Drop it rather
      // than let a later attempt replay work that has already been applied.
      batch.clear()
      logAndThrowException(e)
    }

    if (options.transactionSettings.shouldFailOn(e)) {
      log.error("unable to write batch due to explicitly configured failure condition", e)
      throw e
    }

    // A streaming query being torn down interrupts the commit thread, which technically leaves the outcome unknown
    // too. Spark replays the epoch either way, so there is nothing for the policy below to add here.
    if (failure.phase == Committing && isConnectionFailure(e) && !isStoppedThread(e)) {
      handleUnknownCommitOutcome(e)
    } else {
      retryOrThrow(e)
    }
  }

  private def isStoppedThread(e: Throwable): Boolean =
    e.isInstanceOf[ServiceUnavailableException] && e.getMessage == STOPPED_THREAD_EXCEPTION_MESSAGE

  private def retryOrThrow(e: Throwable): Boolean = {
    if (isRetryableException(e) && retries.getCount > 0) {
      retries.countDown()
      log.info(
        s"encountered a transient exception while writing batch, retrying ${options.transactionSettings.retries - retries.getCount} time",
        e
      )
      close()
      LockSupport.parkNanos(Duration.ofMillis(options.transactionSettings.retryTimeout).toNanos)
      true
    } else {
      logAndThrowException(e)
    }
  }

  /**
   * The connection dropped while the answer to `COMMIT` was in flight, so the server may or may not have applied
   * the batch. Which of the two bad options to take is the user's call.
   */
  private def handleUnknownCommitOutcome(e: Throwable): Boolean = {
    val context =
      s"the outcome of the commit of a batch of ${batch.size()} elements is unknown " +
        s"(jobId=$jobId, partitionId=$partitionId): the connection dropped before the server answered"

    options.transactionSettings.unknownCommitOutcome match {
      case UnknownCommitOutcome.RETRY =>
        logWarning(
          s"$context. Replaying the batch as configured by " +
            s"'${Neo4jOptions.TRANSACTION_COMMIT_UNKNOWN_OUTCOME}=${UnknownCommitOutcome.RETRY}'. If the commit had " +
            s"in fact been applied and this write is not idempotent, the batch is now duplicated. Use " +
            s"'${Neo4jOptions.TRANSACTION_COMMIT_UNKNOWN_OUTCOME}=${UnknownCommitOutcome.FAIL}' to have the task " +
            s"fail instead of replaying it.",
          e
        )
        retryOrThrow(e)

      case UnknownCommitOutcome.FAIL =>
        close()
        throw new Neo4jUnknownCommitOutcomeException(
          s"$context. Failing the task without replaying the batch, as configured by " +
            s"'${Neo4jOptions.TRANSACTION_COMMIT_UNKNOWN_OUTCOME}=${UnknownCommitOutcome.FAIL}'.",
          e
        )
    }
  }

  private def logSkipped(): Unit = {
    if (skipped > 0) {
      log.info(s"Skipped $skipped rows that contained null values in one of their key property values.")
      skipped = 0
    }
  }

  /**
   * df: we check if the thrown exception is STOPPED_THREAD_EXCEPTION. This is the
   * exception that is thrown when the streaming query is interrupted, we don't want to cause
   * any error in this case. The transaction are rolled back automatically.
   */
  private def logAndThrowException(e: Throwable): Nothing = {
    if (isStoppedThread(e)) {
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

private[spark] object BaseDataWriter {

  /**
   * How far an attempt at writing a batch got before it failed.
   */
  sealed private trait CommitPhase

  /** The transaction had not been asked to commit, so the server cannot have applied it. */
  private case object BeforeCommit extends CommitPhase

  /** `COMMIT` had been sent but not answered, so whether the server applied it is unknown. */
  private case object Committing extends CommitPhase

  /** `COMMIT` was answered successfully, so the server definitely applied it. */
  private case object Committed extends CommitPhase

  private class BatchAttemptFailure(val phase: CommitPhase, val cause: Throwable) extends RuntimeException(cause)
}
