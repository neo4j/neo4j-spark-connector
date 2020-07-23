package org.neo4j.spark.writer

import java.util
import java.util.Collections
import java.util.concurrent.CountDownLatch

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.exceptions.{Neo4jException, ServiceUnavailableException, SessionExpiredException}
import org.neo4j.driver.{Session, Transaction, Values}
import org.neo4j.spark.service.{MappingService, Neo4jQueryService, Neo4jQueryWriteStrategy}
import org.neo4j.spark.util.Neo4jUtil._
import org.neo4j.spark.util.Neo4jImplicits._
import org.neo4j.spark.util.Neo4jUtil
import org.neo4j.spark.{DriverCache, Neo4jOptions}

class Neo4jDataWriter(jobId: String,
                      partitionId: Int,
                      structType: StructType,
                      saveMode: SaveMode,
                      options: Neo4jOptions) extends DataWriter[InternalRow] with Logging {

  private val driverCache: DriverCache = new DriverCache(options.connection, jobId)

  private var transaction: Transaction = _
  private var session: Session = _

  private val mappingService = new MappingService(options)

  private val batch: util.List[java.util.Map[String, Object]] = new util.ArrayList[util.Map[String, Object]]()

  private val retries = new CountDownLatch(options.transactionMetadata.retries)

  val query: String = new Neo4jQueryService(options, new Neo4jQueryWriteStrategy(saveMode)).createQuery()

  override def write(record: InternalRow): Unit = {
    batch.add(mappingService.toParameter(record, structType))
    if (batch.size() == options.nodeMetadata.batchSize) {
      writeBatch
    }
  }

  private def writeBatch(): Unit = {
    if (session == null || !session.isOpen) {
      session = driverCache.getOrCreate().session(options.session.toNeo4jSession)
    }
    if (transaction == null || !transaction.isOpen) {
      transaction = session.beginTransaction()
    }
    try {
      log.info(s"Writing a batch of ${batch.size()} elements to Neo4j, for jobId=$jobId and partitionId=$partitionId")
      if (log.isDebugEnabled) {
        log.debug(s"Writing batch into Neo4j with query: $query")
      }
      val result = transaction.run(query,
        Values.value(Collections.singletonMap[String, Object]("events", batch)))
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
      closeSafety(transaction)
      batch.clear()
    } catch {
      case neo4jTransientException: Neo4jException => {
        val code = neo4jTransientException.code()
        if ((neo4jTransientException.isInstanceOf[SessionExpiredException] || neo4jTransientException.isInstanceOf[ServiceUnavailableException])
            && !(Neo4jUtil.unsupportedTransientCodes ++ options.transactionMetadata.failOnTransactionCodes).contains(code)
            && retries.getCount > 0) {
          retries.countDown()
          log.info(s"Matched Neo4j transient exception next retry is ${options.transactionMetadata.retries - retries.getCount}")
          close
          writeBatch
        } else {
          throw neo4jTransientException
        }
      }
      case e: Exception => {
        log.error("Cannot commit the transaction because of the following exception:", e)
        throw e
      }
    }
    Unit
  }

  override def commit(): WriterCommitMessage = {
    writeBatch
    close
    null
  }

  override def abort(): Unit = {
    transaction.rollback()
    close
    Unit
  }

  private def close = {
    closeSafety(transaction)
    closeSafety(session)
  }
}