package org.neo4j.spark.utils

import org.neo4j.driver.Record
import org.neo4j.driver.exceptions.{NoSuchRecordException, ResultConsumedException}
import org.neo4j.spark.Neo4jConfig
import org.slf4j.{Logger, LoggerFactory}

class Neo4jSessionAwareIterator(neo4jConfig: Neo4jConfig,
                                query: String,
                                params: java.util.Map[String, AnyRef],
                                write: Boolean)
    extends Iterator[Record] {

  private val logger = LoggerFactory.getLogger(classOf[Neo4jSessionAwareIterator])

  lazy val (driver, session, transaction, result) = Neo4jUtils.executeTxWithRetries(neo4jConfig, query, params, write)

  def peek(): Record = {
    result.peek()
  }

  override def hasNext: Boolean = {
    try {
      val hasNext = result.hasNext
      if (!hasNext) {
        close()
      }
      hasNext
    } catch {
      case e: Throwable => {
        if (!e.isInstanceOf[NoSuchRecordException] && !e.isInstanceOf[ResultConsumedException]) {
          logger.error("Error while executing hasNext method because of the following exception:", e)
        }
        close()
        false
      }
    }
  }

  override def next(): Record = {
    try {
      result.next()
    } catch {
      case e: Throwable => {
        close(!e.isInstanceOf[NoSuchRecordException] && !e.isInstanceOf[ResultConsumedException])
        throw e
      }
    }
  }

  private def close(rollback: Boolean = false) = {
    try {
      if (result != null) {
        result.consume()
      }
      if (transaction != null && transaction.isOpen) {
        if (rollback && write) {
          transaction.rollback()
        } else {
          transaction.commit()
        }
      }
    } catch {
      case _ => // ignore
    } finally {
      Neo4jUtils.close(driver, session)
    }
  }

}
