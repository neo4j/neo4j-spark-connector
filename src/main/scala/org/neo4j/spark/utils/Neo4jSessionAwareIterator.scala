package org.neo4j.spark.utils

import org.neo4j.driver.exceptions.NoSuchRecordException
import org.neo4j.driver.{Driver, Record, Result, Session, Transaction}
import org.neo4j.spark.Neo4jConfig

class Neo4jSessionAwareIterator(neo4jConfig: Neo4jConfig,
                                query: String,
                                params: java.util.Map[String, AnyRef],
                                write: Boolean)
    extends Iterator[Record] {

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
      case _ => {
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
        close(!e.isInstanceOf[NoSuchRecordException])
        throw e
      }
    }
  }

  private def close(rollback: Boolean = false) = {
    try {
      if (transaction != null && transaction.isOpen) {
        if (rollback && write) {
          transaction.rollback()
        } else {
          transaction.commit()
        }
      }
      if (result != null) {
        result.consume()
      }
    } finally {
      Neo4jUtils.close(driver, session)
    }
  }

}
