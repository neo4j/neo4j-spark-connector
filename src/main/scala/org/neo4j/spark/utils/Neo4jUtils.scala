package org.neo4j.spark.utils
import java.sql.Timestamp
import java.time._
import java.util.concurrent.Callable
import java.util.function

import io.github.resilience4j.retry.{Retry, RetryConfig}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.neo4j.driver.exceptions.{ServiceUnavailableException, SessionExpiredException, TransientException}
import org.neo4j.driver.{Driver, Result, Session, Transaction}
import org.neo4j.spark.Neo4jConfig
import org.slf4j.LoggerFactory

class Neo4jUtils

object Neo4jUtils {

  private val logger = LoggerFactory.getLogger(classOf[Neo4jUtils])

  def close(driver: Driver, session: Session): Unit = {
    try {
      if (session != null && session.isOpen) {
        closeSafety(session)
      }
    } finally {
      if (driver != null) {
        closeSafety(driver)
      }
    }
  }

  private def closeSafety(closable: AutoCloseable): Unit = {
    try {
      closable.close()
    } catch {
      case e: Throwable => {
        logger.error("Exception while trying to close an AutoCloseable, because of the following exception", e)
      }
    }
  }

  private val retryConfig = RetryConfig.custom.retryExceptions(
      classOf[SessionExpiredException], classOf[ServiceUnavailableException] // retry on the same exceptions the driver does [1]
    )
    .retryOnException(new function.Predicate[Throwable] {
      override def test(exception: Throwable): Boolean = exception match {
        case t: TransientException => {
          val code = t.code()
          !("Neo.TransientError.Transaction.Terminated" == code) && !("Neo.TransientError.Transaction.LockClientStopped" == code)
        }
        case _ => false
      }
    })
    .maxAttempts(3)
    .build

  def executeTxWithRetries[T](neo4jConfig: Neo4jConfig,
                              query: String,
                              params: java.util.Map[String, AnyRef],
                              write: Boolean): (Driver, Session, Transaction, Result) = {
    val driver: Driver = neo4jConfig.driver()
    val session: Session = driver.session(neo4jConfig.sessionConfig(write))
    Retry.decorateCallable(
        Retry.of("neo4jTransactionRetryPool", retryConfig),
        new Callable[(Driver, Session, Transaction, Result)] {
          override def call(): (Driver, Session, Transaction, Result) = {
            val transaction = session.beginTransaction()
            val result = transaction.run(query, params)
            (driver, session, transaction, result)
          }
        }
      )
      .call()
  }

  def convert(value: AnyRef): AnyRef = value match {
    case m: ZonedDateTime => new Timestamp(DateTimeUtils.fromUTCTime(m.toInstant.toEpochMilli, m.getZone.getId))
    case m: LocalDateTime => new Timestamp(DateTimeUtils.fromUTCTime(m.toInstant(ZoneOffset.UTC).toEpochMilli,"UTC"))
    case m: LocalDate => java.sql.Date.valueOf(m)
    case m: OffsetTime => new Timestamp(m.atDate(LocalDate.ofEpochDay(0)).toInstant.toEpochMilli)
    case _ => value
  }


  def convertFromSpark(value: Any): Any = value match {
    case m: java.sql.Date => m.toLocalDate
    case m: java.sql.Timestamp => m.toInstant.atZone(ZoneOffset.UTC)
    case _ => value
  }

}
