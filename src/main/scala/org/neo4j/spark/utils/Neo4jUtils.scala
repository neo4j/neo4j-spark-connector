package org.neo4j.spark.utils
import java.util.concurrent.Callable
import java.util.function

import org.neo4j.driver.{Driver, Result, Session, Transaction}
import io.github.resilience4j.retry.{Retry, RetryConfig}
import org.neo4j.driver.exceptions.{ServiceUnavailableException, SessionExpiredException, TransientException}
import org.neo4j.spark.Neo4jConfig


object Neo4jUtils {

  def close(driver: Driver, session: Session): Unit = {
    try {
      if (session != null && session.isOpen) {
        session.close()
      }
    } finally {
      if (driver != null) {
        driver.close()
      }
    }
  }

  private val retryConfig = RetryConfig.custom.retryExceptions(
      classOf[SessionExpiredException], classOf[ServiceUnavailableException] // retry on the same exceptions the driver does [1]
    )
    .retryOnException(new function.Predicate[Throwable] {
      override def test(exception: Throwable): Boolean = if (exception.isInstanceOf[TransientException]) {
        val code = exception.asInstanceOf[TransientException].code
        !("Neo.TransientError.Transaction.Terminated" == code) && !("Neo.TransientError.Transaction.LockClientStopped" == code)
      } else {
        false
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

}
