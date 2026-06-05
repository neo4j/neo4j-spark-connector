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
package org.neo4j.spark.util

import org.neo4j.driver._
import org.neo4j.driver.summary.GqlNotification
import org.neo4j.driver.summary.ResultSummary

import java.util
import java.util.concurrent.CompletionStage
import java.util.concurrent.CopyOnWriteArrayList
import java.util.stream.Collector
import java.util.stream.Stream

import scala.jdk.CollectionConverters.SetHasAsScala

/**
 * StrictDriver is a delegating driver used by the connector when strict query mode is enabled.
 *
 * The Neo4j Driver exposes Cypher notifications on `ResultSummary`, but result streams are lazy: a query can
 * return a `Result` before the server has produced the final summary that contains warnings. This delegate wraps the
 * synchronous session, transaction, transaction context, and result APIs so every returned `Result` is consumed and
 * checked at lifecycle boundaries such as explicit `consume()`, terminal result operations, the next query on the same
 * runner, and session/transaction closure.
 *
 * If a consumed summary contains warning notifications, the wrapper throws a `CypherWarningException`. It does
 * not alter non-synchronous session APIs; those are delegated unchanged.
 */
class StrictDriver(private val delegate: Driver) extends Driver {

  override def executableQuery(query: String): ExecutableQuery =
    new StrictExecutableQuery(query, delegate.executableQuery(query))

  override def executableQueryBookmarkManager(): BookmarkManager =
    delegate.executableQueryBookmarkManager()

  override def isEncrypted: Boolean =
    delegate.isEncrypted

  override def session(): Session =
    session(SessionConfig.defaultConfig())

  override def session(sessionConfig: SessionConfig): Session =
    session(classOf[Session], sessionConfig, null)

  override def session[T <: BaseSession](sessionClass: Class[T]): T =
    session(sessionClass, SessionConfig.defaultConfig(), null)

  override def session[T <: BaseSession](sessionClass: Class[T], sessionAuthToken: AuthToken): T =
    session(sessionClass, SessionConfig.defaultConfig(), sessionAuthToken)

  override def session[T <: BaseSession](sessionClass: Class[T], sessionConfig: SessionConfig): T =
    session(sessionClass, sessionConfig, null)

  override def session[T <: BaseSession](
    sessionClass: Class[T],
    sessionConfig: SessionConfig,
    sessionAuthToken: AuthToken
  ): T = {
    val session = delegate.session(sessionClass, sessionConfig, sessionAuthToken)
    if (sessionClass == classOf[Session]) {
      new StrictSession(session.asInstanceOf[Session]).asInstanceOf[T]
    } else {
      session
    }
  }

  override def close(): Unit =
    delegate.close()

  override def closeAsync(): CompletionStage[Void] =
    delegate.closeAsync()

  override def verifyConnectivity(): Unit =
    delegate.verifyConnectivity()

  override def verifyConnectivityAsync(): CompletionStage[Void] =
    delegate.verifyConnectivityAsync()

  override def verifyAuthentication(authToken: AuthToken): Boolean =
    delegate.verifyAuthentication(authToken)

  override def supportsSessionAuth(): Boolean =
    delegate.supportsSessionAuth()

  override def supportsMultiDb(): Boolean =
    delegate.supportsMultiDb()

  override def supportsMultiDbAsync(): CompletionStage[java.lang.Boolean] =
    delegate.supportsMultiDbAsync()
}

private class StrictExecutableQuery(
  private val query: String,
  private val delegate: ExecutableQuery
) extends ExecutableQuery {

  override def withParameters(parameters: util.Map[String, Object]): ExecutableQuery =
    new StrictExecutableQuery(query, delegate.withParameters(parameters))

  override def withConfig(config: QueryConfig): ExecutableQuery =
    new StrictExecutableQuery(query, delegate.withConfig(config))

  override def withAuthToken(authToken: AuthToken): ExecutableQuery =
    new StrictExecutableQuery(query, delegate.withAuthToken(authToken))

  override def execute[A, R, T](
    collector: Collector[Record, A, R],
    resultFinisher: ExecutableQuery.ResultFinisher[R, T]
  ): T = {
    delegate.execute(
      collector,
      (keys: util.List[String], result: R, summary: ResultSummary) => {
        StrictResult.checkWarnings(query, summary)
        resultFinisher.finish(keys, result, summary)
      }
    )
  }
}

private class StrictSession(private val delegate: Session) extends Session {

  private val tracker = new StrictResultTracker

  override def beginTransaction(): Transaction = {
    tracker.checkResults()
    new StrictTransaction(delegate.beginTransaction())
  }

  override def beginTransaction(config: TransactionConfig): Transaction = {
    tracker.checkResults()
    new StrictTransaction(delegate.beginTransaction(config))
  }

  override def executeRead[T](callback: TransactionCallback[T], config: TransactionConfig): T = {
    tracker.checkResults()
    delegate.executeRead(wrapTransactionCallback(callback), config)
  }

  override def executeWrite[T](callback: TransactionCallback[T], config: TransactionConfig): T = {
    tracker.checkResults()
    delegate.executeWrite(wrapTransactionCallback(callback), config)
  }

  override def run(query: String, parameters: Value): Result =
    track(query, delegate.run(query, parameters))

  override def run(query: String, parameters: util.Map[String, Object]): Result =
    track(query, delegate.run(query, parameters))

  override def run(query: String, parameters: Record): Result =
    track(query, delegate.run(query, parameters))

  override def run(query: String): Result =
    track(query, delegate.run(query))

  override def run(query: Query): Result =
    track(query.text(), delegate.run(query))

  override def run(query: String, config: TransactionConfig): Result =
    track(query, delegate.run(query, config))

  override def run(query: String, parameters: util.Map[String, Object], config: TransactionConfig): Result =
    track(query, delegate.run(query, parameters, config))

  override def run(query: Query, config: TransactionConfig): Result =
    track(query.text(), delegate.run(query, config))

  override def lastBookmarks(): util.Set[Bookmark] =
    delegate.lastBookmarks()

  override def isOpen: Boolean =
    delegate.isOpen

  override def close(): Unit =
    closeAfterCheckingResults(delegate.close())

  private def wrapTransactionCallback[T](callback: TransactionCallback[T]): TransactionCallback[T] =
    (context: TransactionContext) => {
      val strictContext = new StrictTransactionContext(context)
      val result = callback.execute(strictContext)
      strictContext.checkResults()
      result
    }

  private def track(query: String, result: => Result): Result = {
    tracker.checkResults()
    tracker.track(query, result)
  }

  private def closeAfterCheckingResults(closeDelegate: => Unit): Unit = {
    var failure: Throwable = null
    try {
      tracker.checkResults()
    } catch {
      case throwable: Throwable => failure = throwable
    }
    try {
      closeDelegate
    } catch {
      case throwable: Throwable =>
        if (failure != null) {
          failure.addSuppressed(throwable)
        } else {
          failure = throwable
        }
    }
    if (failure != null) {
      throw failure
    }
  }
}

final private class StrictTransaction(private val delegate: Transaction) extends Transaction {

  private val tracker = new StrictResultTracker

  override def run(query: String, parameters: Value): Result =
    track(query, delegate.run(query, parameters))

  override def run(query: String, parameters: util.Map[String, Object]): Result =
    track(query, delegate.run(query, parameters))

  override def run(query: String, parameters: Record): Result =
    track(query, delegate.run(query, parameters))

  override def run(query: String): Result =
    track(query, delegate.run(query))

  override def run(query: Query): Result =
    track(query.text(), delegate.run(query))

  override def commit(): Unit = {
    checkResultsBeforeClosing()
    delegate.commit()
  }

  override def rollback(): Unit =
    closeAfterCheckingResults(delegate.rollback())

  override def close(): Unit =
    closeAfterCheckingResults(delegate.close())

  override def isOpen: Boolean =
    delegate.isOpen

  private def track(query: String, result: => Result): Result = {
    tracker.checkResults()
    tracker.track(query, result)
  }

  private def checkResultsBeforeClosing(): Unit = {
    try {
      tracker.checkResults()
    } catch {
      case throwable: Throwable =>
        try {
          delegate.rollback()
        } catch {
          case rollbackThrowable: Throwable => throwable.addSuppressed(rollbackThrowable)
        }
        throw throwable
    }
  }

  private def closeAfterCheckingResults(closeDelegate: => Unit): Unit = {
    var failure: Throwable = null
    try {
      tracker.checkResults()
    } catch {
      case throwable: Throwable => failure = throwable
    }
    try {
      closeDelegate
    } catch {
      case throwable: Throwable =>
        if (failure != null) {
          failure.addSuppressed(throwable)
        } else {
          failure = throwable
        }
    }
    if (failure != null) {
      throw failure
    }
  }
}

final private class StrictTransactionContext(private val delegate: TransactionContext) extends TransactionContext {

  private val tracker = new StrictResultTracker

  override def run(query: String, parameters: Value): Result =
    track(query, delegate.run(query, parameters))

  override def run(query: String, parameters: util.Map[String, Object]): Result =
    track(query, delegate.run(query, parameters))

  override def run(query: String, parameters: Record): Result =
    track(query, delegate.run(query, parameters))

  override def run(query: String): Result =
    track(query, delegate.run(query))

  override def run(query: Query): Result =
    track(query.text(), delegate.run(query))

  def checkResults(): Unit =
    tracker.checkResults()

  private def track(query: String, result: => Result): Result = {
    tracker.checkResults()
    tracker.track(query, result)
  }
}

final private class StrictResultTracker {

  private val results = new CopyOnWriteArrayList[StrictResult]()

  def track(query: String, result: Result): Result = {
    val strictResult = new StrictResult(query, result)
    results.add(strictResult)
    strictResult
  }

  def checkResults(): Unit = {
    val iterator = results.iterator()
    while (iterator.hasNext) {
      val result = iterator.next()
      result.consumeAndCheck()
      results.remove(result)
    }
  }
}

final private class StrictResult(
  private val query: String,
  private val delegate: Result
) extends Result {

  @volatile
  private var summary: ResultSummary = _

  @volatile
  private var warningFailure: CypherWarningException = _

  override def keys(): util.List[String] =
    delegate.keys()

  override def hasNext: Boolean = {
    val hasRecord = delegate.hasNext
    if (!hasRecord) {
      consumeAndCheck()
    }
    hasRecord
  }

  override def next(): Record =
    delegate.next()

  override def single(): Record = {
    val record = delegate.single()
    consumeAndCheck()
    record
  }

  override def peek(): Record =
    delegate.peek()

  override def stream(): Stream[Record] =
    delegate.stream().onClose(() => consumeAndCheck())

  override def list(): util.List[Record] = {
    val records = delegate.list()
    consumeAndCheck()
    records
  }

  override def list[T](mapFunction: java.util.function.Function[Record, T]): util.List[T] = {
    val records = delegate.list(mapFunction)
    consumeAndCheck()
    records
  }

  override def consume(): ResultSummary =
    consumeAndCheck()

  override def isOpen: Boolean =
    delegate.isOpen

  override def remove(): Unit =
    delegate.remove()

  def consumeAndCheck(): ResultSummary = synchronized {
    if (warningFailure != null) {
      throw warningFailure
    }
    if (summary == null) {
      summary = delegate.consume()
    }
    try {
      StrictResult.checkWarnings(query, summary)
    } catch {
      case exception: CypherWarningException =>
        warningFailure = exception
        throw exception
    }
    summary
  }
}

private object StrictResult {

  private val ignoredCodes = Set(
    // unknown node label - ignored since most tests start from an empty database
    "01N50",
    // unknown rel type - ignored since most tests start from an empty database
    "01N51",
    // unknown property key - ignored since most tests start from an empty database
    "01N52"
  )

  def checkWarnings(query: String, summary: ResultSummary): Unit = {
    val queryWarnings = warnings(summary)
    if (queryWarnings.nonEmpty) {
      throw new CypherWarningException(query, queryWarnings)
    }
  }

  private def warnings(summary: ResultSummary): Set[GqlNotification] = {
    summary.gqlStatusObjects()
      .asScala
      .filter {
        case notification: GqlNotification =>
          !ignoredCodes.contains(notification.gqlStatus()) &&
          notification.severity()
            .filter(sev => sev == NotificationSeverity.WARNING)
            .isPresent
        case _ => false
      }
      .map(obj => obj.asInstanceOf[GqlNotification])
      .toSet
  }
}

class CypherWarningException(query: String, warnings: Set[GqlNotification])
    extends Exception(s"Query $query has produced the following warnings: ${CypherWarningFormatter.format(warnings)}")

object CypherWarningFormatter {

  def format(warnings: Set[GqlNotification]): String = {
    warnings.map(w => s"${w.gqlStatus()}: ${w.statusDescription()}").mkString(", ")
  }
}
