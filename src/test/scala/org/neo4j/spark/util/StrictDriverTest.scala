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

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.util.ClearSystemProperty
import org.junit.jupiter.api.util.SetSystemProperty
import org.neo4j.driver._
import org.neo4j.driver.internal.summary.InternalGqlNotification
import org.neo4j.driver.summary._

import java.util
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeUnit
import java.util.stream.Stream

import scala.annotation.nowarn
import scala.jdk.CollectionConverters.SetHasAsJava

class StrictDriverTest {

  @Test
  def throws_cypher_warning_exception_when_consumed_result_has_warnings(): Unit = {
    val result = new StubResult(warningSummary())
    val session = new StrictDriver(new StubDriver(new StubSession(result))).session()

    assertThatExceptionOfType(classOf[CypherWarningException])
      .isThrownBy(() => session.run("RETURN 1").consume())
      .withMessageContaining("RETURN 1")
      .withMessageContaining("01N42")
  }

  @Test
  def checks_open_results_when_transaction_closes(): Unit = {
    val result = new StubResult(warningSummary())
    val delegateTransaction = new StubTransaction(result)
    val session = new StrictDriver(new StubDriver(new StubSession(result, delegateTransaction))).session()

    val transaction = session.beginTransaction()
    transaction.run("RETURN 1")

    assertThatExceptionOfType(classOf[CypherWarningException])
      .isThrownBy(() => transaction.close())
      .withMessageContaining("RETURN 1")
      .withMessageContaining("01N42")
    assertThat(delegateTransaction.closed).isTrue
  }

  @Test
  def checks_previous_result_before_running_next_query(): Unit = {
    val result = new StubResult(warningSummary())
    val delegateSession = new StubSession(result)
    val session = new StrictDriver(new StubDriver(delegateSession)).session()

    session.run("RETURN 1")

    assertThatExceptionOfType(classOf[CypherWarningException])
      .isThrownBy(() => session.run("RETURN 2"))
      .withMessageContaining("RETURN 1")
      .withMessageContaining("01N42")
    assertThat(delegateSession.runs).isEqualTo(1)
  }

  @Test
  @SetSystemProperty(key = "strict.cypher", value = "true")
  def driver_cache_wraps_driver_when_strict_mode_is_enabled(): Unit = {
    val strictCache = new DriverCache(neo4jDriverOptions(port = 17600 + (System.nanoTime() % 1000).toInt))
    val strictDriver = strictCache.getOrCreate()
    assertThat(strictDriver).isInstanceOf(classOf[StrictDriver])
    strictCache.close()
  }

  @Test
  @ClearSystemProperty(key = "strict.cypher")
  def driver_cache_uses_regular_driver_when_strict_mode_is_disabled(): Unit = {
    val nonStrictCache = new DriverCache(neo4jDriverOptions(port = 17600 + (System.nanoTime() % 1000).toInt))
    val nonStrictDriver = nonStrictCache.getOrCreate()
    assertThat(nonStrictDriver).isNotInstanceOf(classOf[StrictDriver])
    nonStrictCache.close()
  }

  private def warningSummary(): ResultSummary = {
    val warning = new InternalGqlNotification(
      "01N42",
      "warning",
      util.Collections.emptyMap[String, Value](),
      null,
      NotificationSeverity.WARNING,
      "WARNING",
      null,
      null,
      "Neo.ClientNotification.Statement.UnknownWarning",
      "warning",
      "warning"
    )
    new StubResultSummary(Set[GqlStatusObject](warning).asJava)
  }

  private def neo4jDriverOptions(port: Int): Neo4jDriverOptions = {
    new Neo4jOptions(Map(
      Neo4jOptions.URL -> s"bolt://localhost:$port",
      Neo4jOptions.AUTH_TYPE -> "none",
      org.neo4j.spark.util.QueryType.QUERY.toString.toLowerCase -> "RETURN 1"
    )).connection
  }
}

final private class StubDriver(private val stubSession: Session) extends Driver {

  override def executableQuery(query: String): ExecutableQuery =
    throw new UnsupportedOperationException

  override def executableQueryBookmarkManager(): BookmarkManager =
    throw new UnsupportedOperationException

  override def isEncrypted: Boolean =
    false

  override def session[T <: BaseSession](
    sessionClass: Class[T],
    sessionConfig: SessionConfig,
    sessionAuthToken: AuthToken
  ): T =
    stubSession.asInstanceOf[T]

  override def close(): Unit = ()

  override def closeAsync(): CompletionStage[Void] =
    CompletableFuture.completedStage(null)

  override def verifyConnectivity(): Unit = ()

  override def verifyConnectivityAsync(): CompletionStage[Void] =
    CompletableFuture.completedStage(null)

  override def verifyAuthentication(authToken: AuthToken): Boolean =
    true

  override def supportsSessionAuth(): Boolean =
    true

  override def supportsMultiDb(): Boolean =
    true

  override def supportsMultiDbAsync(): CompletionStage[java.lang.Boolean] =
    CompletableFuture.completedStage(java.lang.Boolean.TRUE)
}

final private class StubSession(
  private val result: Result,
  private val transaction: Transaction = null
) extends Session {

  var runs = 0

  override def beginTransaction(): Transaction =
    transaction

  override def beginTransaction(config: TransactionConfig): Transaction =
    transaction

  override def executeRead[T](callback: TransactionCallback[T], config: TransactionConfig): T =
    throw new UnsupportedOperationException

  override def executeWrite[T](callback: TransactionCallback[T], config: TransactionConfig): T =
    throw new UnsupportedOperationException

  override def run(query: String, parameters: Value): Result = {
    runs += 1
    result
  }

  override def run(query: String, parameters: util.Map[String, Object]): Result = {
    runs += 1
    result
  }

  override def run(query: String, parameters: Record): Result = {
    runs += 1
    result
  }

  override def run(query: String): Result = {
    runs += 1
    result
  }

  override def run(query: Query): Result = {
    runs += 1
    result
  }

  override def run(query: String, config: TransactionConfig): Result = {
    runs += 1
    result
  }

  override def run(query: String, parameters: util.Map[String, Object], config: TransactionConfig): Result = {
    runs += 1
    result
  }

  override def run(query: Query, config: TransactionConfig): Result = {
    runs += 1
    result
  }

  override def lastBookmarks(): util.Set[Bookmark] =
    util.Collections.emptySet()

  override def isOpen: Boolean =
    true

  override def close(): Unit = ()
}

final private class StubTransaction(private val result: Result) extends Transaction {

  var closed = false

  override def run(query: String, parameters: Value): Result =
    result

  override def run(query: String, parameters: util.Map[String, Object]): Result =
    result

  override def run(query: String, parameters: Record): Result =
    result

  override def run(query: String): Result =
    result

  override def run(query: Query): Result =
    result

  override def commit(): Unit = ()

  override def rollback(): Unit = ()

  override def close(): Unit = {
    closed = true
  }

  override def isOpen: Boolean =
    true
}

final private class StubResult(private val resultSummary: ResultSummary) extends Result {

  override def keys(): util.List[String] =
    util.Collections.emptyList()

  override def hasNext: Boolean =
    false

  override def next(): Record =
    throw new NoSuchElementException

  override def single(): Record =
    throw new NoSuchElementException

  override def peek(): Record =
    throw new NoSuchElementException

  override def stream(): Stream[Record] =
    Stream.empty()

  override def list(): util.List[Record] =
    util.Collections.emptyList()

  override def list[T](mapFunction: java.util.function.Function[Record, T]): util.List[T] =
    util.Collections.emptyList()

  override def consume(): ResultSummary =
    resultSummary

  override def isOpen: Boolean =
    true
}

final private class StubResultSummary(private val warnings: util.Set[GqlStatusObject]) extends ResultSummary {

  override def query(): Query =
    new Query("RETURN 1")

  override def counters(): SummaryCounters =
    throw new UnsupportedOperationException

  override def queryType(): org.neo4j.driver.summary.QueryType =
    throw new UnsupportedOperationException

  override def hasPlan(): Boolean =
    false

  override def hasProfile(): Boolean =
    false

  override def plan(): Plan =
    throw new UnsupportedOperationException

  override def profile(): ProfiledPlan =
    throw new UnsupportedOperationException

  @nowarn("cat=deprecation")
  override def notifications(): util.List[Notification] =
    util.Collections.emptyList()

  override def gqlStatusObjects(): util.Set[GqlStatusObject] =
    warnings

  override def resultAvailableAfter(timeUnit: TimeUnit): Long =
    0L

  override def resultConsumedAfter(timeUnit: TimeUnit): Long =
    0L

  override def server(): ServerInfo =
    throw new UnsupportedOperationException

  override def database(): DatabaseInfo =
    throw new UnsupportedOperationException
}
