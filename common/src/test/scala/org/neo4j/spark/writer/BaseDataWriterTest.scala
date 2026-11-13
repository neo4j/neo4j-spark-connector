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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDeploymentType
import org.neo4j.caniuse.Neo4jEdition
import org.neo4j.caniuse.Neo4jVersion
import org.neo4j.driver.Driver
import org.neo4j.driver.Result
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.Transaction
import org.neo4j.driver.TransactionConfig
import org.neo4j.driver.Value
import org.neo4j.driver.exceptions.TransientException
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.summary.SummaryCounters
import org.neo4j.spark.util.DriverCache
import org.neo4j.spark.util.Neo4jOptions

import java.util.Collections

import scala.collection.JavaConverters._

/**
 * Covers how `transaction.retries` is applied to batches. The driver is mocked so that transient failures can be
 * injected on demand.
 */
class BaseDataWriterTest {

  private val neo4j =
    new Neo4j(new Neo4jVersion(5, 26, 0), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)

  private val deadlock =
    new TransientException("Neo.TransientError.Transaction.DeadlockDetected", "deadlock detected")

  private var driver: Driver = _
  private var session: Session = _
  private var transaction: Transaction = _
  private var result: Result = _

  @Before
  def setUp(): Unit = {
    driver = mock(classOf[Driver])
    session = mock(classOf[Session])
    transaction = mock(classOf[Transaction])
    result = mock(classOf[Result])
    val summary = mock(classOf[ResultSummary])

    when(driver.session(any(classOf[SessionConfig]))).thenReturn(session)
    when(session.isOpen).thenReturn(true)
    when(session.beginTransaction(any(classOf[TransactionConfig]))).thenReturn(transaction)
    when(transaction.isOpen).thenReturn(true)
    when(transaction.run(anyString(), any(classOf[Value]))).thenReturn(result)
    when(result.consume()).thenReturn(summary)
    when(summary.counters()).thenReturn(mock(classOf[SummaryCounters]))
  }

  @Test
  def givesEveryBatchItsOwnRetryBudget(): Unit = {
    // Batch 1 uses its whole budget of 2 retries before succeeding, then batch 2 hits another deadlock.
    when(transaction.run(anyString(), any(classOf[Value])))
      .thenThrow(deadlock)
      .thenThrow(deadlock)
      .thenReturn(result)
      .thenThrow(deadlock)
      .thenReturn(result)

    val w = writer(Map(Neo4jOptions.BATCH_SIZE -> "1"))
    w.write(InternalRow.empty)
    w.write(InternalRow.empty)

    // Batch 1: original attempt plus 2 retries. Batch 2: original attempt plus 1 retry.
    verifyDataQueriesRun(5)
  }

  @Test
  def stillCapsTheRetriesOfASingleBatch(): Unit = {
    // Batch 1 succeeds at once, so batch 2 is the one that keeps deadlocking.
    when(transaction.run(anyString(), any(classOf[Value])))
      .thenReturn(result)
      .thenThrow(deadlock)

    val w = writer(Map(Neo4jOptions.BATCH_SIZE -> "1"))
    w.write(InternalRow.empty)

    try {
      w.write(InternalRow.empty)
      fail("expected a TransientException once the batch ran out of retries")
    } catch {
      case _: TransientException => // expected
    }

    // Batch 1 once, then batch 2's original attempt plus its 2 retries.
    verifyDataQueriesRun(4)
  }

  /**
   * Asserts how many times a batch was sent to the server, which is the number that says whether it was replayed.
   */
  private def verifyDataQueriesRun(expected: Int): Unit = {
    verify(transaction, times(expected)).run(anyString(), any(classOf[Value]))
  }

  /**
   * Builds a writer whose [[DriverCache]] hands out the mocked driver. The real cache only connects on
   * `getOrCreate()`, so constructing the writer is safe; its cache is then swapped for a mock.
   */
  private def writer(extraOptions: Map[String, String]): BaseDataWriter = {
    val options = new Neo4jOptions((Map(
      Neo4jOptions.URL -> "bolt://localhost:7687",
      "labels" -> ":Person",
      Neo4jOptions.TRANSACTION_RETRIES -> "2",
      Neo4jOptions.TRANSACTION_RETRY_TIMEOUT -> "0"
    ) ++ extraOptions).asJava)

    val writer = new BaseDataWriter(
      neo4j,
      "job-1",
      0,
      new StructType(),
      SaveMode.Append,
      options,
      Collections.emptyList()
    ) {}
    useMockedDriver(writer)
    writer
  }

  private def useMockedDriver(writer: BaseDataWriter): Unit = {
    val cache = mock(classOf[DriverCache])
    when(cache.getOrCreate()).thenReturn(driver)
    val field = classOf[BaseDataWriter].getDeclaredField("driverCache")
    field.setAccessible(true)
    field.set(writer, cache)
  }
}
