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
import org.apache.spark.sql.types.StructType
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.MockedConstruction
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.mock
import org.mockito.Mockito.mockConstruction
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDeploymentType.SELF_MANAGED
import org.neo4j.caniuse.Neo4jEdition.ENTERPRISE
import org.neo4j.caniuse.Neo4jVersion
import org.neo4j.driver.Driver
import org.neo4j.driver.Result
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.Transaction
import org.neo4j.driver.TransactionConfig
import org.neo4j.driver.Value
import org.neo4j.driver.exceptions.ServiceUnavailableException
import org.neo4j.driver.exceptions.TransientException
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.summary.SummaryCounters
import org.neo4j.spark.util.DriverCache
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.Neo4jUnknownCommitOutcomeException

import java.util.Collections

/**
 * Covers how a failed batch is classified by the phase it failed in, which is what decides whether replaying it is
 * safe. The driver is mocked so that a commit can be made to fail in ways that are impossible to provoke on demand
 * against a real server.
 */
class BaseDataWriterTest {

  private val neo4j = new Neo4j(new Neo4jVersion(5, 26, 0), ENTERPRISE, SELF_MANAGED, Collections.emptySet())

  private val connectionLost = new ServiceUnavailableException("Connection to the database terminated")

  private val deadlock =
    new TransientException("Neo.TransientError.Transaction.DeadlockDetected", "deadlock detected")

  @Test
  def retriesABatchWhoseCommitOutcomeIsUnknownByDefault(): Unit = {
    val fixture = new Fixture()
    doThrow(connectionLost).when(fixture.transaction).commit()

    assertThatExceptionOfType(classOf[ServiceUnavailableException])
      .isThrownBy(() => fixture.writer(Map.empty).commit())

    // The original attempt plus the whole retry budget, all of them replaying a batch that may already be there.
    fixture.verifyDataQueriesRun(3)
  }

  @Test
  def failsWithoutReplayingABatchWhoseCommitOutcomeIsUnknown(): Unit = {
    val fixture = new Fixture()
    doThrow(connectionLost).when(fixture.transaction).commit()

    assertThatExceptionOfType(classOf[Neo4jUnknownCommitOutcomeException])
      .isThrownBy(() => fixture.writer(Map(Neo4jOptions.TRANSACTION_COMMIT_UNKNOWN_OUTCOME -> "FAIL")).commit())
      .withCauseInstanceOf(classOf[ServiceUnavailableException])

    fixture.verifyDataQueriesRun(1)
  }

  @Test
  def stillRetriesAConnectionLostBeforeTheCommitUnderFail(): Unit = {
    val fixture = new Fixture()
    doThrow(connectionLost).when(fixture.transaction).run(anyString(), any(classOf[Value]))

    // The server never saw a COMMIT, so this batch is not in the database and replaying it is safe.
    assertThatExceptionOfType(classOf[ServiceUnavailableException])
      .isThrownBy(() => fixture.writer(Map(Neo4jOptions.TRANSACTION_COMMIT_UNKNOWN_OUTCOME -> "FAIL")).commit())

    fixture.verifyDataQueriesRun(3)
  }

  @Test
  def stillRetriesADeterminateCommitFailureUnderFail(): Unit = {
    val fixture = new Fixture()
    doThrow(deadlock).when(fixture.transaction).commit()

    // The server answered the COMMIT with a failure, so it definitely did not apply the transaction.
    assertThatExceptionOfType(classOf[TransientException])
      .isThrownBy(() => fixture.writer(Map(Neo4jOptions.TRANSACTION_COMMIT_UNKNOWN_OUTCOME -> "FAIL")).commit())

    fixture.verifyDataQueriesRun(3)
  }

  private class Fixture {
    val driver: Driver = mock(classOf[Driver])
    val session: Session = mock(classOf[Session])
    val transaction: Transaction = mock(classOf[Transaction])

    private val result = mock(classOf[Result])
    private val summary = mock(classOf[ResultSummary])

    when(driver.session(any(classOf[SessionConfig]))).thenReturn(session)
    when(session.isOpen).thenReturn(true)
    when(session.beginTransaction(any(classOf[TransactionConfig]))).thenReturn(transaction)
    when(transaction.isOpen).thenReturn(true)
    when(transaction.run(anyString(), any(classOf[Value]))).thenReturn(result)
    when(result.consume()).thenReturn(summary)
    when(summary.counters()).thenReturn(mock(classOf[SummaryCounters]))

    /**
     * Asserts how many times the batch was sent to the server, which is the number that says whether it was replayed.
     */
    def verifyDataQueriesRun(expected: Int): Unit = {
      verify(transaction, times(expected)).run(anyString(), any(classOf[Value]))
    }

    /**
     * Builds a writer whose [[DriverCache]] hands out the mocked driver. The construction mock stays open for the
     * life of the fixture, which is the whole test.
     */
    def writer(extraOptions: Map[String, String]): Neo4jDataWriter = {
      val construction: MockedConstruction[DriverCache] = mockConstruction(
        classOf[DriverCache],
        (cache: DriverCache, _: MockedConstruction.Context) => {
          when(cache.getOrCreate()).thenReturn(driver)
          ()
        }
      )
      try {
        val options = new Neo4jOptions(Map(
          Neo4jOptions.URL -> "bolt://localhost:7687",
          "labels" -> ":Person",
          "access.mode" -> "Write",
          Neo4jOptions.TRANSACTION_RETRIES -> "2"
        ) ++ extraOptions)
        new Neo4jDataWriter(
          neo4j,
          "job-1",
          0,
          new StructType(),
          SaveMode.Append,
          options,
          Collections.emptyList()
        )
      } finally {
        construction.close()
      }
    }
  }
}
