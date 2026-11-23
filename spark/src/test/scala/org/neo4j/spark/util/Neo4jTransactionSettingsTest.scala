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
import org.junit.jupiter.api.Test
import org.neo4j.driver.Value
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.exceptions.Neo4jException
import org.neo4j.driver.exceptions.ServiceUnavailableException
import org.neo4j.driver.exceptions.TransientException

import java.io.IOException
import java.util.Collections

import scala.jdk.CollectionConverters.SetHasAsJava

class Neo4jTransactionSettingsTest {

  private val DeadlockCode = "Neo.TransientError.Transaction.DeadlockDetected"

  private def settings(codes: String*): Neo4jTransactionSettings =
    Neo4jTransactionSettings(retries = 3, failOnTransactionCodes = codes.toSet, batchSize = 10, retryTimeout = 0)

  private def gqlError(gqlStatus: String, code: String, cause: Throwable = null): Neo4jException =
    new TransientException(
      gqlStatus,
      s"description of $gqlStatus",
      code,
      s"message of $gqlStatus",
      Collections.emptyMap[String, Value](),
      cause
    )

  @Test
  def `matches the Neo4j status code`(): Unit = {
    assertThat(settings(DeadlockCode).shouldFailOn(new TransientException(DeadlockCode, "deadlock"))).isTrue
  }

  @Test
  def `matches the GQLSTATUS code`(): Unit = {
    assertThat(settings("12N34").shouldFailOn(gqlError("12N34", DeadlockCode))).isTrue
  }

  @Test
  def `matches a GQLSTATUS code further down the GQL cause chain`(): Unit = {
    val error = gqlError("50N42", "Neo.DatabaseError.General.UnknownError", gqlError("12N34", DeadlockCode))

    assertThat(settings("12N34").shouldFailOn(error)).isTrue
  }

  @Test
  def `matches a Neo4j exception wrapped in a non Neo4j exception`(): Unit = {
    val error = new IOException(new RuntimeException(new TransientException(DeadlockCode, "deadlock")))

    assertThat(settings(DeadlockCode).shouldFailOn(error)).isTrue
    assertThat(settings("50N42").shouldFailOn(error)).isTrue
  }

  @Test
  def `does not match when no code is listed`(): Unit = {
    val error = gqlError("12N34", DeadlockCode)

    assertThat(settings().shouldFailOn(error)).isFalse
    assertThat(settings("Neo.ClientError.Statement.SyntaxError", "22N01").shouldFailOn(error)).isFalse
  }

  @Test
  def `does not match non Neo4j exceptions`(): Unit = {
    assertThat(settings(DeadlockCode).shouldFailOn(new RuntimeException(DeadlockCode))).isFalse
  }

  @Test
  def `terminates on a cyclic cause chain`(): Unit = {
    val first = new RuntimeException("first")
    val second = new RuntimeException("second", first)
    first.initCause(second)

    assertThat(settings(DeadlockCode).shouldFailOn(first)).isFalse
  }

  @Test
  def `finds a match inside a cyclic cause chain`(): Unit = {
    val wrapper = new RuntimeException("wrapper")
    val deadlock = gqlError("12N34", DeadlockCode, wrapper)
    wrapper.initCause(deadlock)

    assertThat(settings(DeadlockCode).shouldFailOn(wrapper)).isTrue
  }

  @Test
  def `does not match a null exception`(): Unit = {
    assertThat(settings(DeadlockCode).shouldFailOn(null)).isFalse
  }

  @Test
  def `does not match a code that differs only in case`(): Unit = {
    val error = gqlError("12N34", DeadlockCode)

    assertThat(settings("12n34").shouldFailOn(error)).isFalse
    assertThat(settings(DeadlockCode.toLowerCase).shouldFailOn(error)).isFalse
  }

  @Test
  def `parses an empty option into no codes`(): Unit = {
    val options = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "bolt://localhost",
      "labels" -> "Person",
      Neo4jOptions.TRANSACTION_CODES_FAIL -> " , ,"
    ))

    assertThat(options.transactionSettings.failOnTransactionCodes.asJava).isEmpty()
  }

  @Test
  def `parses both kinds of code from the option`(): Unit = {
    val options = new Neo4jOptions(Map(
      Neo4jOptions.URL -> "bolt://localhost",
      "labels" -> "Person",
      Neo4jOptions.TRANSACTION_CODES_FAIL -> s" $DeadlockCode , 12N34,,"
    ))

    assertThat(options.transactionSettings.failOnTransactionCodes.asJava).containsExactlyInAnyOrder(
      DeadlockCode,
      "12N34"
    )
    assertThat(options.transactionSettings.shouldFailOn(gqlError("12N34", "Neo.TransientError.Other"))).isTrue
    assertThat(options.transactionSettings.shouldFailOn(new ClientException(DeadlockCode, "deadlock"))).isTrue
  }
}
