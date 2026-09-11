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

import org.apache.commons.lang3.StringUtils
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.exceptions.ConnectionReadTimeoutException
import org.neo4j.driver.exceptions.ServiceUnavailableException
import org.neo4j.driver.exceptions.SessionExpiredException
import org.neo4j.driver.exceptions.TransientException

class Neo4jUtilTest {

  @Test
  def testSafetyCloseShouldNotFailWithNull(): Unit = {
    Neo4jUtil.closeSafely(null)
  }

  @Test
  def testConnectorEnv(): Unit = {
    val expected = if (StringUtils.isNotBlank(System.getenv("DATABRICKS_RUNTIME_VERSION"))) {
      "databricks"
    } else {
      "spark"
    }
    val actual = Neo4jUtil.connectorEnv
    assertEquals(expected, actual)
  }

  @Test
  def testConnectorEnvForCustom(): Unit = {
    System.setProperty("neo4j.spark.platform", "abc")
    val actual = Neo4jUtil.connectorEnv
    assertEquals("abc", actual)
  }

  @Test
  def testIsConnectionFailureForLostConnections(): Unit = {
    assertTrue(Neo4jUtil.isConnectionFailure(new ServiceUnavailableException("connection terminated")))
    assertTrue(Neo4jUtil.isConnectionFailure(new SessionExpiredException("server no longer available")))
    assertTrue(Neo4jUtil.isConnectionFailure(ConnectionReadTimeoutException.INSTANCE))
  }

  @Test
  def testIsConnectionFailureWalksTheCauseChain(): Unit = {
    val wrapped = new RuntimeException("wrapper", new ServiceUnavailableException("connection terminated"))
    assertTrue(Neo4jUtil.isConnectionFailure(wrapped))
  }

  @Test
  def testIsConnectionFailureForAnswersFromTheServer(): Unit = {
    assertFalse(Neo4jUtil.isConnectionFailure(null))
    assertFalse(Neo4jUtil.isConnectionFailure(new ClientException(
      "Neo.ClientError.Schema.ConstraintValidationFailed",
      "already exists"
    )))
    // Retryable, but a definite answer: the server did not apply the transaction.
    assertFalse(Neo4jUtil.isConnectionFailure(new TransientException(
      "Neo.TransientError.Transaction.DeadlockDetected",
      "deadlock"
    )))
  }

  @Test
  def testTransientExceptionsStayRetryable(): Unit = {
    assertTrue(Neo4jUtil.isRetryableException(new TransientException(
      "Neo.TransientError.Transaction.DeadlockDetected",
      "deadlock"
    )))
  }

}
