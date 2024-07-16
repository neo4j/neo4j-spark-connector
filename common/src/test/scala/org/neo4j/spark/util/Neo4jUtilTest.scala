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
import org.junit.Assert
import org.junit.Test

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
    Assert.assertEquals(expected, actual)
  }

}
