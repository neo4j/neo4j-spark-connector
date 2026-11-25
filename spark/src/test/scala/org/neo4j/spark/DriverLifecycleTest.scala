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
package org.neo4j.spark

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.write.PhysicalWriteInfo
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.MockedConstruction
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
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.neo4j.spark.util.DriverCache
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.ValidateConnection
import org.neo4j.spark.writer.Neo4jBatchWriter

import java.util.Collections

class DriverLifecycleTest {

  private val neo4j = new Neo4j(new Neo4jVersion(5, 26, 0), ENTERPRISE, SELF_MANAGED, Collections.emptySet())

  @Test
  def releasesDriverAfterSuccessfulConnectionValidation(): Unit = {
    val driver = mock(classOf[Driver])
    var driverCache: DriverCache = null
    val construction = mockConstruction(
      classOf[DriverCache],
      (cache: DriverCache, _: MockedConstruction.Context) => {
        driverCache = cache
        when(cache.getOrCreate()).thenReturn(driver)
        ()
      }
    )

    try {
      ValidateConnection(new Neo4jOptions(options), "job-1").validate()
      verify(driverCache).close()
    } finally {
      construction.close()
    }
  }

  @Test
  def releasesIndexAwaitDriverAndSessionBeforeCommit(): Unit = {
    val driver = mock(classOf[Driver])
    val session = mock(classOf[Session])
    var driverCache: DriverCache = null
    when(driver.session(any(classOf[SessionConfig]))).thenReturn(session)
    when(session.isOpen).thenReturn(true)

    val construction = mockConstruction(
      classOf[DriverCache],
      (cache: DriverCache, _: MockedConstruction.Context) => {
        driverCache = cache
        when(cache.getOrCreate()).thenReturn(driver)
        ()
      }
    )

    try {
      val writer = new Neo4jBatchWriter(
        neo4j,
        "job-1",
        new StructType(),
        SaveMode.Append,
        new Neo4jOptions(options)
      )

      writer.createBatchWriterFactory(mock(classOf[PhysicalWriteInfo]))

      verify(driverCache).close()
      verify(session, times(2)).close()

      writer.commit(Array.empty[WriterCommitMessage])
      verify(driverCache, times(2)).close()
    } finally {
      construction.close()
    }
  }

  private def options: Map[String, String] = Map(
    Neo4jOptions.URL -> "bolt://localhost:7687",
    "labels" -> ":Person"
  )
}
