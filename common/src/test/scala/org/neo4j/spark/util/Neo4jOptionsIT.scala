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

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Ignore
import org.junit.Test
import org.neo4j.spark.SparkConnectorScalaSuiteIT
import org.neo4j.spark.SparkConnectorScalaSuiteIT.server

class Neo4jOptionsIT extends SparkConnectorScalaSuiteIT {

  @Test
  def shouldConstructDriver(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, server.getBoltUrl)
    options.put(Neo4jOptions.AUTH_TYPE, "none")

    val neo4jOptions = new Neo4jOptions(options)

    use(neo4jOptions.connection.createDriver()) { driver =>
      assertNotNull(driver)

      use(driver.session()) { session =>
        assertEquals(1, session.run("RETURN 1").single().get(0).asInt())
      }
    }
  }

  @Test
  @Ignore("This requires a fix on driver, ignoring until it is implemented")
  def shouldConstructDriverWithResolver(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(
      Neo4jOptions.URL,
      s"neo4j://localhost.localdomain:8888, bolt://localhost.localdomain:9999, ${server.getBoltUrl}"
    )
    options.put(Neo4jOptions.AUTH_TYPE, "none")

    val neo4jOptions = new Neo4jOptions(options)

    use(neo4jOptions.connection.createDriver()) { driver =>
      assertNotNull(driver)

      use(driver.session()) { session =>
        assertEquals(1, session.run("RETURN 1").single().get(0).asInt())
      }
    }
  }

  def use[A <: AutoCloseable, B](resource: A)(code: A ⇒ B): B =
    try
      code(resource)
    finally
      resource.close()

}
