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

import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.spark.util.DriverCache.cache
import org.neo4j.spark.util.DriverCache.jobIdCache

import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.function

object DriverCache {
  private val cache: ConcurrentHashMap[Neo4jDriverOptions, Driver] = new ConcurrentHashMap[Neo4jDriverOptions, Driver]
  private val jobIdCache = Collections.newSetFromMap[String](new ConcurrentHashMap[String, java.lang.Boolean]())
}

class DriverCache(private val options: Neo4jDriverOptions, private val jobId: String) extends Serializable
    with AutoCloseable {

  def getOrCreate(): Driver = {
    this.synchronized {
      jobIdCache.add(jobId)
      cache.computeIfAbsent(options, (t: Neo4jDriverOptions) => t.createDriver())
    }
  }

  def close(): Unit = {
    this.synchronized {
      jobIdCache.remove(jobId)
      if (jobIdCache.isEmpty) {
        val driver = cache.remove(options)
        if (driver != null) {
          Neo4jUtil.closeSafely(driver)
        }
      }
    }
  }
}
