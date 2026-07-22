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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

object DriverCache {
  private case class DriverWithCounter(driver: Driver, atomicCounter: AtomicInteger)

  private val cache: ConcurrentHashMap[Neo4jDriverOptions, DriverWithCounter] =
    new ConcurrentHashMap[Neo4jDriverOptions, DriverWithCounter]
}

class DriverCache(private val options: Neo4jDriverOptions) extends Serializable with AutoCloseable {

  import DriverCache._

  def getOrCreate(): Driver = {
    val driverWithCounter = cache.compute(
      options,
      (_, current) => {
        if (current == null) {
          DriverWithCounter(options.createDriver(), new AtomicInteger(1))
        } else {
          current.atomicCounter.incrementAndGet()
          current
        }
      }
    )
    driverWithCounter.driver
  }

  def close(): Unit = {
    cache.computeIfPresent(
      options,
      (_, current) => {
        if (current.atomicCounter.decrementAndGet() == 0) {
          Neo4jUtil.closeSafely(current.driver)
          null // i.e. remove from cache
        } else {
          current // keep it in cache
        }
      }
    )
  }
}
