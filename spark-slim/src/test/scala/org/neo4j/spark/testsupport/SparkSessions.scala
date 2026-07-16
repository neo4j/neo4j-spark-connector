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
package org.neo4j.spark.testsupport

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.testcontainers.neo4j.Neo4jContainer

object SparkSessions {
  private var root: SparkSession = _

  def forContainer(container: Neo4jContainer, username: String, password: String): SparkSession = {
    val session = this.synchronized {
      if (root == null || root.sparkContext.isStopped) {
        root = SparkSession.builder()
          .config(new SparkConf()
            .setAppName("neoTest")
            .setMaster("local[*]")
            .set("spark.driver.host", "127.0.0.1"))
          .getOrCreate()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
      root.newSession()
    }

    session.conf.set("neo4j.url", container.getBoltUrl)
    session.conf.set("neo4j.authentication.basic.username", username)
    session.conf.set("neo4j.authentication.basic.password", password)
    SparkSession.setActiveSession(session)
    session
  }

  def release(session: SparkSession): Unit = {
    if (session != null) {
      session.catalog.clearCache()
      if (SparkSession.getActiveSession.contains(session)) {
        SparkSession.clearActiveSession()
      }
    }
  }

  def stop(): Unit = this.synchronized {
    if (root != null && !root.sparkContext.isStopped) {
      root.stop()
    }
    root = null
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }
}
