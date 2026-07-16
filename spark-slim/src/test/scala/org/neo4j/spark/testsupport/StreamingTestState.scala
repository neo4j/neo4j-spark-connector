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

import org.apache.spark.sql.streaming.StreamingQuery

import scala.collection.mutable

object StreamingTestState {

  final class State {
    var query: StreamingQuery = _
    val createdTables: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String]
  }

  private val currentState = new ThreadLocal[State]()

  def set(): Unit = currentState.set(new State)

  def clear(): Unit = currentState.remove()

  def current: State = {
    val state = currentState.get()
    if (state == null) {
      throw new IllegalStateException("No streaming test state bound to the current thread")
    }
    state
  }
}
