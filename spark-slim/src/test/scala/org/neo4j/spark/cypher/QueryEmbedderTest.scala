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
package org.neo4j.spark.cypher

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.argumentSet
import org.junit.jupiter.params.provider.MethodSource

import java.util.stream.Stream

@TestInstance(Lifecycle.PER_CLASS)
class QueryEmbedderTest {

  private val embedder = new QueryEmbedder()

  @ParameterizedTest
  @MethodSource(Array("embed_cases"))
  def embeds_user_defined_query(query: String, scriptResult: String, expected: String): Unit = {
    val result = embedder.embed(query, scriptResult).build().getCypher

    assertThat(result).isEqualTo(expected)
  }

  private def embed_cases(): Stream[Arguments] =
    Stream.of(
      argumentSet(
        "returns automatically aliased fields",
        "MATCH (o:Object) RETURN o.name",
        "",
        "CALL () {MATCH (o:Object) RETURN o.name AS `o.name`} RETURN `o.name`"
      ),
      argumentSet(
        "preserves return all",
        "MATCH (o:Object) RETURN *",
        "",
        "CALL () {MATCH (o:Object) RETURN *} RETURN *"
      ),
      argumentSet(
        "embeds script result prelude",
        "MATCH (o:Object) RETURN o",
        "WITH $scriptResult AS scriptResult ",
        "CALL () {WITH $scriptResult AS scriptResult MATCH (o:Object) RETURN o} RETURN o"
      ),
      argumentSet(
        "uses final return order from nested call",
        "MATCH (n) CALL (n) { RETURN 1 AS a, 2 AS b } RETURN b AS x, a AS y",
        "",
        "CALL () {MATCH (n) CALL (*) {RETURN 1 AS a, 2 AS b} RETURN b AS x, a AS y} RETURN x, y"
      ),
      argumentSet(
        "uses final return names from deeper nested call",
        "CALL { CALL { RETURN 1 AS a, 2 AS b } RETURN b AS c, a AS d } RETURN d AS x, c AS y",
        "",
        "CALL () {CALL () {CALL () {RETURN 1 AS a, 2 AS b} RETURN b AS c, a AS d} RETURN d AS x, c AS y} RETURN x, y"
      ),
      argumentSet(
        "preserves order after asterisk",
        "WITH 42 as y RETURN *, y AS x",
        "",
        "CALL () {WITH 42 AS y RETURN *, y AS x} RETURN *, x"
      )
    )
}
