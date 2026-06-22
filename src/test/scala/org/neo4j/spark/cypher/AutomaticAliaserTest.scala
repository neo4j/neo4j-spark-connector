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

package org.neo4j.spark.cypher;

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.argumentSet
import org.junit.jupiter.params.provider.MethodSource

import java.util.stream.Stream;

@TestInstance(Lifecycle.PER_CLASS)
class AutomaticAliaserTest {

  @ParameterizedTest
  @MethodSource(Array("alias_cases"))
  def aliases_query(initialQuery: String, expectedQuery: String): Unit = {
    val query = new AutomaticAliaser().aliasResults(initialQuery)

    assertThat(query).isEqualTo(expectedQuery)
  }

  @ParameterizedTest
  @MethodSource(Array("preserve_cases"))
  def preserves_query(query: String): Unit = {
    val result = new AutomaticAliaser().aliasResults(query)

    assertThat(result).isEqualTo(query)
  }

  private def alias_cases(): Stream[Arguments] = {
    Stream.of(
      argumentSet(
        "unaliased number literal",
        "RETURN 42",
        "RETURN 42 AS `42`"
      ),
      argumentSet(
        "unaliased boolean literal",
        "RETURN false",
        "RETURN false AS false"
      ),
      argumentSet(
        "unaliased string literal",
        "RETURN 'foo'",
        "RETURN 'foo' AS `'foo'`"
      ),
      argumentSet(
        "unaliased array literal",
        "RETURN ['foo']",
        "RETURN ['foo'] AS `['foo']`"
      ),
      argumentSet(
        "unaliased map literal",
        "RETURN {foo: 'bar'}",
        "RETURN {foo: 'bar'} AS `{foo: 'bar'}`"
      ),
      argumentSet(
        "unaliased parameter",
        "RETURN $foo",
        "RETURN $foo AS `$foo`"
      ),
      argumentSet(
        "unaliased function call",
        "UNWIND [1, 2, 3] AS x RETURN avg(x)",
        "UNWIND [1, 2, 3] AS x RETURN avg(x) AS `avg(x)`"
      ),
      argumentSet(
        "unaliased node property",
        "MATCH (n:Person) RETURN n.bar",
        "MATCH (n:Person) RETURN n.bar AS `n.bar`"
      ),
      argumentSet(
        "unaliased relationship property",
        "MATCH ()-[r:LINKS]->() RETURN r.bar",
        "MATCH ()-[r:LINKS]->() RETURN r.bar AS `r.bar`"
      ),
      argumentSet(
        "unaliased field access",
        "UNWIND [{foo: 'bar'}] AS object RETURN object.foo",
        "UNWIND [{foo: 'bar'}] AS object RETURN object.foo AS `object.foo`"
      ),
      argumentSet(
        "unaliased indexed array access",
        "UNWIND [['foo']] AS array RETURN array[0]",
        "UNWIND [['foo']] AS array RETURN array[0] AS `array[0]`"
      )
    )
  }

  private def preserve_cases(): Stream[Arguments] = {
    Stream.of(
      argumentSet(
        "whole node",
        "MATCH (n:Person) RETURN n"
      ),
      argumentSet(
        "whole rel",
        "MATCH ()-[r:LINKS]->() RETURN r"
      ),
      argumentSet(
        "whole path",
        "MATCH p = (:Person)-[:LINKS]->() RETURN p"
      ),
      argumentSet(
        "aliased number literal",
        "RETURN 42 AS foo"
      ),
      argumentSet(
        "aliased boolean literal",
        "RETURN false AS foo"
      ),
      argumentSet(
        "aliased string literal",
        "RETURN 'foo' AS foo"
      ),
      argumentSet(
        "aliased array literal",
        "RETURN ['foo'] AS foo"
      ),
      argumentSet(
        "aliased map literal",
        "RETURN {foo: 'bar'} AS foo"
      ),
      argumentSet(
        "aliased parameter",
        "RETURN $foo AS foo"
      ),
      argumentSet(
        "aliased function call",
        "UNWIND [1, 2, 3] AS x RETURN avg(x) AS foo"
      ),
      argumentSet(
        "aliased node property",
        "MATCH (n:Person) RETURN n.bar AS foo"
      ),
      argumentSet(
        "aliased relationship property",
        "MATCH ()-[r:LINKS]->() RETURN r.bar AS foo"
      ),
      argumentSet(
        "aliased field access",
        "UNWIND [{foo: 'bar'}] AS object RETURN object.foo AS foo"
      ),
      argumentSet(
        "aliased indexed array access",
        "UNWIND [['foo']] AS array RETURN array[0] AS foo"
      )
    )
  }
}
