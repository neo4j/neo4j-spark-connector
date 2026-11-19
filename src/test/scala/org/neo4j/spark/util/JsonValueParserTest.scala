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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

import java.math.BigInteger

class JsonValueParserTest {

  @Test
  def parses_numbers_into_the_smallest_fitting_type(): Unit = {
    assertThat(JsonValueParser.parse("1")).isEqualTo(Integer.valueOf(1))
    assertThat(JsonValueParser.parse("-2147483648")).isEqualTo(Integer.valueOf(Int.MinValue))
    assertThat(JsonValueParser.parse("2147483648")).isEqualTo(java.lang.Long.valueOf(2147483648L))
    assertThat(JsonValueParser.parse("9223372036854775808")).isEqualTo(new BigInteger("9223372036854775808"))
    assertThat(JsonValueParser.parse("42.0")).isEqualTo(java.lang.Double.valueOf(42.0))
    assertThat(JsonValueParser.parse("1e3")).isEqualTo(java.lang.Double.valueOf(1000.0))
  }

  @Test
  def parses_primitives(): Unit = {
    assertThat(JsonValueParser.parse("true")).isEqualTo(java.lang.Boolean.TRUE)
    assertThat(JsonValueParser.parse("false")).isEqualTo(java.lang.Boolean.FALSE)
    assertThat(JsonValueParser.parse("null").asInstanceOf[AnyRef]).isNull()
    assertThat(JsonValueParser.parse("\"foo\"")).isEqualTo("foo")
  }

  @Test
  def returns_raw_value_when_not_json(): Unit = {
    assertThat(JsonValueParser.parse("foo")).isEqualTo("foo")
    assertThat(JsonValueParser.parse("")).isEqualTo("")
    assertThat(JsonValueParser.parse("""{"a", 1}""")).isEqualTo("""{"a", 1}""")
    assertThat(JsonValueParser.parse("[1, 2")).isEqualTo("[1, 2")
  }

  @Test
  def does_not_accept_non_standard_json(): Unit = {
    Seq("abc#123", "foo // comment", "NaN", "Infinity", "{a: 1}", "[1,,2]", """{"a"=1}""", "1 2", "01")
      .foreach { raw =>
        assertThat(JsonValueParser.parse(raw)).isEqualTo(raw)
        assertThat(JsonValueParser.parsePrimitive(raw)).isEqualTo(raw)
      }
  }

  @Test
  def parses_single_and_double_quoted_containers(): Unit = {
    val expectedList = new java.util.ArrayList[Any]()
    expectedList.add("a")
    expectedList.add(1)
    assertThat(JsonValueParser.parse("['a', 1]")).isEqualTo(expectedList)
    assertThat(JsonValueParser.parse("""["a", 1]""")).isEqualTo(expectedList)

    val expectedMap = new java.util.LinkedHashMap[String, Any]()
    expectedMap.put("key", "value")
    expectedMap.put("nested", java.util.Map.of("n", 1))
    assertThat(JsonValueParser.parse("{'key': 'value', 'nested': {\"n\": 1}}")).isEqualTo(expectedMap)
  }

  @Test
  def handles_quotes_and_escapes_inside_strings(): Unit = {
    assertThat(JsonValueParser.parse("""'it\'s'""")).isEqualTo("it's")
    assertThat(JsonValueParser.parse("""'say "hi"'""")).isEqualTo("say \"hi\"")
    assertThat(JsonValueParser.parse("""'say \"hi\"'""")).isEqualTo("say \"hi\"")
    assertThat(JsonValueParser.parse(""""it's"""")).isEqualTo("it's")
    assertThat(JsonValueParser.parse("""'a\\'""")).isEqualTo("a\\")
    assertThat(JsonValueParser.parse("""'line\nbreak'""")).isEqualTo("line\nbreak")
  }

  @Test
  def primitive_parsing_keeps_containers_as_compact_json(): Unit = {
    assertThat(JsonValueParser.parsePrimitive("1")).isEqualTo(Integer.valueOf(1))
    assertThat(JsonValueParser.parsePrimitive("\"foo\"")).isEqualTo("foo")
    assertThat(JsonValueParser.parsePrimitive("""[1, "a", 2.50]""")).isEqualTo("""[1,"a",2.50]""")
    assertThat(JsonValueParser.parsePrimitive("""{ "a" : { "b" : true } }""")).isEqualTo("""{"a":{"b":true}}""")
  }

  @Test
  def primitive_parsing_does_not_accept_single_quotes(): Unit = {
    assertThat(JsonValueParser.parsePrimitive("['a', 1]")).isEqualTo("['a', 1]")
    assertThat(JsonValueParser.parsePrimitive("'foo'")).isEqualTo("'foo'")
  }
}
