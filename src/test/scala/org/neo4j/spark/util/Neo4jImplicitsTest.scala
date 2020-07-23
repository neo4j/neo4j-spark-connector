package org.neo4j.spark.util

import org.junit.Test

import org.junit.Assert._
import org.neo4j.spark.util.Neo4jImplicits._

class Neo4jImplicitsTest {

  @Test
  def `should quote the string` {
    // given
    val value = "Test with space"

    // when
    val actual = value.quote

    // then
    assertEquals(s"`$value`", actual)
  }

  @Test
  def `should not re-quote the string` {
    // given
    val value = "`Test with space`"

    // when
    val actual = value.quote

    // then
    assertEquals(value, actual)
  }

  @Test
  def `should not quote the string` {
    // given
    val value = "Test"

    // when
    val actual = value.quote

    // then
    assertEquals(value, actual)
  }


}
