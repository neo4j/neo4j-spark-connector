package org.neo4j.cypher

import org.junit.Test

import org.junit.Assert._
import org.neo4j.spark.cypher.CypherHelpers._

class CypherHelpersTest {


  @Test def `should quote the string` {
    // given
    val value = "Test with space"

    // when
    val actual = value.quote

    // then
    assertEquals(s"`$value`", actual)
  }


  @Test def `should not quote the string` {
    // given
    val value = "Test"

    // when
    val actual = value.quote

    // then
    assertEquals(value, actual)
  }
}
