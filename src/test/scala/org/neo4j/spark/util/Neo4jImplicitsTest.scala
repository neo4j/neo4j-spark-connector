package org.neo4j.spark.util

import org.apache.spark.sql.sources.{And, EqualTo, Not}
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

  @Test
  def `should return attribute if filter has it` {
    // given
    val filter = EqualTo("name", "John")

    // when
    val attribute = filter.getAttribute

    // then
    assertTrue(attribute.isDefined)
  }

  @Test
  def `should return an empty option if the filter doesn't have an attribute` {
    // given
    val filter = And(EqualTo("name", "John"), EqualTo("age", 32))

    // when
    val attribute = filter.getAttribute

    // then
    assertFalse(attribute.isDefined)
  }

  @Test
  def `should return the attribute without the entity identifier` {
    // given
    val filter = EqualTo("person.address.coords", 32)

    // when
    val attribute = filter.getAttributeWithoutEntityName

    // then
    assertEquals("address.coords", attribute.get)
  }
}
