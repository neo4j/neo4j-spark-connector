package org.neo4j.spark.util

import org.apache.spark.sql.sources.{And, EqualTo, Not}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
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
  def `should quote text that starts with $` {
    // given
    val value = "$tring"

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

  @Test
  def `struct should return true if contains fields`: Unit = {
    val struct = StructType(Seq(StructField("is_hero", DataTypes.BooleanType),
      StructField("name", DataTypes.StringType),
      StructField("fi``(╯°□°)╯︵ ┻━┻eld", DataTypes.StringType)))

    assertEquals(0, struct.getMissingFields(Set("is_hero", "name", "fi``(╯°□°)╯︵ ┻━┻eld")).size)
  }

  @Test
  def `struct should return false if not contains fields`: Unit = {
    val struct = StructType(Seq(StructField("is_hero", DataTypes.BooleanType), StructField("name", DataTypes.StringType)))

    assertEquals(Set[String]("hero_name"), struct.getMissingFields(Set("is_hero", "hero_name")))
  }

  @Test
  def `getMissingFields should handle maps`: Unit = {
    val struct = StructType(Seq(
      StructField("im", DataTypes.StringType),
      StructField("im.a", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
      StructField("im.also.a", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
      StructField("im.not.a.map", DataTypes.StringType),
      StructField("fi``(╯°□°)╯︵ ┻━┻eld", DataTypes.StringType)
    ))

    val result = struct.getMissingFields(Set("im.aMap", "`im.also.a`.field", "`im.a`.map", "`im.not.a.map`", "fi``(╯°□°)╯︵ ┻━┻eld"))

    assertEquals(Set("im.aMap"), result)
  }
}
