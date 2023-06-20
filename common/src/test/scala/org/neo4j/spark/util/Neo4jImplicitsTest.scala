package org.neo4j.spark.util

import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, Sum}
import org.apache.spark.sql.sources.{And, EqualTo}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.junit.Assert._
import org.junit.{Assert, Test}
import org.neo4j.spark.util.Neo4jImplicits._

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter, seqAsJavaListConverter}
import scala.collection.immutable.ListMap

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

  @Test
  def `groupByCols aggregation should work`: Unit = {
    val aggField = new NamedReference {
      override def fieldNames(): Array[String] = Array("foo")

      override def describe(): String = "foo"
    }
    val gbyField = new NamedReference {
      override def fieldNames(): Array[String] = Array("bar")

      override def describe(): String = "bar"
    }
    val agg = new Aggregation(Array(new Sum(aggField, false)), Array(gbyField))
    assertEquals(1, agg.groupByCols().length)
    assertEquals("bar", agg.groupByCols()(0).describe())
  }

  @Test
  def `should flatten the map`(): Unit = {
    val input = Map(
      "foo" -> "bar",
      "key" -> Map(
        "innerKey" -> Map("innerKey2" -> "value")
      )
    )
    val expected = Map(
      "foo" -> "bar",
      "key.innerKey.innerKey2" -> "value"
    )
    val actual = input.flattenMap()
    Assert.assertEquals(expected, actual)
  }

  @Test
  def `should not handle collision`(): Unit = {
    val input = ListMap(
      "my" -> Map(
        "inner" -> Map("key" -> 42424242),
        "inner.key" -> 424242
      ),
      "my.inner" -> Map("key" -> 4242).asJava,
      "my.inner.key" -> 42
    )
    val expected = Map(
      "my.inner.key" -> 42
    )
    val actual = input.flattenMap()
    Assert.assertEquals(expected, actual)
  }

  @Test
  def `should handle collision by aggregating values`(): Unit = {
    val input = ListMap(
      "my" -> Map(
        "inner" -> Map("key" -> 42424242),
        "inner.key" -> 424242
      ),
      "my.inner" -> Map("key" -> 4242).asJava,
      "my.inner.key" -> 42
    )
    val expected = Map(
      "my.inner.key" -> Seq(42424242, 424242, 4242, 42).asJava
    )
    val actual = input.flattenMap(groupDuplicateKeys = true)
    Assert.assertEquals(expected, actual)
  }

  @Test
  def `should show duplicate keys`(): Unit = {
    val input = Map(
      "my" -> Map(
        "inner" -> Map("key" -> 42424242),
        "inner.key" -> 424242
      ),
      "my.inner" -> Map("key" -> 4242).asJava,
      "my.inner.key" -> 42
    )
    val expected = Seq("my.inner.key", "my.inner.key", "my.inner.key", "my.inner.key")
    val actual = input.flattenKeys()
    Assert.assertEquals(expected, actual)
  }

  @Test
  def `should deserialized dotted/stringified map into a nested Java map`(): Unit = {
    val actual = Map(
      "graphName" -> "foo",
      "configuration.number" -> "1",
      "configuration.string" -> "foo",
      "configuration.list" -> "['a', 1]",
      "configuration.map.key" -> "value",
      "relationshipProjection.LINK.properties.foobar.defaultValue" -> "42.0"
    ).toNestedJavaMap
    val expected: java.util.Map[String, Object] = Map(
      "graphName" -> "foo",
      "configuration" -> Map(
        "number" -> 1,
        "string" -> "foo",
        "list" -> Seq("a", 1).toList.asJava,
        "map" -> Map(
          "key" -> "value"
        ).asJava
      ).asJava,
      "relationshipProjection" -> Map(
        "LINK" -> Map(
          "properties" -> Map(
            "foobar" -> Map("defaultValue" -> 42.0).asJava
          ).asJava
        ).asJava
      ).asJava
    ).asJava
    Assert.assertEquals(expected, actual)

    val ucActual = Map(
      "graphName" -> "myGraph",
      "nodeProjection" -> "Website",
      "relationshipProjection.LINK.indexInverse" -> "true",
    ).toNestedJavaMap
    val ucExpected: java.util.Map[String, Object] = Map(
      "graphName" -> "myGraph",
      "nodeProjection" -> "Website",
      "relationshipProjection" -> Map(
        "LINK" -> Map(
          "indexInverse" -> true
        ).asJava
      ).asJava
    ).asJava
    Assert.assertEquals(ucExpected, ucActual)
  }
}
