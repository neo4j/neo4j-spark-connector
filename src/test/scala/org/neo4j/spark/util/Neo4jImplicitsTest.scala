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

import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.aggregate.Sum
import org.apache.spark.sql.sources.And
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.neo4j.spark.util.MapConverter.toScala
import org.neo4j.spark.util.Neo4jImplicits._

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class Neo4jImplicitsTest {

  @Nested
  class Sanitizes {

    @Test
    def quotes_the_string(): Unit = {
      val value = "Test with space"

      val actual = value.sanitizeSchemaName()

      assertThat(actual).isEqualTo(s"`$value`")
    }

    @Test
    def quotes_text_that_starts_with_$(): Unit = {
      val value = "$string"

      val actual = value.sanitizeSchemaName()

      assertThat(actual).isEqualTo("`$string`")
    }

    @Test
    def quotes_text_that_starts_with_$_even_when_quoted(): Unit = {
      val value = "`$string`"

      val actual = value.sanitizeSchemaName()

      assertThat(actual).isEqualTo("`$string`")
    }

    @Test
    def sanitizes_text_that_has_backtick(): Unit = {
      val value = "User can put back`ticks"

      val actual = value.sanitizeSchemaName()

      assertThat(actual).isEqualTo(s"`User can put back``ticks`")
    }

    @Test
    def sanitizes_text_that_has_backtick_and_no_spaces(): Unit = {
      val value = "Per`son"

      val actual = value.sanitizeSchemaName()

      assertThat(actual).isEqualTo(s"`Per``son`")
    }

    @Test
    def does_not_requote_the_string(): Unit = {
      val value = "`Test with space`"

      val actual = value.sanitizeSchemaName()

      assertThat(actual).isEqualTo(value)
    }

    @Test
    def does_not_requote_the_string_even_when_sanitized(): Unit = {
      val value = "`Test wi`th space`"

      val actual = value.sanitizeSchemaName()

      assertThat(actual).isEqualTo("`Test wi``th space`")
    }

    @Test
    def does_not_quote_the_string(): Unit = {
      val value = "Test"

      val actual = value.sanitizeSchemaName()

      assertThat(actual).isEqualTo(value)
    }
  }

  @Nested
  class Attributes {

    @Test
    def returns_attribute_if_filter_has_it(): Unit = {
      val filter = EqualTo("name", "John")

      val attribute = filter.getAttribute

      assertTrue(attribute.isDefined)
    }

    @Test
    def returns_an_empty_option_if_the_filter_does_not_have_an_attribute(): Unit = {
      val filter = And(EqualTo("name", "John"), EqualTo("age", 32))

      val attribute = filter.getAttribute

      assertFalse(attribute.isDefined)
    }

    @Test
    def detects_the_entity_alias(): Unit = {
      assertTrue(EqualTo("source.name", "John").hasEntityAlias("source"))
      assertTrue(EqualTo("`source.name`", "John").hasEntityAlias("source"))
      assertTrue(EqualTo("`source.table.key`", "John").hasEntityAlias("source"))
      assertTrue(EqualTo("`rel.since`", 32).hasEntityAlias("rel"))
    }

    @Test
    def does_not_detect_an_entity_alias_when_it_is_only_part_of_the_property_name(): Unit = {
      assertFalse(EqualTo("`mysource.name`", "John").hasEntityAlias("source"))
      assertFalse(EqualTo("`name.source.x`", "John").hasEntityAlias("source"))
      assertFalse(EqualTo("source", "John").hasEntityAlias("source"))
    }
  }

  @Nested
  class PropertyPaths {

    @Test
    def splits_a_nested_property_access(): Unit = {
      assertThat("location.x".propertyPath().asJava).containsExactly("location", "x")
    }

    @Test
    def keeps_a_quoted_property_name_as_a_single_segment(): Unit = {
      assertThat("`table.key`".propertyPath().asJava).containsExactly("table.key")
      assertThat("`a.b.c`".propertyPath().asJava).containsExactly("a.b.c")
    }

    @Test
    def splits_a_nested_access_on_a_quoted_property_name(): Unit = {
      assertThat("`table.key`.x".propertyPath().asJava).containsExactly("table.key", "x")
    }

    @Test
    def removes_the_entity_alias_keeping_the_dots_of_the_property_name(): Unit = {
      assertThat("`source.table.key`".removeEntityAlias("source")).isEqualTo("table.key")
      assertThat("`source.a.b.c`".removeEntityAlias("source")).isEqualTo("a.b.c")
      assertThat("source.name".removeEntityAlias("source")).isEqualTo("name")
      assertThat("`source.location`.x".removeEntityAlias("source")).isEqualTo("location.x")
    }

    @Test
    def leaves_the_property_untouched_when_it_is_not_prefixed_with_the_alias(): Unit = {
      assertThat("`table.key`".removeEntityAlias("source")).isEqualTo("table.key")
      assertThat("`mysource.key`".removeEntityAlias("source")).isEqualTo("mysource.key")
    }
  }

  @Nested
  class MissingFields {

    @Test
    def struct_returns_true_if_contains_fields(): Unit = {
      val struct = StructType(Seq(
        StructField("is_hero", DataTypes.BooleanType),
        StructField("name", DataTypes.StringType),
        StructField("fi``(╯°□°)╯︵ ┻━┻eld", DataTypes.StringType)
      ))

      assertThat(struct.getMissingFields(Set("is_hero", "name", "fi``(╯°□°)╯︵ ┻━┻eld")).asJava)
        .isEmpty()
    }

    @Test
    def struct_returns_false_if_not_contains_fields(): Unit = {
      val struct =
        StructType(Seq(StructField("is_hero", DataTypes.BooleanType), StructField("name", DataTypes.StringType)))

      val result = struct.getMissingFields(Set("is_hero", "hero_name"))

      assertThat(result).isEqualTo(Set[String]("hero_name"))
    }

    @Test
    def missing_fields_include_maps(): Unit = {
      val struct = StructType(Seq(
        StructField("im", DataTypes.StringType),
        StructField("im.a", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
        StructField("im.also.a", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
        StructField("im.not.a.map", DataTypes.StringType),
        StructField("fi``(╯°□°)╯︵ ┻━┻eld", DataTypes.StringType)
      ))

      val result = struct.getMissingFields(Set(
        "im.aMap",
        "`im.also.a`.field",
        "`im.a`.map",
        "`im.not.a.map`",
        "fi``(╯°□°)╯︵ ┻━┻eld"
      ))

      assertThat(result).isEqualTo(Set("im.aMap"))
    }
  }

  @Nested
  class Aggregations {

    @Test
    def groupByCols_aggregation_works(): Unit = {
      val aggField = new NamedReference {
        override def fieldNames(): Array[String] = Array("foo")
        override def describe(): String = "foo"
      }
      val gbyField = new NamedReference {
        override def fieldNames(): Array[String] = Array("bar")
        override def describe(): String = "bar"
      }
      val aggregation = new Aggregation(Array(new Sum(aggField, false)), Array(gbyField))

      val columnAggregation = aggregation.groupByCols()

      assertThat(columnAggregation).hasSize(1)
      assertThat(columnAggregation(0).describe()).isEqualTo("bar")
    }
  }

  @Nested
  class Flattening {

    @Test
    def flattens_the_map(): Unit = {
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

      assertThat(actual).isEqualTo(expected)
    }

    @Test
    def map_flattening_does_not_handle_collision(): Unit = {
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

      assertThat(actual).isEqualTo(expected)
    }

    @Test
    def handles_collision_by_aggregating_values(): Unit = {
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

      assertThat(actual).isEqualTo(expected)
    }

    @Test
    def shows_duplicate_keys(): Unit = {
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

      assertThat(actual).isEqualTo(expected)
    }
  }

  @Nested
  class MapConversions {

    @Test
    def deserializes_dotted_and_stringified_map_into_a_nested_Java_map(): Unit = {
      val map = Map(
        "graphName" -> "foo",
        "configuration.number" -> "1",
        "configuration.string" -> "foo",
        "configuration.list" -> "['a', 1]",
        "configuration.map.key" -> "value",
        "relationshipProjection.LINK.properties.foobar.defaultValue" -> "42.0"
      )

      val result = map.toNestedDeserializedJavaMap

      assertThat(toScala(result)).isEqualTo(Map(
        "graphName" -> "foo",
        "configuration" -> Map(
          "number" -> 1,
          "string" -> "foo",
          "list" -> List("a", 1),
          "map" -> Map(
            "key" -> "value"
          )
        ),
        "relationshipProjection" -> Map(
          "LINK" -> Map(
            "properties" -> Map(
              "foobar" -> Map("defaultValue" -> 42.0)
            )
          )
        )
      ))
    }

    @Test
    def deserializes_dotted_map_into_a_nested_primitive_Java_map(): Unit = {
      val map = Map(
        "graphName" -> "foo",
        "configuration.number" -> "1",
        "configuration.string" -> "foo",
        "configuration.list" -> "['a', 1]", // treated as string
        "configuration.map.key" -> "value",
        "configuration.another_map" -> """{"a", 1}""", // treated as string
        "relationshipProjection.LINK.properties.foobar.defaultValue" -> "42.0"
      )

      val result = map.toNestedPrimitiveDeserializedJsonJavaMap

      assertThat(toScala(result)).isEqualTo(Map(
        "graphName" -> "foo",
        "configuration" -> Map(
          "number" -> 1,
          "string" -> "foo",
          "list" -> "['a', 1]",
          "map" -> Map(
            "key" -> "value"
          ),
          "another_map" -> """{"a", 1}"""
        ),
        "relationshipProjection" -> Map(
          "LINK" -> Map(
            "properties" -> Map(
              "foobar" -> Map("defaultValue" -> 42.0)
            )
          )
        )
      ))
    }

  }
}
