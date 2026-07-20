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
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.neo4j.spark.util.MapConverter.toScala
import org.neo4j.spark.util.Neo4jImplicits.AggregationImplicit
import org.neo4j.spark.util.Neo4jImplicits.CypherImplicits
import org.neo4j.spark.util.Neo4jImplicits.FilterImplicit
import org.neo4j.spark.util.Neo4jImplicits.MapImplicit
import org.neo4j.spark.util.Neo4jImplicits.StringMapImplicits
import org.neo4j.spark.util.Neo4jImplicits.StructTypeImplicit

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class Neo4jImplicitsTest {

  @Nested
  class Quotes {

    @Test
    def quotes_the_string(): Unit = {
      val value = "Test with space"

      val actual = value.quote()

      assertThat(actual).isEqualTo(s"`$value`")
    }

    @Test
    def quotes_text_that_starts_with_$(): Unit = {
      val value = "$string"

      val actual = value.quote()

      assertThat(actual).isEqualTo(s"`$value`")
    }

    @Test
    def does_not_requote_the_string(): Unit = {
      val value = "`Test with space`"

      val actual = value.quote()

      assertThat(actual).isEqualTo(value)
    }

    @Test
    def does_not_quote_the_string(): Unit = {
      val value = "Test"

      val actual = value.quote()

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
    def returns_the_attribute_without_the_entity_identifier(): Unit = {
      val filter = EqualTo("person.address.coords", 32)

      val attribute = filter.getAttributeWithoutEntityName

      assertThat(attribute.get).isEqualTo("address.coords")
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
