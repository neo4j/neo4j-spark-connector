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

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.connector.expressions.Literal
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.neo4j.cypherdsl.support.schema_name.SchemaNames
import org.neo4j.driver.Value
import org.neo4j.driver.types.Entity
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Relationship
import org.neo4j.spark.converter.CypherToSparkTypeConverter
import org.neo4j.spark.converter.SparkToNeo4jDataConverter
import org.neo4j.spark.service.SchemaService

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

object Neo4jImplicits {

  /**
   * Matches one segment of a property path: either a backtick quoted name,
   * which can contain dots, or a run of characters up to the next dot.
   */
  private val PATH_SEGMENT = """`([^`]*)`|([^.`]+)""".r

  private val BACKTICK = "`"

  implicit class CypherImplicits(str: String) {

    def sanitizeSchemaName(): String = {
      if (str.startsWith(BACKTICK) && str.endsWith(BACKTICK)) {
        SchemaNames.sanitize(str.substring(1, str.length - 1)).orElse("")
      } else {
        SchemaNames.sanitize(str).orElse("")
      }
    }

    def unquote(): String = str.replaceAll(BACKTICK, "")

    /**
     * Splits a, possibly backtick quoted, property path into its segments.
     *
     * Dots inside backticks belong to the property name, so `` `table.key` `` is a single property
     * named `table.key`, while `location.x` is the `x` field of the `location` property.
     */
    def propertyPath(): Seq[String] = {
      val segments = PATH_SEGMENT.findAllMatchIn(str)
        .map(m => Option(m.group(1)).getOrElse(m.group(2)))
        .toSeq

      if (segments.isEmpty) Seq(str) else segments
    }

    /**
     * Removes the given entity alias (`source`, `target` or `rel`) from the head of the property
     * path, leaving the rest of the path untouched.
     *
     * Only the alias is removed, not everything up to the first dot: `source.table.key` is the
     * `table.key` property of the `source` node, not the `key` field of its `table` property.
     */
    def propertyPathWithoutAlias(alias: String): Seq[String] = {
      val path = str.propertyPath()
      val prefix = s"$alias."

      path.head match {
        case head if head.startsWith(prefix) => head.substring(prefix.length) +: path.tail
        case `alias` if path.size > 1        => path.tail
        case _                               => path
      }
    }

    def hasEntityAlias(alias: String): Boolean = {
      val path = str.propertyPath()

      path.head.startsWith(s"$alias.") || (path.head == alias && path.size > 1)
    }

    /**
     * Same as [[propertyPathWithoutAlias]], for the callers that need the path back as a string.
     */
    def removeEntityAlias(alias: String): String = str.propertyPathWithoutAlias(alias).mkString(".")

    /**
     * df: we need this to handle scenarios like `WHERE age > 19 and age < 22`,
     * so we can't basically add a parameter named \$age.
     * So we base64 encode the value to ensure a unique parameter name
     */
    def toParameterName(value: Any): String = {
      val attributeValue = if (value == null) {
        "NULL"
      } else {
        value.toString
      }

      val base64ed = java.util.Base64.getEncoder.encodeToString(attributeValue.getBytes())

      s"${base64ed}_${str.unquote()}".sanitizeSchemaName()
    }
  }

  implicit class EntityImplicits(entity: Entity) {

    def toStruct(options: Neo4jOptions): StructType = {
      val fields = entity.asMap().asScala
        .groupBy(_._1)
        .map(t => {
          val value = t._2.head._2
          val cypherType = SchemaService.normalizedClassNameFromGraphEntity(value, options)
          StructField(t._1, CypherToSparkTypeConverter(options).convert(cypherType))
        })
      val entityFields = entity match {
        case _: Node => {
          Seq(
            StructField(Neo4jUtil.INTERNAL_ID_FIELD, DataTypes.StringType, nullable = false),
            StructField(
              Neo4jUtil.INTERNAL_LABELS_FIELD,
              DataTypes.createArrayType(DataTypes.StringType),
              nullable = true
            )
          )
        }
        case _: Relationship => {
          Seq(
            StructField(Neo4jUtil.INTERNAL_REL_ID_FIELD, DataTypes.StringType, nullable = false),
            StructField(Neo4jUtil.INTERNAL_REL_TYPE_FIELD, DataTypes.StringType, nullable = false),
            StructField(Neo4jUtil.INTERNAL_REL_SOURCE_ID_FIELD, DataTypes.StringType, nullable = false),
            StructField(Neo4jUtil.INTERNAL_REL_TARGET_ID_FIELD, DataTypes.StringType, nullable = false)
          )
        }
      }

      StructType(entityFields ++ fields)
    }

    def toMap: java.util.Map[String, Any] = {
      val entityMap = entity.asMap().asScala
      val entityFields = entity match {
        case node: Node =>
          Map(Neo4jUtil.INTERNAL_ID_FIELD -> node.elementId(), Neo4jUtil.INTERNAL_LABELS_FIELD -> node.labels())
        case relationship: Relationship =>
          Map[String, Any](
            Neo4jUtil.INTERNAL_REL_ID_FIELD -> relationship.elementId(),
            Neo4jUtil.INTERNAL_REL_TYPE_FIELD -> relationship.`type`(),
            Neo4jUtil.INTERNAL_REL_SOURCE_ID_FIELD -> relationship.startNodeElementId(),
            Neo4jUtil.INTERNAL_REL_TARGET_ID_FIELD -> relationship.endNodeElementId()
          )
      }
      (entityFields ++ entityMap).asJava
    }
  }

  implicit class PredicateImplicit(predicate: Predicate) {

    def toFilter(options: Neo4jOptions): Option[Filter] = {
      predicate.name() match {
        case "IS_NULL"     => Some(IsNull(predicate.rawAttributeName()))
        case "IS_NOT_NULL" => Some(IsNotNull(predicate.rawAttributeName()))
        case "STARTS_WITH" =>
          predicate.rawLiteralValue(options).map(lit => StringStartsWith(predicate.rawAttributeName(), lit.asString()))
        case "ENDS_WITH" =>
          predicate.rawLiteralValue(options).map(lit => StringEndsWith(predicate.rawAttributeName(), lit.asString()))
        case "CONTAINS" =>
          predicate.rawLiteralValue(options).map(lit => StringContains(predicate.rawAttributeName(), lit.asString()))
        case "IN" => Some(In(predicate.rawAttributeName(), predicate.rawLiteralValues(options)))
        case "=" => predicate.rawLiteralValue(options).map(lit => EqualTo(predicate.rawAttributeName(), lit.asObject()))
        case "<>" =>
          predicate.rawLiteralValue(options).map(lit => Not(EqualTo(predicate.rawAttributeName(), lit.asObject())))
        case "<=>" =>
          predicate.rawLiteralValue(options).map(lit => EqualNullSafe(predicate.rawAttributeName(), lit.asObject()))
        case "<" =>
          predicate.rawLiteralValue(options).map(lit => LessThan(predicate.rawAttributeName(), lit.asObject()))
        case "<=" =>
          predicate.rawLiteralValue(options).map(lit => LessThanOrEqual(predicate.rawAttributeName(), lit.asObject()))
        case ">" =>
          predicate.rawLiteralValue(options).map(lit => GreaterThan(predicate.rawAttributeName(), lit.asObject()))
        case ">=" =>
          predicate.rawLiteralValue(options).map(lit =>
            GreaterThanOrEqual(predicate.rawAttributeName(), lit.asObject())
          )
        case "AND" =>
          val andPredicate = predicate.asInstanceOf[filter.And]
          (andPredicate.left().toFilter(options), andPredicate.right().toFilter(options)) match {
            case (_, None)                 => None
            case (None, _)                 => None
            case (Some(left), Some(right)) => Some(And(left, right))
          }
        case "OR" =>
          val andPredicate = predicate.asInstanceOf[filter.Or]
          (andPredicate.left().toFilter(options), andPredicate.right().toFilter(options)) match {
            case (_, None)                 => None
            case (None, _)                 => None
            case (Some(left), Some(right)) => Some(Or(left, right))
          }
        case "NOT" =>
          val notPredicate = predicate.asInstanceOf[filter.Not]
          notPredicate.child().toFilter(options).map(Not)
        case "ALWAYS_TRUE"  => Some(AlwaysTrue)
        case "ALWAYS_FALSE" => Some(AlwaysFalse)
      }
    }

    /**
     * Spark keeps the field names of a reference separate, so it already knows whether `a.b` is a
     * property literally named `a.b` or the `b` field of the `a` property. Quoting each field name
     * preserves that information once the reference is flattened into a single string.
     */
    def rawAttributeName(): String = {
      predicate.references().head.fieldNames().map(_.quote()).mkString(".")
    }

    def rawLiteralValue(options: Neo4jOptions): Option[Value] = {
      predicate.children()
        .filter(_.isInstanceOf[Literal[_]])
        .map(_.asInstanceOf[Literal[_]])
        .headOption
        .map(literal => SparkToNeo4jDataConverter(options).convert(literal.value(), literal.dataType()))
    }

    def rawLiteralValues(options: Neo4jOptions): Array[Any] = {
      predicate.children()
        .filter(_.isInstanceOf[Literal[_]])
        .map(_.asInstanceOf[Literal[_]])
        .map(v => SparkToNeo4jDataConverter(options).convert(v.value(), v.dataType()).asObject())
    }
  }

  implicit class FilterImplicit(filter: Filter) {

    def flattenFilters: Array[Filter] = {
      filter match {
        case or: Or    => Array(or.left.flattenFilters, or.right.flattenFilters).flatten
        case and: And  => Array(and.left.flattenFilters, and.right.flattenFilters).flatten
        case f: Filter => Array(f)
      }
    }

    def getAttribute: Option[String] = Option(filter match {
      case eqns: EqualNullSafe         => eqns.attribute
      case eq: EqualTo                 => eq.attribute
      case gt: GreaterThan             => gt.attribute
      case gte: GreaterThanOrEqual     => gte.attribute
      case lt: LessThan                => lt.attribute
      case lte: LessThanOrEqual        => lte.attribute
      case in: In                      => in.attribute
      case notNull: IsNotNull          => notNull.attribute
      case isNull: IsNull              => isNull.attribute
      case startWith: StringStartsWith => startWith.attribute
      case endsWith: StringEndsWith    => endsWith.attribute
      case contains: StringContains    => contains.attribute
      case not: Not                    => not.child.getAttribute.orNull
      case _                           => null
    })

    def getValue: Option[Any] = Option(filter match {
      case eqns: EqualNullSafe         => eqns.value
      case eq: EqualTo                 => eq.value
      case gt: GreaterThan             => gt.value
      case gte: GreaterThanOrEqual     => gte.value
      case lt: LessThan                => lt.value
      case lte: LessThanOrEqual        => lte.value
      case in: In                      => in.values
      case startWith: StringStartsWith => startWith.value
      case endsWith: StringEndsWith    => endsWith.value
      case contains: StringContains    => contains.value
      case not: Not                    => not.child.getValue.orNull
      case _                           => null
    })

    def hasEntityAlias(alias: String): Boolean = getAttribute.exists(_.hasEntityAlias(alias))

    /**
     * df: we are not handling AND/OR because they are not actually filters
     * and have a different internal structure. Before calling this function on the filters
     * it's highly suggested FilterImplicit::flattenFilter() which returns a collection
     * of filters, including the one contained in the ANDs/ORs objects.
     */
    def getAttributeAndValue: Seq[Any] = {
      filter match {
        case f: EqualNullSafe      => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: EqualTo            => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: GreaterThan        => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: GreaterThanOrEqual => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: LessThan           => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: LessThanOrEqual    => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: In                 => Seq(f.attribute.toParameterName(f.values), f.values)
        case f: StringStartsWith   => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: StringEndsWith     => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: StringContains     => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: Not                => f.child.getAttributeAndValue
        case _                     => Seq()
      }
    }
  }

  implicit class StructTypeImplicit(structType: StructType) {

    private def isValidMapOrStructField(field: String, structFieldName: String) = {
      val value: String = """(`.*`)|([^\.]*)""".r.findFirstIn(field).getOrElse("")
      structFieldName == value.unquote() || structFieldName == value
    }

    def getByName(name: String): Option[StructField] = {
      val index = structType.fieldIndex(name)
      if (index > -1) Some(structType(index)) else None
    }

    def getFieldIndex(fieldName: String): Long = structType.fields.map(_.name).indexOf(fieldName)

    def getMissingFields(fields: Set[String]): Set[String] = fields
      .map(field => {
        val maybeField = structType
          .find(structField => {
            structField.dataType match {
              case _: MapType    => isValidMapOrStructField(field, structField.name)
              case _: StructType => isValidMapOrStructField(field, structField.name)
              case _             => structField.name == field.unquote() || structField.name == field
            }
          })
        field -> maybeField.isDefined
      })
      .filterNot(e => e._2)
      .map(e => e._1)
  }

  implicit class AggregationImplicit(aggregation: Aggregation) {
    def groupByCols(): Array[Expression] = ReflectionUtils.groupByCols(aggregation)
  }

  implicit class MapImplicit[K, V](map: Map[K, V]) {

    private def innerFlattenMap(map: Map[_, _], prefix: String): Seq[(String, AnyRef)] = map
      .toSeq
      .flatMap(t => {
        val key: String = if (prefix != "") s"$prefix.${t._1}" else t._1.toString
        t._2 match {
          case nestedMap: Map[_, _]           => innerFlattenMap(nestedMap, key)
          case nestedMap: java.util.Map[_, _] => innerFlattenMap(nestedMap.asScala.toMap, key)
          case _                              => Seq((key, t._2.asInstanceOf[AnyRef]))
        }
      })
      .toList

    def flattenMap(prefix: String = "", groupDuplicateKeys: Boolean = false): Map[String, AnyRef] =
      innerFlattenMap(map, prefix)
        .groupBy(_._1)
        .view
        .mapValues(seq => if (groupDuplicateKeys && seq.size > 1) seq.map(_._2).asJava else seq.last._2)
        .toMap

    def flattenKeys(prefix: String = ""): Seq[String] = map
      .flatMap((t: (K, V)) => {
        val key: String = if (prefix != "") s"$prefix.${t._1}" else t._1.toString
        t._2 match {
          case nestedMap: Map[_, _]           => nestedMap.flattenKeys(key)
          case nestedMap: java.util.Map[_, _] => nestedMap.asScala.toMap.flattenKeys(key)
          case _                              => Seq(key)
        }
      })
      .toList
  }

  implicit class StringMapImplicits(map: Map[String, String]) {

    private val propertyMapper = new ObjectMapper()
    propertyMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)

    private val primitivePropertyMapper = new ObjectMapper()

    primitivePropertyMapper.registerModule {
      val module = new SimpleModule()
      module.addDeserializer(classOf[AnyRef], new PrimitiveValueDeserializer)
      module
    }

    def toNestedDeserializedJavaMap: java.util.Map[String, Any] = nestAndDeserializeMap(map, propertyMapper)

    def toNestedPrimitiveDeserializedJsonJavaMap: java.util.Map[String, Any] =
      nestAndDeserializeMap(map, primitivePropertyMapper)

    private def nestAndDeserializeMap(data: Map[String, String], mapper: ObjectMapper): java.util.Map[String, Any] = {
      val result = new java.util.HashMap[String, Any]()
      data.foreach(keyValuePair => {
        val rawKey = keyValuePair._1
        val keyParts = rawKey.split("\\.")
        val rawValue = keyValuePair._2
        if (keyParts.size == 1) {
          result.put(rawKey, safeDeserializeValue(rawValue, mapper))
        } else {
          val firstKeyElement = keyParts.head
          val keyTail = keyParts.drop(1).mkString(".")
          val nestedValue = nestAndDeserializeMap(Map(keyTail -> rawValue), mapper)
          if (result.containsKey(firstKeyElement)) {
            val value = result.get(firstKeyElement).asInstanceOf[java.util.Map[String, Any]]
            value.putAll(nestedValue)
            result.put(firstKeyElement, value)
          } else {
            result.put(firstKeyElement, nestedValue)
          }
        }
      })
      result
    }

    private def safeDeserializeValue(rawValue: String, mapper: ObjectMapper) = {
      try {
        mapper.readValue[Any](rawValue, classOf[Any])
      } catch {
        case _: JsonParseException => rawValue
      }
    }
  }

  implicit class ValueImplicits(value: Value) {

    def asOptionalLong(): Option[Long] = {
      if (value.isNull) {
        Option.empty
      } else {
        Option(value.asLong())
      }
    }
  }
}
