package org.neo4j.spark.util

import com.fasterxml.jackson.core.{JsonParseException, JsonParser}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.expressions.{Expression, Literal, filter}
import org.apache.spark.sql.sources.{AlwaysFalse, AlwaysTrue, And, EqualNullSafe, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith, StringStartsWith}
import org.apache.spark.sql.types.{DataTypes, MapType, StructField, StructType}
import org.neo4j.driver.Value
import org.neo4j.driver.types.{Entity, Node, Relationship}
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.Neo4jUtil.convertFromSpark

import javax.lang.model.SourceVersion
import scala.collection.JavaConverters._

object Neo4jImplicits {

  implicit class CypherImplicits(str: String) {
    private def isValidCypherIdentifier() = SourceVersion.isIdentifier(str) && !str.trim.startsWith("$")

    def quote(): String = if (!isValidCypherIdentifier() && !str.isQuoted()) s"`$str`" else str

    def unquote(): String = str.replaceAll("`", "");

    def isQuoted(): Boolean = str.startsWith("`");

    def removeAlias(): String = {
      val splatString = str.unquote().split('.')

      if (splatString.size > 1) {
        splatString.tail.mkString(".")
      }
      else {
        str
      }
    }

    /**
     * df: we need this to handle scenarios like `WHERE age > 19 and age < 22`,
     * so we can't basically add a parameter named \$age.
     * So we base64 encode the value to ensure a unique parameter name
     */
    def toParameterName(value: Any): String = {
      val attributeValue = if (value == null) {
        "NULL"
      }
      else {
        value.toString
      }

      val base64ed = java.util.Base64.getEncoder.encodeToString(attributeValue.getBytes())

      s"${base64ed}_${str.unquote()}".quote()
    }
  }

  implicit class EntityImplicits(entity: Entity) {
    def toStruct: StructType = {
      val fields = entity.asMap().asScala
        .groupBy(_._1)
        .map(t => {
          val value = t._2.head._2
          val cypherType = SchemaService.normalizedClassNameFromGraphEntity(value)
          StructField(t._1, SchemaService.cypherToSparkType(cypherType))
        })
      val entityFields = entity match {
        case _: Node => {
          Seq(StructField(Neo4jUtil.INTERNAL_ID_FIELD, DataTypes.LongType, nullable = false),
            StructField(Neo4jUtil.INTERNAL_LABELS_FIELD, DataTypes.createArrayType(DataTypes.StringType), nullable = true))
        }
        case _: Relationship => {
          Seq(StructField(Neo4jUtil.INTERNAL_REL_ID_FIELD, DataTypes.LongType, nullable = false),
            StructField(Neo4jUtil.INTERNAL_REL_TYPE_FIELD, DataTypes.StringType, nullable = false),
            StructField(Neo4jUtil.INTERNAL_REL_SOURCE_ID_FIELD, DataTypes.LongType, nullable = false),
            StructField(Neo4jUtil.INTERNAL_REL_TARGET_ID_FIELD, DataTypes.LongType, nullable = false))
        }
      }

      StructType(entityFields ++ fields)
    }

    def toMap: java.util.Map[String, Any] = {
      val entityMap = entity.asMap().asScala
      val entityFields = entity match {
        case node: Node => {
          Map(Neo4jUtil.INTERNAL_ID_FIELD -> node.id(),
            Neo4jUtil.INTERNAL_LABELS_FIELD -> node.labels())
        }
        case relationship: Relationship => {
          Map(Neo4jUtil.INTERNAL_REL_ID_FIELD -> relationship.id(),
            Neo4jUtil.INTERNAL_REL_TYPE_FIELD -> relationship.`type`(),
            Neo4jUtil.INTERNAL_REL_SOURCE_ID_FIELD -> relationship.startNodeId(),
            Neo4jUtil.INTERNAL_REL_TARGET_ID_FIELD -> relationship.endNodeId())
        }
      }
      (entityFields ++ entityMap).asJava
    }
  }

  implicit class PredicateImplicit(predicate: Predicate) {

    def toFilter: Option[Filter] = {
      predicate.name() match {
        case "IS_NULL" => Some(IsNull(predicate.rawAttributeName()))
        case "IS_NOT_NULL" => Some(IsNotNull(predicate.rawAttributeName()))
        case "STARTS_WITH" => predicate.rawLiteralValue().map(lit => StringStartsWith(predicate.rawAttributeName(), lit.asString()))
        case "ENDS_WITH" => predicate.rawLiteralValue().map(lit => StringEndsWith(predicate.rawAttributeName(), lit.asString()))
        case "CONTAINS" => predicate.rawLiteralValue().map(lit => StringContains(predicate.rawAttributeName(), lit.asString()))
        case "IN" => Some(In(predicate.rawAttributeName(), predicate.rawLiteralValues()))
        case "=" => predicate.rawLiteralValue().map(lit => EqualTo(predicate.rawAttributeName(), lit.asObject()))
        case "<>" => predicate.rawLiteralValue().map(lit => Not(EqualTo(predicate.rawAttributeName(), lit.asObject())))
        case "<=>" => predicate.rawLiteralValue().map(lit => EqualNullSafe(predicate.rawAttributeName(), lit.asObject()))
        case "<" => predicate.rawLiteralValue().map(lit => LessThan(predicate.rawAttributeName(), lit.asObject()))
        case "<=" => predicate.rawLiteralValue().map(lit => LessThanOrEqual(predicate.rawAttributeName(), lit.asObject()))
        case ">" => predicate.rawLiteralValue().map(lit => GreaterThan(predicate.rawAttributeName(), lit.asObject()))
        case ">=" => predicate.rawLiteralValue().map(lit => GreaterThanOrEqual(predicate.rawAttributeName(), lit.asObject()))
        case "AND" =>
          val andPredicate = predicate.asInstanceOf[filter.And]
          (andPredicate.left().toFilter, andPredicate.right().toFilter) match {
            case (_, None) => None
            case (None, _) => None
            case (Some(left), Some(right)) => Some(And(left, right))
          }
        case "OR" =>
          val andPredicate = predicate.asInstanceOf[filter.Or]
          (andPredicate.left().toFilter, andPredicate.right().toFilter) match {
            case (_, None) => None
            case (None, _) => None
            case (Some(left), Some(right)) => Some(Or(left, right))
          }
        case "NOT" =>
          val notPredicate = predicate.asInstanceOf[filter.Not]
          notPredicate.child().toFilter.map(Not)
        case "ALWAYS_TRUE" => Some(AlwaysTrue)
        case "ALWAYS_FALSE" => Some(AlwaysFalse)
      }
    }

    def rawAttributeName(): String = {
      predicate.references().head.fieldNames().mkString(".")
    }

    def rawLiteralValue(): Option[Value] = {
      predicate.children()
        .filter(_.isInstanceOf[Literal[_]])
        .map(_.asInstanceOf[Literal[_]])
        .headOption
        .map(literal => convertFromSpark(literal.value(), literal.dataType()))
    }

    def rawLiteralValues(): Array[Any] = {
      predicate.children()
        .filter(_.isInstanceOf[Literal[_]])
        .map(_.asInstanceOf[Literal[_]])
        .map(v => convertFromSpark(v.value(), v.dataType()).asObject())
    }
  }

  implicit class FilterImplicit(filter: Filter) {
    def flattenFilters: Array[Filter] = {
      filter match {
        case or: Or => Array(or.left.flattenFilters, or.right.flattenFilters).flatten
        case and: And => Array(and.left.flattenFilters, and.right.flattenFilters).flatten
        case f: Filter => Array(f)
      }
    }

    def getAttribute: Option[String] = Option(filter match {
      case eqns: EqualNullSafe => eqns.attribute
      case eq: EqualTo => eq.attribute
      case gt: GreaterThan => gt.attribute
      case gte: GreaterThanOrEqual => gte.attribute
      case lt: LessThan => lt.attribute
      case lte: LessThanOrEqual => lte.attribute
      case in: In => in.attribute
      case notNull: IsNotNull => notNull.attribute
      case isNull: IsNull => isNull.attribute
      case startWith: StringStartsWith => startWith.attribute
      case endsWith: StringEndsWith => endsWith.attribute
      case contains: StringContains => contains.attribute
      case not: Not => not.child.getAttribute.orNull
      case _ => null
    })

    def getValue: Option[Any] = Option(filter match {
      case eqns: EqualNullSafe => eqns.value
      case eq: EqualTo => eq.value
      case gt: GreaterThan => gt.value
      case gte: GreaterThanOrEqual => gte.value
      case lt: LessThan => lt.value
      case lte: LessThanOrEqual => lte.value
      case in: In => in.values
      case startWith: StringStartsWith => startWith.value
      case endsWith: StringEndsWith => endsWith.value
      case contains: StringContains => contains.value
      case not: Not => not.child.getValue.orNull
      case _ => null
    })

    def isAttribute(entityType: String): Boolean = {
      getAttribute.exists(_.contains(s"$entityType."))
    }

    def getAttributeWithoutEntityName: Option[String] = filter.getAttribute.map(_.unquote().split('.').tail.mkString("."))

    /**
     * df: we are not handling AND/OR because they are not actually filters
     * and have a different internal structure. Before calling this function on the filters
     * it's highly suggested FilterImplicit::flattenFilter() which returns a collection
     * of filters, including the one contained in the ANDs/ORs objects.
     */
    def getAttributeAndValue: Seq[Any] = {
      filter match {
        case f: EqualNullSafe => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: EqualTo => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: GreaterThan => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: GreaterThanOrEqual => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: LessThan => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: LessThanOrEqual => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: In => Seq(f.attribute.toParameterName(f.values), f.values)
        case f: StringStartsWith => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: StringEndsWith => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: StringContains => Seq(f.attribute.toParameterName(f.value), f.value)
        case f: Not => f.child.getAttributeAndValue
        case _ => Seq()
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
              case _: MapType => isValidMapOrStructField(field, structField.name)
              case _: StructType => isValidMapOrStructField(field, structField.name)
              case _ => structField.name == field.unquote() || structField.name == field
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

  implicit class MapImplicit[K, V](map: Map[String, V]) {

    private def innerFlattenMap(map: Map[String, _], prefix: String): Seq[(String, AnyRef)] = map
      .toSeq
      .flatMap(t => {
        val key: String = if (prefix != "") s"$prefix.${t._1}" else t._1
        t._2 match {
          case nestedMap: Map[String, _] => innerFlattenMap(nestedMap, key)
          case nestedMap: java.util.Map[String, _] => innerFlattenMap(nestedMap.asScala.toMap, key)
          case _ => Seq((key, t._2.asInstanceOf[AnyRef]))
        }
      })
      .toList

    def flattenMap(prefix: String = "", groupDuplicateKeys: Boolean = false): Map[String, AnyRef] = innerFlattenMap(map, prefix)
      .groupBy(_._1)
      .mapValues(seq => if (groupDuplicateKeys && seq.size > 1) seq.map(_._2).asJava else seq.last._2)
      .toMap

    def flattenKeys(prefix: String = ""): Seq[String] = map
      .flatMap(t => {
        val key: String = if (prefix != "") s"$prefix.${t._1}" else t._1
        t._2 match {
          case nestedMap: Map[String, _] => nestedMap.flattenKeys(key)
          case nestedMap: java.util.Map[String, _] => nestedMap.asScala.toMap.flattenKeys(key)
          case _ => Seq(key)
        }
      })
      .toList
  }


  implicit class StringMapImplicits(map: Map[String, String]) {

    private val propertyMapper = new ObjectMapper()
    propertyMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)


    private def nestingMap(data: Map[String, String]): java.util.Map[String, Any] = {
      val map = new java.util.HashMap[String, Any]();
      data.foreach(t => {
        val splitted = t._1.split("\\.")
        if (splitted.size == 1) {
          val value = try {
            propertyMapper.readValue[Any](t._2, classOf[Any])
          } catch {
            case _: JsonParseException => t._2
          }
          map.put(t._1, value)
        } else {
          if (map.containsKey(splitted.head)) {
            val value = map.get(splitted.head).asInstanceOf[java.util.Map[String, Any]]
            value.putAll(nestingMap(Map(splitted.drop(1).mkString(".") -> t._2)))
            map.put(splitted.head, value)
          } else {
            map.put(splitted.head, nestingMap(Map(splitted.drop(1).mkString(".") -> t._2)))
          }
        }
      })
      map
    }

    def toNestedJavaMap: java.util.Map[String, Any] = nestingMap(map)
  }

}
