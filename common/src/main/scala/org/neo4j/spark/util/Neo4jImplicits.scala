package org.neo4j.spark.util

import org.apache.spark.sql.connector.expressions.{Expression, GeneralScalarExpression, Literal, filter}
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.{And, Not, Or, Predicate}
import org.apache.spark.sql.types.{DataTypes, MapType, StructField, StructType}
import org.neo4j.driver.types.{Entity, Node, Relationship}
import org.neo4j.spark.service.SchemaService

import javax.lang.model.SourceVersion
import scala.collection.JavaConverters._

object Neo4jImplicits {

  implicit class CypherImplicits(str: String) {
    private def isValidCypherIdentifier() = SourceVersion.isIdentifier(str) && !str.trim.startsWith("$")

    def quote(): String = if (!isValidCypherIdentifier() && !str.isQuoted()) s"`$str`" else str

    def unquote(): String = str.replaceAll("`", "");

    def isQuoted(): Boolean = str.startsWith("`");

    def removeAlias(): String = {
      val splatString = str.split('.')

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
    def toStruct(): StructType = {
      val fields = entity.asMap().asScala
        .groupBy(_._1)
        .map(t => {
          val value = t._2.head._2
          val cypherType = SchemaService.normalizedClassNameFromGraphEntity(value)
          StructField(t._1, SchemaService.cypherToSparkType(cypherType))
        })
      val entityFields = entity match {
        case node: Node => {
          Seq(StructField(Neo4jUtil.INTERNAL_ID_FIELD, DataTypes.LongType, nullable = false),
            StructField(Neo4jUtil.INTERNAL_LABELS_FIELD, DataTypes.createArrayType(DataTypes.StringType), nullable = true))
        }
        case relationship: Relationship => {
          Seq(StructField(Neo4jUtil.INTERNAL_REL_ID_FIELD, DataTypes.LongType, false),
            StructField(Neo4jUtil.INTERNAL_REL_TYPE_FIELD, DataTypes.StringType, false),
            StructField(Neo4jUtil.INTERNAL_REL_SOURCE_ID_FIELD, DataTypes.LongType, false),
            StructField(Neo4jUtil.INTERNAL_REL_TARGET_ID_FIELD, DataTypes.LongType, false))
        }
      }

      StructType(entityFields ++ fields)
    }

    def toMap(): java.util.Map[String, Any] = {
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

  implicit class ExpressionImplicit(expression: Expression) {
    def flattenExpressions: Array[Expression] = {
      expression match {
        case or: Or => Array(or.left.flattenExpressions, or.right.flattenExpressions).flatten
        case and: And => Array(and.left.flattenExpressions, and.right.flattenExpressions).flatten
        case p: Predicate =>
          p.children()
      }
    }

    def getAttribute: Option[String] = expression.references().headOption.flatMap(_.fieldNames().headOption)

    def getValue: Option[Any] = {
      expression match {
        case scalar: GeneralScalarExpression => scalar.name() match {
          case "STARTS_WITH" | "ENDS_WITH" | "CONTAINS" | "=" | "<>" | "<=>" | "<" | "<=" | ">" | ">=" | "AND" | "OR" =>
            expression.references().tail.headOption.map(_.asInstanceOf[Literal[_]].value)
          case "IN" =>
            Some(expression.references().tail.map(_.asInstanceOf[Literal[_]].value))
          case _ => null
        }
        case _ => null
      }
    }

    def isAttribute(entityType: String): Boolean = {
      getAttribute.exists(_.contains(s"$entityType."))
    }

    def getAttributeWithoutEntityName: Option[String] = expression.getAttribute.map(_.unquote().split('.').tail.mkString("."))

    /**
     * df: we are not handling AND/OR because they are not actually filters
     * and have a different internal structure. Before calling this function on the filters
     * it's highly suggested FilterImplicit::flattenFilter() which returns a collection
     * of filters, including the one contained in the ANDs/ORs objects.
     */
    def getAttributeAndValue: Seq[Any] = {
      expression match {
        case scalar: GeneralScalarExpression =>
          (scalar, scalar.name()) match {
            case (_, "STARTS_WITH" | "ENDS_WITH" | "CONTAINS" | "IN" | "=" | "<>" | "<=>" | "<" | "<=" | ">" | ">=" | "AND" | "OR") =>
              val value = expression.getValue.get
              Seq(expression.getAttribute.get.toParameterName(value), value)
            case (not: Not, _) =>
              not.child().getAttributeAndValue
            case _ => Seq.empty
          }
        case _ => Seq.empty
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

  }
