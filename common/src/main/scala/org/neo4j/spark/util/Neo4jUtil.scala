package org.neo4j.spark.util

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{JsonSerializer, ObjectMapper, SerializerProvider}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.neo4j.cypherdsl.core.{Condition, Cypher, Expression, Functions, Property, PropertyContainer}
import org.neo4j.driver.exceptions.{Neo4jException, ServiceUnavailableException, SessionExpiredException, TransientException}
import org.neo4j.driver.internal._
import org.neo4j.driver.types.{Entity, Path}
import org.neo4j.driver.{Session, Transaction, Value, Values}
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.Neo4jImplicits.{EntityImplicits, _}
import org.slf4j.Logger

import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.Temporal
import java.util.{Optional, Properties}
import scala.collection.JavaConverters._

object Neo4jUtil {

  val NODE_ALIAS = "n"
  private val INTERNAL_ID_FIELD_NAME = "id"
  val INTERNAL_ID_FIELD = s"<${INTERNAL_ID_FIELD_NAME}>"
  private val INTERNAL_LABELS_FIELD_NAME = "labels"
  val INTERNAL_LABELS_FIELD = s"<${INTERNAL_LABELS_FIELD_NAME}>"
  val INTERNAL_REL_ID_FIELD = s"<rel.${INTERNAL_ID_FIELD_NAME}>"
  val INTERNAL_REL_TYPE_FIELD = "<rel.type>"
  val RELATIONSHIP_SOURCE_ALIAS = "source"
  val RELATIONSHIP_TARGET_ALIAS = "target"
  val INTERNAL_REL_SOURCE_ID_FIELD = s"<${RELATIONSHIP_SOURCE_ALIAS}.${INTERNAL_ID_FIELD_NAME}>"
  val INTERNAL_REL_TARGET_ID_FIELD = s"<${RELATIONSHIP_TARGET_ALIAS}.${INTERNAL_ID_FIELD_NAME}>"
  val INTERNAL_REL_SOURCE_LABELS_FIELD = s"<${RELATIONSHIP_SOURCE_ALIAS}.${INTERNAL_LABELS_FIELD_NAME}>"
  val INTERNAL_REL_TARGET_LABELS_FIELD = s"<${RELATIONSHIP_TARGET_ALIAS}.${INTERNAL_LABELS_FIELD_NAME}>"
  val RELATIONSHIP_ALIAS = "rel"

  private val properties = new Properties()
  properties.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("neo4j-spark-connector.properties"))

  def closeSafely(autoCloseable: AutoCloseable, logger: Logger = null): Unit = {
    try {
      autoCloseable match {
        case s: Session => if (s.isOpen) s.close()
        case t: Transaction => if (t.isOpen) t.close()
        case null => ()
        case _ => autoCloseable.close()
      }
    } catch {
      case t: Throwable => if (logger != null) logger
        .warn(s"Cannot close ${autoCloseable.getClass.getSimpleName} because of the following exception:", t)
    }
  }

  val mapper = new ObjectMapper()
  private val module = new SimpleModule("Neo4jApocSerializer")
  module.addSerializer(classOf[Path], new JsonSerializer[Path]() {
    override def serialize(path: Path,
                           jsonGenerator: JsonGenerator,
                           serializerProvider: SerializerProvider): Unit = jsonGenerator.writeString(path.toString)
  })
  module.addSerializer(classOf[Entity], new JsonSerializer[Entity]() {
    override def serialize(entity: Entity,
                           jsonGenerator: JsonGenerator,
                           serializerProvider: SerializerProvider): Unit = jsonGenerator.writeObject(entity.toMap)
  })
  module.addSerializer(classOf[Temporal], new JsonSerializer[Temporal]() {
    override def serialize(entity: Temporal,
                           jsonGenerator: JsonGenerator,
                           serializerProvider: SerializerProvider): Unit = jsonGenerator.writeRaw(entity.toString)
  })
  mapper.registerModule(module)

  def convertFromNeo4j(value: Any, schema: DataType = null): Any = {
    if (schema != null && schema == DataTypes.StringType && value != null && !value.isInstanceOf[String]) {
      convertFromNeo4j(mapper.writeValueAsString(value), schema)
    } else {
      value match {
        case node: InternalNode => {
          val map = node.asMap()
          val structType = extractStructType(schema)
          val fields = structType
            .filter(field => field.name != Neo4jUtil.INTERNAL_ID_FIELD && field.name != Neo4jUtil.INTERNAL_LABELS_FIELD)
            .map(field => convertFromNeo4j(map.get(field.name), field.dataType))
          InternalRow.fromSeq(Seq(convertFromNeo4j(node.id()), convertFromNeo4j(node.labels())) ++ fields)
        }
        case rel: InternalRelationship => {
          val map = rel.asMap()
          val structType = extractStructType(schema)
          val fields = structType
            .filter(field => field.name != Neo4jUtil.INTERNAL_REL_ID_FIELD
              && field.name != Neo4jUtil.INTERNAL_REL_TYPE_FIELD
              && field.name != INTERNAL_REL_SOURCE_ID_FIELD
              && field.name != INTERNAL_REL_TARGET_ID_FIELD)
            .map(field => convertFromNeo4j(map.get(field.name), field.dataType))
          InternalRow.fromSeq(Seq(convertFromNeo4j(rel.id()),
            convertFromNeo4j(rel.`type`()),
            convertFromNeo4j(rel.startNodeId()),
            convertFromNeo4j(rel.endNodeId())) ++ fields)
        }
        case d: InternalIsoDuration => {
          val months = d.months()
          val days = d.days()
          val nanoseconds: Integer = d.nanoseconds()
          val seconds = d.seconds()
          InternalRow.fromSeq(Seq(UTF8String.fromString(SchemaService.DURATION_TYPE), months, days, seconds, nanoseconds, UTF8String.fromString(d.toString)))
        }
        case zt: ZonedDateTime => DateTimeUtils.instantToMicros(zt.toInstant)
        case dt: LocalDateTime => DateTimeUtils.instantToMicros(dt.toInstant(ZoneOffset.UTC))
        case d: LocalDate => d.toEpochDay.toInt
        case lt: LocalTime => {
          InternalRow.fromSeq(Seq(
            UTF8String.fromString(SchemaService.TIME_TYPE_LOCAL),
            UTF8String.fromString(lt.format(DateTimeFormatter.ISO_TIME))
          ))
        }
        case t: OffsetTime => {
          InternalRow.fromSeq(Seq(
            UTF8String.fromString(SchemaService.TIME_TYPE_OFFSET),
            UTF8String.fromString(t.format(DateTimeFormatter.ISO_TIME))
          ))
        }
        case p: InternalPoint2D => {
          val srid: Integer = p.srid()
          InternalRow.fromSeq(Seq(UTF8String.fromString(SchemaService.POINT_TYPE_2D), srid, p.x(), p.y(), null))
        }
        case p: InternalPoint3D => {
          val srid: Integer = p.srid()
          InternalRow.fromSeq(Seq(UTF8String.fromString(SchemaService.POINT_TYPE_3D), srid, p.x(), p.y(), p.z()))
        }
        case l: java.util.List[_] => {
          val elementType = if (schema != null) schema.asInstanceOf[ArrayType].elementType else null
          ArrayData.toArrayData(l.asScala.map(e => convertFromNeo4j(e, elementType)).toArray)
        }
        case map: java.util.Map[_, _] => {
          if (schema != null) {
            val mapType = schema.asInstanceOf[MapType]
            ArrayBasedMapData(map.asScala.map(t => (convertFromNeo4j(t._1, mapType.keyType), convertFromNeo4j(t._2, mapType.valueType))))
          } else {
            ArrayBasedMapData(map.asScala.map(t => (convertFromNeo4j(t._1), convertFromNeo4j(t._2))))
          }
        }
        case s: String => UTF8String.fromString(s)
        case _ => value
      }
    }
  }

  private def extractStructType(dataType: DataType): StructType = dataType match {
    case structType: StructType => structType
    case mapType: MapType => extractStructType(mapType.valueType)
    case arrayType: ArrayType => extractStructType(arrayType.elementType)
    case _ => throw new UnsupportedOperationException(s"$dataType not supported")
  }

  def convertFromSpark(value: Any, dataType: DataType = null): Value = value match {
    case date: java.sql.Date => convertFromSpark(date.toLocalDate, dataType)
    case timestamp: java.sql.Timestamp => convertFromSpark(timestamp.toLocalDateTime, dataType)
    case intValue: Int if dataType == DataTypes.DateType => convertFromSpark(DateTimeUtils
      .toJavaDate(intValue), dataType)
    case longValue: Long if dataType == DataTypes.TimestampType => convertFromSpark(DateTimeUtils
      .toJavaTimestamp(longValue), dataType)
    case unsafeRow: UnsafeRow => {
      val structType = extractStructType(dataType)
      val row = new GenericRowWithSchema(unsafeRow.toSeq(structType).toArray, structType)
      convertFromSpark(row)
    }
    case struct: GenericRowWithSchema => {
      def toMap(struct: GenericRowWithSchema): Value = {
        Values.value(
          struct.schema.fields.map(
            f => f.name -> Neo4jUtil.convertFromSpark(struct.getAs(f.name), f.dataType)
          ).toMap.asJava)
      }

      try {
        struct.getAs[UTF8String]("type").toString match {
          case SchemaService.POINT_TYPE_2D => Values.point(struct.getAs[Number]("srid").intValue(),
            struct.getAs[Number]("x").doubleValue(),
            struct.getAs[Number]("y").doubleValue())
          case SchemaService.POINT_TYPE_3D => Values.point(struct.getAs[Number]("srid").intValue(),
            struct.getAs[Number]("x").doubleValue(),
            struct.getAs[Number]("y").doubleValue(),
            struct.getAs[Number]("z").doubleValue())
          case SchemaService.DURATION_TYPE => Values.isoDuration(struct.getAs[Number]("months").longValue(),
            struct.getAs[Number]("days").longValue(),
            struct.getAs[Number]("seconds").longValue(),
            struct.getAs[Number]("nanoseconds").intValue())
          case SchemaService.TIME_TYPE_OFFSET => Values.value(OffsetTime.parse(struct.getAs[UTF8String]("value").toString))
          case SchemaService.TIME_TYPE_LOCAL => Values.value(LocalTime.parse(struct.getAs[UTF8String]("value").toString))
          case _ => toMap(struct)
        }
      } catch {
        case _: Throwable => toMap(struct)
      }
    }
    case unsafeArray: UnsafeArrayData => {
      val sparkType = dataType match {
        case arrayType: ArrayType => arrayType.elementType
        case _ => dataType
      }
      val javaList = unsafeArray.toSeq[AnyRef](sparkType)
        .map(elem => convertFromSpark(elem, sparkType))
        .asJava
      Values.value(javaList)
    }
    case unsafeMapData: UnsafeMapData => { // Neo4j only supports Map[String, AnyRef]
      val mapType = dataType.asInstanceOf[MapType]
      val map: Map[String, AnyRef] = (0 until unsafeMapData.numElements())
        .map(i => (unsafeMapData.keyArray().getUTF8String(i).toString, unsafeMapData.valueArray().get(i, mapType.valueType)))
        .toMap[String, AnyRef]
        .mapValues(innerValue => convertFromSpark(innerValue, mapType.valueType))
        .toMap[String, AnyRef]
      Values.value(map.asJava)
    }
    case string: UTF8String => convertFromSpark(string.toString)
    case _ => Values.value(value)
  }

  def isLong(str: String): Boolean = {
    if (str == null) {
      false
    } else {
      try {
        str.trim.toLong
        true
      } catch {
        case nfe: NumberFormatException => false
        case t: Throwable => throw t
      }
    }
  }

  def connectorVersion: String = properties.getOrDefault("version", "UNKNOWN").toString

  def connectorEnv: String = Option(System.getenv("DATABRICKS_RUNTIME_VERSION"))
    .map(_ => "databricks")
    .getOrElse("spark")

  def getCorrectProperty(container: PropertyContainer, attribute: String): Property = {
    container.property(attribute.split('.'): _*)
  }

  def paramsFromFilters(filters: Array[Filter]): Map[String, Any] = {
    filters.flatMap(f => f.flattenFilters).map(_.getAttributeAndValue)
      .filter(_.nonEmpty)
      .map(valAndAtt => valAndAtt.head.toString.unquote() -> toParamValue(valAndAtt(1)))
      .toMap
  }

  def toParamValue(value: Any): Any = {
    value match {
      case date: java.sql.Date => date.toString
      case timestamp: java.sql.Timestamp => timestamp.toLocalDateTime
      case _ => value
    }
  }

  def valueToCypherExpression(attribute: String, value: Any): Expression = {
    val parameter = Cypher.parameter(attribute.toParameterName(value))
    value match {
      case d: java.sql.Date => Functions.date(parameter)
      case t: java.sql.Timestamp => Functions.localdatetime(parameter)
      case _ => parameter
    }
  }

  def mapSparkFiltersToCypher(filter: Filter, container: PropertyContainer, attributeAlias: Option[String] = None): Condition = {
    filter match {
      case eqns: EqualNullSafe =>
        val parameter = valueToCypherExpression(eqns.attribute, eqns.value)
        val property = getCorrectProperty(container, attributeAlias.getOrElse(eqns.attribute))
        property.isNull.and(parameter.isNull)
          .or(property.isEqualTo(parameter))
      case eq: EqualTo =>
        getCorrectProperty(container, attributeAlias.getOrElse(eq.attribute))
          .isEqualTo(valueToCypherExpression(eq.attribute, eq.value))
      case gt: GreaterThan =>
        getCorrectProperty(container, attributeAlias.getOrElse(gt.attribute))
          .gt(valueToCypherExpression(gt.attribute, gt.value))
      case gte: GreaterThanOrEqual =>
        getCorrectProperty(container, attributeAlias.getOrElse(gte.attribute))
          .gte(valueToCypherExpression(gte.attribute, gte.value))
      case lt: LessThan =>
        getCorrectProperty(container, attributeAlias.getOrElse(lt.attribute))
          .lt(valueToCypherExpression(lt.attribute, lt.value))
      case lte: LessThanOrEqual =>
        getCorrectProperty(container, attributeAlias.getOrElse(lte.attribute))
          .lte(valueToCypherExpression(lte.attribute, lte.value))
      case in: In =>
        getCorrectProperty(container, attributeAlias.getOrElse(in.attribute))
          .in(valueToCypherExpression(in.attribute, in.values))
      case startWith: StringStartsWith =>
        getCorrectProperty(container, attributeAlias.getOrElse(startWith.attribute))
          .startsWith(valueToCypherExpression(startWith.attribute, startWith.value))
      case endsWith: StringEndsWith =>
        getCorrectProperty(container, attributeAlias.getOrElse(endsWith.attribute))
          .endsWith(valueToCypherExpression(endsWith.attribute, endsWith.value))
      case contains: StringContains =>
        getCorrectProperty(container, attributeAlias.getOrElse(contains.attribute))
          .contains(valueToCypherExpression(contains.attribute, contains.value))
      case notNull: IsNotNull => getCorrectProperty(container, attributeAlias.getOrElse(notNull.attribute)).isNotNull
      case isNull: IsNull => getCorrectProperty(container, attributeAlias.getOrElse(isNull.attribute)).isNull
      case not: Not => mapSparkFiltersToCypher(not.child, container, attributeAlias).not()
      case filter@(_: Filter) => throw new IllegalArgumentException(s"Filter of type `$filter` is not supported.")
    }
  }

  def getStreamingPropertyName(options: Neo4jOptions): String = options.query.queryType match {
    case QueryType.RELATIONSHIP => s"rel.${options.streamingOptions.propertyName}"
    case _ => options.streamingOptions.propertyName
  }

  def callSchemaService[T](neo4jOptions: Neo4jOptions,
                           jobId: String,
                           filters: Array[Filter],
                           function: SchemaService => T): T = {
    val driverCache = new DriverCache(neo4jOptions.connection, jobId)
    val schemaService = new SchemaService(neo4jOptions, driverCache, filters)
    var hasError = false
    try {
      function(schemaService)
    } catch {
      case e: Throwable => {
        hasError = true
        throw e
      }
    } finally {
      schemaService.close()
      if (hasError) {
        driverCache.close()
      }
    }
  }

  def isRetryableException(neo4jTransientException: Neo4jException) = (neo4jTransientException.isInstanceOf[SessionExpiredException]
    || neo4jTransientException.isInstanceOf[TransientException]
    || neo4jTransientException.isInstanceOf[ServiceUnavailableException])

}
