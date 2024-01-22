package org.neo4j.spark.converter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.neo4j.driver.internal._
import org.neo4j.driver.types.{IsoDuration, Node, Relationship}
import org.neo4j.driver.{Value, Values}
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.Neo4jUtil

import java.time._
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.collection.JavaConverters._

trait DataConverter[T] {
  def convert(value: Any, dataType: DataType = null): T

  @tailrec
  private[converter] final def extractStructType(dataType: DataType): StructType = dataType match {
    case structType: StructType => structType
    case mapType: MapType => extractStructType(mapType.valueType)
    case arrayType: ArrayType => extractStructType(arrayType.elementType)
    case _ => throw new UnsupportedOperationException(s"$dataType not supported")
  }
}

object SparkToNeo4jDataConverter {
  def apply(): SparkToNeo4jDataConverter = new SparkToNeo4jDataConverter()
}

class SparkToNeo4jDataConverter extends DataConverter[Value] {
  override def convert(value: Any, dataType: DataType): Value = {
    value match {
      case date: java.sql.Date => convert(date.toLocalDate, dataType)
      case timestamp: java.sql.Timestamp => convert(timestamp.toLocalDateTime, dataType)
      case intValue: Int if dataType == DataTypes.DateType => convert(DateTimeUtils
        .toJavaDate(intValue), dataType)
      case longValue: Long if dataType == DataTypes.TimestampType => convert(DateTimeUtils
        .toJavaTimestamp(longValue), dataType)
      case unsafeRow: UnsafeRow => {
        val structType = extractStructType(dataType)
        val row = new GenericRowWithSchema(unsafeRow.toSeq(structType).toArray, structType)
        convert(row)
      }
      case struct: GenericRow => {
        def toMap(struct: GenericRow): Value = {
          Values.value(
            struct.schema.fields.map(
              f => f.name -> convert(struct.getAs(f.name), f.dataType)
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
      case unsafeArray: ArrayData => {
        val sparkType = dataType match {
          case arrayType: ArrayType => arrayType.elementType
          case _ => dataType
        }
        val javaList = unsafeArray.toSeq[AnyRef](sparkType)
          .map(elem => convert(elem, sparkType))
          .asJava
        Values.value(javaList)
      }
      case unsafeMapData: MapData => { // Neo4j only supports Map[String, AnyRef]
        val mapType = dataType.asInstanceOf[MapType]
        val map: Map[String, AnyRef] = (0 until unsafeMapData.numElements())
          .map(i => (unsafeMapData.keyArray().getUTF8String(i).toString, unsafeMapData.valueArray().get(i, mapType.valueType)))
          .toMap[String, AnyRef]
          .mapValues(innerValue => convert(innerValue, mapType.valueType))
          .toMap[String, AnyRef]
        Values.value(map.asJava)
      }
      case string: UTF8String => convert(string.toString)
      case _ => Values.value(value)
    }
  }
}

object Neo4jToSparkDataConverter {
  def apply(): Neo4jToSparkDataConverter = new Neo4jToSparkDataConverter()
}

class Neo4jToSparkDataConverter extends DataConverter[Any] {
  override def convert(value: Any, dataType: DataType): Any = {
    if (dataType != null && dataType == DataTypes.StringType && value != null && !value.isInstanceOf[String]) {
      convert(Neo4jUtil.mapper.writeValueAsString(value), dataType)
    } else {
      value match {
        case node: Node => {
          val map = node.asMap()
          val structType = extractStructType(dataType)
          val fields = structType
            .filter(field => field.name != Neo4jUtil.INTERNAL_ID_FIELD && field.name != Neo4jUtil.INTERNAL_LABELS_FIELD)
            .map(field => convert(map.get(field.name), field.dataType))
          InternalRow.fromSeq(Seq(convert(node.id()), convert(node.labels())) ++ fields)
        }
        case rel: Relationship => {
          val map = rel.asMap()
          val structType = extractStructType(dataType)
          val fields = structType
            .filter(field => field.name != Neo4jUtil.INTERNAL_REL_ID_FIELD
              && field.name != Neo4jUtil.INTERNAL_REL_TYPE_FIELD
              && field.name != Neo4jUtil.INTERNAL_REL_SOURCE_ID_FIELD
              && field.name != Neo4jUtil.INTERNAL_REL_TARGET_ID_FIELD)
            .map(field => convert(map.get(field.name), field.dataType))
          InternalRow.fromSeq(Seq(convert(rel.id()),
            convert(rel.`type`()),
            convert(rel.startNodeId()),
            convert(rel.endNodeId())) ++ fields)
        }
        case d: IsoDuration => {
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
          val elementType = if (dataType != null) dataType.asInstanceOf[ArrayType].elementType else null
          ArrayData.toArrayData(l.asScala.map(e => convert(e, elementType)).toArray)
        }
        case map: java.util.Map[_, _] => {
          if (dataType != null) {
            val mapType = dataType.asInstanceOf[MapType]
            ArrayBasedMapData(map.asScala.map(t => (convert(t._1, mapType.keyType), convert(t._2, mapType.valueType))))
          } else {
            ArrayBasedMapData(map.asScala.map(t => (convert(t._1), convert(t._2))))
          }
        }
        case s: String => UTF8String.fromString(s)
        case _ => value
      }
    }
  }
}
