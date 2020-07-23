package org.neo4j.spark.util

import java.time._
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.neo4j.driver.internal.{InternalIsoDuration, InternalPoint2D, InternalPoint3D}
import org.neo4j.driver.{Session, Transaction, Values}
import org.neo4j.spark.service.SchemaService

import scala.collection.JavaConverters._

object Neo4jUtil {

  val unsupportedTransientCodes = Set("Neo.TransientError.Transaction.Terminated",
    "Neo.TransientError.Transaction.LockClientStopped") // use the same strategy for TransientException as in the driver

  def closeSafety(autoCloseable: AutoCloseable): Unit = {
    if (autoCloseable == null) {
      return Unit
    }

    try {
      autoCloseable match {
        case s: Session => if (s.isOpen) s.close()
        case t: Transaction => if (t.isOpen) t.close()
        case _ => autoCloseable.close()
      }
    } catch {
      case _ => throw new Exception("This exception should be logged") // @todo Log
    }
  }

  def convertFromNeo4j(value: Any): Any = value match {
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
    case l: java.util.List[Any] => ArrayData.toArrayData(l.asScala.map(convertFromNeo4j).toArray)
    case s: String => UTF8String.fromString(s)
    case _ => value
  }

  def convertFromSpark(value: Any, schema: StructField = null): AnyRef = value match {
    case date: java.sql.Date => convertFromSpark(date.toLocalDate)
    case timestamp: java.sql.Timestamp => convertFromSpark(timestamp.toInstant.atZone(ZoneOffset.UTC))
    case unsafeRow: UnsafeRow => {
      def extractStructType(dataType: DataType): StructType = dataType match {
        case structType: StructType => structType
        case arrayType: ArrayType => extractStructType(arrayType.elementType)
        case _ => throw new UnsupportedOperationException(s"$dataType not supported")
      }
      val structType = extractStructType(schema.dataType)
      val row = new GenericRowWithSchema(unsafeRow.toSeq(structType).toArray, structType)
      convertFromSpark(row)
    }
    case struct: GenericRowWithSchema => {
      if (struct.fieldIndex("type") == -1) {
        Values.NULL
      }
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
        case SchemaService.TIME_TYPE_OFFSET => Values.value(OffsetTime.parse(struct.getAs[String]("value")))
        case SchemaService.TIME_TYPE_LOCAL => Values.value(LocalTime.parse(struct.getAs[String]("value")))
        case _ => Values.NULL // check if it's correct
      }
    }
    case unsafeArray: UnsafeArrayData => {
      val sparkType = schema.dataType match {
        case arrayType: ArrayType => arrayType.elementType
        case _ => schema.dataType
      }
      val javaList = unsafeArray.toSeq[AnyRef](sparkType)
        .map(elem => convertFromSpark(elem, schema))
        .asJava
      Values.value(javaList)
    }
    case unsafeMapData: UnsafeMapData => { // Neo4j only supports Map[String, AnyRef]
      val mapType = schema.dataType.asInstanceOf[MapType]
      Values.value((0 to unsafeMapData.numElements() - 1)
        .map(i => (unsafeMapData.keyArray().getUTF8String(i).toString, unsafeMapData.valueArray().get(i, mapType.valueType)))
        .toMap[String, AnyRef]
        .mapValues(value => convertFromNeo4j(value))
        .asJava)
    }
    case string: UTF8String => convertFromSpark(string.toString)
    case _ => Values.value(value)
  }

  def flattenMap(map: java.util.Map[String, AnyRef],
                 prefix: String = ""): java.util.Map[String, AnyRef] = map.asScala.flatMap(t => {
      val key = if (prefix != "") s"$prefix.${t._1}" else t._1
      t._2 match {
        case nestedMap: Map[String, AnyRef] => flattenMap(nestedMap.asJava, key).asScala.toSeq
        case _ => Seq((key, t._2))
      }
    })
    .toMap
    .asJava

}
