package org.neo4j.spark.service

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, MapType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.neo4j.driver.internal.value.MapValue
import org.neo4j.driver.internal.{InternalIsoDuration, InternalNode, InternalPoint2D, InternalPoint3D, InternalRelationship}
import org.neo4j.driver.types.Node
import org.neo4j.driver.{Record, Value, Values}
import org.neo4j.spark.service.Neo4jWriteMappingStrategy.{KEYS, PROPERTIES}
import org.neo4j.spark.util.Neo4jImplicits._
import org.neo4j.spark.util.Neo4jUtil.{extractStructType, mapper}
import org.neo4j.spark.util.{DriverCache, Neo4jNodeMetadata, Neo4jOptions, Neo4jUtil, QueryType, RelationshipSaveStrategy, ValidateSchemaOptions, Validations}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetTime, ZoneOffset, ZonedDateTime}
import java.util
import java.util.function
import java.util.function.BiConsumer
import scala.collection.JavaConverters._
import scala.collection.mutable

class Neo4jWriteMappingStrategy(private val options: Neo4jOptions, private val jobId: String)
  extends Neo4jMappingStrategy[InternalRow, java.util.Map[String, AnyRef]]
    with Logging {

  override def node(row: InternalRow, schema: StructType): java.util.Map[String, AnyRef] = {
    Validations.validate(ValidateSchemaOptions(options, schema))

    val rowMap: java.util.Map[String, Object] = new java.util.HashMap[String, Object]
    val keys: java.util.Map[String, Object] = new java.util.HashMap[String, Object]
    val properties: java.util.Map[String, Object] = new java.util.HashMap[String, Object]
    rowMap.put(KEYS, keys)
    rowMap.put(PROPERTIES, properties)

    query(row, schema)
      .forEach(new BiConsumer[String, AnyRef] {
        override def accept(key: String, value: AnyRef): Unit = if (options.nodeMetadata.nodeKeys.contains(key)) {
          keys.put(options.nodeMetadata.nodeKeys.getOrElse(key, key), value)
        } else {
          properties.put(options.nodeMetadata.nodeProps.getOrElse(key, key), value)
        }
      })

    rowMap
  }

  private def nativeStrategyConsumer(): MappingBiConsumer = new MappingBiConsumer {
    override def accept(key: String, value: AnyRef): Unit = {
      if (key.startsWith(Neo4jUtil.RELATIONSHIP_ALIAS.concat("."))) {
        relMap.get(PROPERTIES).put(key.removeAlias(), value)
      }
      else if (key.startsWith(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS.concat("."))) {
        if (options.relationshipMetadata.source.nodeKeys.contains(key)) {
          sourceNodeMap.get(KEYS).put(key.removeAlias(), value)
        }
        else {
          sourceNodeMap.get(PROPERTIES).put(key.removeAlias(), value)
        }
      }
      else if (key.startsWith(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS.concat("."))) {
        if (options.relationshipMetadata.target.nodeKeys.contains(key)) {
          targetNodeMap.get(KEYS).put(key.removeAlias(), value)
        }
        else {
          targetNodeMap.get(PROPERTIES).put(key.removeAlias(), value)
        }
      }
    }
  }

  private def addToNodeMap(nodeMap: util.Map[String, util.Map[String, AnyRef]], nodeMetadata: Neo4jNodeMetadata, key: String, value: AnyRef): Unit = {
    if (nodeMetadata.nodeKeys.contains(key)) {
      nodeMap.get(KEYS).put(nodeMetadata.nodeKeys.getOrElse(key, key), value)
    }
    if (nodeMetadata.nodeProps.contains(key)) {
      nodeMap.get(PROPERTIES).put(nodeMetadata.nodeProps.getOrElse(key, key), value)
    }
  }

  private def keysStrategyConsumer(): MappingBiConsumer = new MappingBiConsumer {
    override def accept(key: String, value: AnyRef): Unit = {
      val source = options.relationshipMetadata.source
      val target = options.relationshipMetadata.target

      addToNodeMap(sourceNodeMap, source, key, value)
      addToNodeMap(targetNodeMap, target, key, value)

      if (options.relationshipMetadata.properties.contains(key)) {
        relMap.get(PROPERTIES).put(options.relationshipMetadata.properties.getOrElse(key, key), value)
      }
    }
  }

  override def relationship(row: InternalRow, schema: StructType): java.util.Map[String, AnyRef] = {
    val rowMap: java.util.Map[String, AnyRef] = new java.util.HashMap[String, AnyRef]

    Validations.validate(ValidateSchemaOptions(options, schema))

    val consumer = options.relationshipMetadata.saveStrategy match {
      case RelationshipSaveStrategy.NATIVE => nativeStrategyConsumer()
      case RelationshipSaveStrategy.KEYS => keysStrategyConsumer()
    }

    query(row, schema).forEach(consumer)

    if (
      options.relationshipMetadata.saveStrategy.equals(RelationshipSaveStrategy.NATIVE)
        && consumer.relMap.get(PROPERTIES).isEmpty
        && consumer.sourceNodeMap.get(PROPERTIES).isEmpty && consumer.sourceNodeMap.get(KEYS).isEmpty
        && consumer.targetNodeMap.get(PROPERTIES).isEmpty && consumer.targetNodeMap.get(KEYS).isEmpty
    ) {
      throw new IllegalArgumentException("NATIVE write strategy requires a schema like: rel.[props], source.[props], target.[props]. " +
        "All of this columns are empty in the current schema.")
    }

    rowMap.put(Neo4jUtil.RELATIONSHIP_ALIAS, consumer.relMap)
    rowMap.put(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS, consumer.sourceNodeMap)
    rowMap.put(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS, consumer.targetNodeMap)

    rowMap
  }
  override def query(row: InternalRow, schema: StructType): java.util.Map[String, AnyRef] = {
    val seq = row.toSeq(schema)
    schema.indices
      .flatMap(i => {
        val field = schema(i)
        val neo4jValue = MappingService.convertFromSpark(seq(i), field.dataType)
        neo4jValue match {
          case map: MapValue =>
            map.asMap().asScala.toMap
              .flattenMap(field.name, options.schemaMetadata.mapGroupDuplicateKeys)
              .mapValues(value => Values.value(value).asInstanceOf[AnyRef])
              .toSeq
          case _ => Seq((field.name, neo4jValue))
        }
      })
      .toMap
      .asJava
  }
}

// FIXME: find a better way for Neo4jImplicits to rely on convertFromSpark
object MappingService {
  def convertFromSpark(value: Any, dataType: DataType = null): Value = value match {
    case date: java.sql.Date => convertFromSpark(date.toLocalDate, dataType)
    case timestamp: java.sql.Timestamp =>
      // TODO discuss with Florent, I'm not sure about this
      // I think that is better here to get the value of
      // spark.sql.session.timeZone
      //      val config = new DriverCache(options.connection, jobId).getServerConfig
      //      val tz = config.getOrElse("db.temporal.timezone", "Z")
      // This is the correct implementation for me
      val tz = SparkSession.getActiveSession
        .map(_.conf)
        .map(c => c.get("spark.sql.session.timeZone"))
        .getOrElse("Z")
      convertFromSpark(timestamp.toInstant.atZone(ZoneOffset.of(tz)), dataType)
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
            f => f.name -> convertFromSpark(struct.getAs(f.name), f.dataType)
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

}

class Neo4jReadMappingStrategy(private val options: Neo4jOptions, requiredColumns: StructType, jobId: String) extends Neo4jMappingStrategy[Record, InternalRow] {

  override def node(record: Record, schema: StructType): InternalRow = {
    if (requiredColumns.nonEmpty) {
      query(record, schema)
    }
    else {
      val node = record.get(Neo4jUtil.NODE_ALIAS).asNode()
      val nodeMap = new util.HashMap[String, Any](node.asMap())
      nodeMap.put(Neo4jUtil.INTERNAL_ID_FIELD, node.id())
      nodeMap.put(Neo4jUtil.INTERNAL_LABELS_FIELD, node.labels())

      mapToInternalRow(nodeMap, schema)
    }
  }

  private def convertFromNeo4j(value: Any, schema: DataType = null): Any = {
    if (schema != null && schema == DataTypes.StringType && value != null && !value.isInstanceOf[String]) {
      convertFromNeo4j(mapper.writeValueAsString(value), schema)
    } else {
      value match {
        case node: InternalNode => {
          val map = node.asMap()
          val structType = Neo4jUtil.extractStructType(schema)
          val fields = structType
            .filter(field => field.name != Neo4jUtil.INTERNAL_ID_FIELD && field.name != Neo4jUtil.INTERNAL_LABELS_FIELD)
            .map(field => convertFromNeo4j(map.get(field.name), field.dataType))
          InternalRow.fromSeq(Seq(convertFromNeo4j(node.id()), convertFromNeo4j(node.labels())) ++ fields)
        }
        case rel: InternalRelationship => {
          val map = rel.asMap()
          val structType = Neo4jUtil.extractStructType(schema)
          val fields = structType
            .filter(field => field.name != Neo4jUtil.INTERNAL_REL_ID_FIELD
              && field.name != Neo4jUtil.INTERNAL_REL_TYPE_FIELD
              && field.name != Neo4jUtil.INTERNAL_REL_SOURCE_ID_FIELD
              && field.name != Neo4jUtil.INTERNAL_REL_TARGET_ID_FIELD)
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
        case zt: ZonedDateTime => DateTimeUtils.anyToMicros(zt.toInstant)
        case dt: LocalDateTime => {
          val config = new DriverCache(options.connection, jobId).getServerConfig
          val tz = config.getOrElse("db.temporal.timezone", "Z")
          DateTimeUtils.anyToMicros(dt.toInstant(ZoneOffset.of(tz)))
        }
        case d: LocalDate => DateTimeUtils.anyToDays(d)
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

  private def mapToInternalRow(map: util.Map[String, Any],
                               schema: StructType) = InternalRow
    .fromSeq(
      schema.map(
        field => convertFromNeo4j(map.get(field.name), field.dataType)
      )
    )

  private def flatRelNodeMapping(node: Node, alias: String): mutable.Map[String, Any] = {
    val nodeMap: mutable.Map[String, Any] = node.asMap().asScala
      .map(t => (s"$alias.${t._1}", t._2))
    nodeMap.put(s"<$alias.${
      Neo4jUtil.INTERNAL_ID_FIELD
        .replaceAll("[<|>]", "")
    }>",
      node.id())
    nodeMap.put(s"<$alias.${
      Neo4jUtil.INTERNAL_LABELS_FIELD
        .replaceAll("[<|>]", "")
    }>",
      node.labels())
    nodeMap
  }

  private def mapRelNodeMapping(node: Node, alias: String): Map[String, util.Map[String, String]] = {
    val nodeMap: util.Map[String, String] = new util.HashMap[String, String](node.asMap(new function.Function[Value, String] {
      override def apply(t: Value): String = t.toString
    }))

    nodeMap.put(Neo4jUtil.INTERNAL_ID_FIELD, Neo4jUtil.mapper.writeValueAsString(node.id()))
    nodeMap.put(Neo4jUtil.INTERNAL_LABELS_FIELD, Neo4jUtil.mapper.writeValueAsString(node.labels()))
    Map(s"<$alias>" -> nodeMap)
  }

  override def relationship(record: Record, schema: StructType): InternalRow = {
    if (requiredColumns.nonEmpty) {
      query(record, schema)
    }
    else {
      val rel = record.get(Neo4jUtil.RELATIONSHIP_ALIAS).asRelationship()
      val relMap = new util.HashMap[String, Any](rel.asMap())
        .asScala
        .map(t => (s"rel.${t._1}", t._2))
        .asJava
      relMap.put(Neo4jUtil.INTERNAL_REL_ID_FIELD, rel.id())
      relMap.put(Neo4jUtil.INTERNAL_REL_TYPE_FIELD, rel.`type`())

      val source = record.get(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS).asNode()
      val target = record.get(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS).asNode()

      val (sourceMap, targetMap) = if (options.relationshipMetadata.nodeMap) {
        (mapRelNodeMapping(source, Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS),
          mapRelNodeMapping(target, Neo4jUtil.RELATIONSHIP_TARGET_ALIAS))
      } else {
        (flatRelNodeMapping(source, Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS),
          flatRelNodeMapping(target, Neo4jUtil.RELATIONSHIP_TARGET_ALIAS))
      }

      relMap.putAll(sourceMap.toMap.asJava)
      relMap.putAll(targetMap.toMap.asJava)

      mapToInternalRow(relMap, schema)
    }
  }

  override def query(elem: Record, schema: StructType): InternalRow = mapToInternalRow(elem.asMap(new function.Function[Value, Any] {
    override def apply(t: Value): Any = t.asObject()
  }), schema)
}

abstract class Neo4jMappingStrategy[IN, OUT] extends Serializable {
  def node(elem: IN, schema: StructType): OUT

  def relationship(elem: IN, schema: StructType): OUT

  def query(elem: IN, schema: StructType): OUT
}

class MappingService[IN, OUT](private val strategy: Neo4jMappingStrategy[IN, OUT], private val options: Neo4jOptions) extends Serializable {

  def convert(record: IN, schema: StructType): OUT = options.query.queryType match {
    case QueryType.LABELS => strategy.node(record, schema)
    case QueryType.RELATIONSHIP => strategy.relationship(record, schema)
    case QueryType.QUERY => strategy.query(record, schema)
  }

}

object Neo4jWriteMappingStrategy {
  val KEYS = "keys"
  val PROPERTIES = "properties"
}

private abstract class MappingBiConsumer extends BiConsumer[String, AnyRef] {

  val relMap = new util.HashMap[String, util.Map[String, AnyRef]]()
  val sourceNodeMap = new util.HashMap[String, util.Map[String, AnyRef]]()
  val targetNodeMap = new util.HashMap[String, util.Map[String, AnyRef]]()

  relMap.put(PROPERTIES, new util.HashMap[String, AnyRef]())
  sourceNodeMap.put(PROPERTIES, new util.HashMap[String, AnyRef]())
  sourceNodeMap.put(KEYS, new util.HashMap[String, AnyRef]())
  targetNodeMap.put(PROPERTIES, new util.HashMap[String, AnyRef]())
  targetNodeMap.put(KEYS, new util.HashMap[String, AnyRef]())
}
