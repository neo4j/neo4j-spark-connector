package org.neo4j.spark.service

import java.util
import java.util.function
import java.util.function.BiConsumer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.internal.value.MapValue
import org.neo4j.driver.types.Node
import org.neo4j.driver.{Record, Value, Values}
import org.neo4j.spark.service.Neo4jWriteMappingStrategy.{KEYS, PROPERTIES}
import org.neo4j.spark.util.{Neo4jNodeMetadata, Neo4jOptions, Neo4jUtil, QueryType, RelationshipSaveStrategy, Validations}

import scala.collection.JavaConverters._
import scala.collection.mutable
import org.neo4j.spark.util.Neo4jImplicits._

class Neo4jWriteMappingStrategy(private val options: Neo4jOptions)
  extends Neo4jMappingStrategy[InternalRow, java.util.Map[String, AnyRef]] {

  override def node(row: InternalRow, schema: StructType): java.util.Map[String, AnyRef] = {
    Validations.schemaOptions(options, schema)

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

    Validations.schemaOptions(options, schema)

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
    (0 to schema.size - 1)
      .flatMap(i => {
        val field = schema(i)
        val neo4jValue = Neo4jUtil.convertFromSpark(seq(i), field)
        neo4jValue match {
          case map: MapValue => Neo4jUtil.flattenMap(map.asMap(), field.name)
            .asScala
            .map(t => (t._1, Values.value(t._2)))
            .toSeq
          case _ => Seq((field.name, neo4jValue))
        }
      })
      .toMap
      .asJava
  }
}

class Neo4jReadMappingStrategy(private val options: Neo4jOptions, requiredColumns: StructType) extends Neo4jMappingStrategy[Record, InternalRow] {

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

  private def mapToInternalRow(map: util.Map[String, Any],
                               schema: StructType) = InternalRow.fromSeq(schema
    .map(field => Neo4jUtil.convertFromNeo4j(map.get(field.name), field.dataType)))

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