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
package org.neo4j.spark.service

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.Record
import org.neo4j.driver.Value
import org.neo4j.driver.Values
import org.neo4j.driver.internal.value.MapValue
import org.neo4j.driver.types.Node
import org.neo4j.spark.converter.Neo4jToSparkDataConverter
import org.neo4j.spark.converter.SparkToNeo4jDataConverter
import org.neo4j.spark.service.Neo4jWriteMappingStrategy.KEYS
import org.neo4j.spark.service.Neo4jWriteMappingStrategy.PROPERTIES
import org.neo4j.spark.util.Neo4jImplicits._
import org.neo4j.spark.util.Neo4jNodeMetadata
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.Neo4jUtil
import org.neo4j.spark.util.QueryType
import org.neo4j.spark.util.RelationshipSaveStrategy
import org.neo4j.spark.util.ValidateSchemaOptions
import org.neo4j.spark.util.Validations

import java.util
import java.util.function
import java.util.function.BiConsumer

import scala.collection.JavaConverters._
import scala.collection.mutable

class Neo4jWriteMappingStrategy(private val options: Neo4jOptions)
    extends Neo4jMappingStrategy[InternalRow, java.util.Map[String, AnyRef]]
    with Logging {

  private val dataConverter = SparkToNeo4jDataConverter()

  override def node(row: InternalRow, schema: StructType): java.util.Map[String, AnyRef] = {
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
          properties.put(options.nodeMetadata.properties.getOrElse(key, key), value)
        }
      })

    rowMap
  }

  private def nativeStrategyConsumer(): MappingBiConsumer = new MappingBiConsumer {

    override def accept(key: String, value: AnyRef): Unit = {
      if (key.startsWith(Neo4jUtil.RELATIONSHIP_ALIAS.concat("."))) {
        relMap.get(PROPERTIES).put(key.removeAlias(), value)
      } else if (key.startsWith(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS.concat("."))) {
        if (options.relationshipMetadata.source.nodeKeys.contains(key)) {
          sourceNodeMap.get(KEYS).put(key.removeAlias(), value)
        } else {
          sourceNodeMap.get(PROPERTIES).put(key.removeAlias(), value)
        }
      } else if (key.startsWith(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS.concat("."))) {
        if (options.relationshipMetadata.target.nodeKeys.contains(key)) {
          targetNodeMap.get(KEYS).put(key.removeAlias(), value)
        } else {
          targetNodeMap.get(PROPERTIES).put(key.removeAlias(), value)
        }
      }
    }
  }

  private def addToNodeMap(
    nodeMap: util.Map[String, util.Map[String, AnyRef]],
    nodeMetadata: Neo4jNodeMetadata,
    key: String,
    value: AnyRef
  ): Unit = {
    if (nodeMetadata.nodeKeys.contains(key)) {
      nodeMap.get(KEYS).put(nodeMetadata.nodeKeys.getOrElse(key, key), value)
    }
    if (nodeMetadata.properties.contains(key)) {
      nodeMap.get(PROPERTIES).put(nodeMetadata.properties.getOrElse(key, key), value)
    }
  }

  private def keysStrategyConsumer(): MappingBiConsumer = new MappingBiConsumer {

    override def accept(key: String, value: AnyRef): Unit = {
      val source = options.relationshipMetadata.source
      val target = options.relationshipMetadata.target

      addToNodeMap(sourceNodeMap, source, key, value)
      addToNodeMap(targetNodeMap, target, key, value)

      if (options.relationshipMetadata.relationshipKeys.contains(key)) {
        relMap.get(KEYS).put(options.relationshipMetadata.relationshipKeys.getOrElse(key, key), value)
      } else if (!source.includesProperty(key) && !target.includesProperty(key)) {
        relMap.get(PROPERTIES).put(options.relationshipMetadata.properties.getOrElse(key, key), value)
      }
    }
  }

  override def relationship(row: InternalRow, schema: StructType): java.util.Map[String, AnyRef] = {
    val rowMap: java.util.Map[String, AnyRef] = new java.util.HashMap[String, AnyRef]

    val consumer = options.relationshipMetadata.saveStrategy match {
      case RelationshipSaveStrategy.NATIVE => nativeStrategyConsumer()
      case RelationshipSaveStrategy.KEYS   => keysStrategyConsumer()
    }

    query(row, schema).forEach(consumer)

    if (
      options.relationshipMetadata.saveStrategy.equals(RelationshipSaveStrategy.NATIVE)
      && consumer.relMap.get(PROPERTIES).isEmpty
      && consumer.sourceNodeMap.get(PROPERTIES).isEmpty && consumer.sourceNodeMap.get(KEYS).isEmpty
      && consumer.targetNodeMap.get(PROPERTIES).isEmpty && consumer.targetNodeMap.get(KEYS).isEmpty
    ) {
      throw new IllegalArgumentException(
        "NATIVE write strategy requires a schema like: rel.[props], source.[props], target.[props]. " +
          "All of this columns are empty in the current schema."
      )
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
        val neo4jValue = dataConverter.convert(seq(i), field.dataType)
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

class Neo4jReadMappingStrategy(private val options: Neo4jOptions, requiredColumns: StructType)
    extends Neo4jMappingStrategy[Record, InternalRow] {

  private val dataConverter = Neo4jToSparkDataConverter()

  override def node(record: Record, schema: StructType): InternalRow = {
    if (requiredColumns.nonEmpty) {
      query(record, schema)
    } else {
      val node = record.get(Neo4jUtil.NODE_ALIAS).asNode()
      val nodeMap = new util.HashMap[String, Any](node.asMap())
      nodeMap.put(Neo4jUtil.INTERNAL_ID_FIELD, node.id())
      nodeMap.put(Neo4jUtil.INTERNAL_LABELS_FIELD, node.labels())

      mapToInternalRow(nodeMap, schema)
    }
  }

  private def mapToInternalRow(map: util.Map[String, Any], schema: StructType) = InternalRow
    .fromSeq(
      schema.map(field => dataConverter.convert(map.get(field.name), field.dataType))
    )

  private def flatRelNodeMapping(node: Node, alias: String): mutable.Map[String, Any] = {
    val nodeMap: mutable.Map[String, Any] = node.asMap().asScala
      .map(t => (s"$alias.${t._1}", t._2))
    nodeMap.put(
      s"<$alias.${
          Neo4jUtil.INTERNAL_ID_FIELD
            .replaceAll("[<|>]", "")
        }>",
      node.id()
    )
    nodeMap.put(
      s"<$alias.${
          Neo4jUtil.INTERNAL_LABELS_FIELD
            .replaceAll("[<|>]", "")
        }>",
      node.labels()
    )
    nodeMap
  }

  private def mapRelNodeMapping(node: Node, alias: String): Map[String, util.Map[String, String]] = {
    val nodeMap: util.Map[String, String] =
      new util.HashMap[String, String](node.asMap(new function.Function[Value, String] {
        override def apply(t: Value): String = t.toString
      }))

    nodeMap.put(Neo4jUtil.INTERNAL_ID_FIELD, Neo4jUtil.mapper.writeValueAsString(node.id()))
    nodeMap.put(Neo4jUtil.INTERNAL_LABELS_FIELD, Neo4jUtil.mapper.writeValueAsString(node.labels()))
    Map(s"<$alias>" -> nodeMap)
  }

  override def relationship(record: Record, schema: StructType): InternalRow = {
    if (requiredColumns.nonEmpty) {
      query(record, schema)
    } else {
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
        (
          mapRelNodeMapping(source, Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS),
          mapRelNodeMapping(target, Neo4jUtil.RELATIONSHIP_TARGET_ALIAS)
        )
      } else {
        (
          flatRelNodeMapping(source, Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS),
          flatRelNodeMapping(target, Neo4jUtil.RELATIONSHIP_TARGET_ALIAS)
        )
      }

      relMap.putAll(sourceMap.toMap.asJava)
      relMap.putAll(targetMap.toMap.asJava)

      mapToInternalRow(relMap, schema)
    }
  }

  override def query(elem: Record, schema: StructType): InternalRow = mapToInternalRow(
    elem.asMap(new function.Function[Value, Any] {
      override def apply(t: Value): Any = t.asObject()
    }),
    schema
  )
}

abstract class Neo4jMappingStrategy[IN, OUT] extends Serializable {
  def node(elem: IN, schema: StructType): OUT

  def relationship(elem: IN, schema: StructType): OUT

  def query(elem: IN, schema: StructType): OUT
}

class MappingService[IN, OUT](private val strategy: Neo4jMappingStrategy[IN, OUT], private val options: Neo4jOptions)
    extends Serializable {

  def convert(record: IN, schema: StructType): OUT = options.query.queryType match {
    case QueryType.LABELS       => strategy.node(record, schema)
    case QueryType.RELATIONSHIP => strategy.relationship(record, schema)
    case QueryType.QUERY        => strategy.query(record, schema)
    case QueryType.GDS          => strategy.query(record, schema)
  }

}

object Neo4jWriteMappingStrategy {
  val KEYS = "keys"
  val PROPERTIES = "properties"
}

abstract private class MappingBiConsumer extends BiConsumer[String, AnyRef] {

  val relMap = new util.HashMap[String, util.Map[String, AnyRef]]()
  val sourceNodeMap = new util.HashMap[String, util.Map[String, AnyRef]]()
  val targetNodeMap = new util.HashMap[String, util.Map[String, AnyRef]]()

  relMap.put(KEYS, new util.HashMap[String, AnyRef]())
  relMap.put(PROPERTIES, new util.HashMap[String, AnyRef]())
  sourceNodeMap.put(PROPERTIES, new util.HashMap[String, AnyRef]())
  sourceNodeMap.put(KEYS, new util.HashMap[String, AnyRef]())
  targetNodeMap.put(PROPERTIES, new util.HashMap[String, AnyRef]())
  targetNodeMap.put(KEYS, new util.HashMap[String, AnyRef]())
}
