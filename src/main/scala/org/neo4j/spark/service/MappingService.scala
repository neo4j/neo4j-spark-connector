package org.neo4j.spark.service

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.Values
import org.neo4j.driver.internal.value.MapValue
import org.neo4j.spark.util.Neo4jUtil
import org.neo4j.spark.{Neo4jOptions, QueryType}

import collection.JavaConverters._

class MappingService(private val options: Neo4jOptions) {

  private def toQuery(record: InternalRow, structType: StructType): java.util.Map[String, Object] = {
    throw new UnsupportedOperationException("TODO implement the method")
  }

  private def toRelationship(record: InternalRow, structType: StructType): java.util.Map[String, Object] = {
    throw new UnsupportedOperationException("TODO implement the method")
  }

  private def toNode(row: InternalRow, schema: StructType): java.util.Map[String, Object] = {
    val rowMap: java.util.Map[String, Object] = new java.util.HashMap[String, Object]
    val keys: java.util.Map[String, Object] = new java.util.HashMap[String, Object]
    val properties: java.util.Map[String, Object] = new java.util.HashMap[String, Object]
    rowMap.put("keys", keys)
    rowMap.put("properties", properties)

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
        .foreach(t => {
          if (options.nodeMetadata.nodeKeys.contains(t._1)) {
            keys.put(t._1, t._2)
          } else {
            properties.put(t._1, t._2)
          }
        })

    rowMap
  }

  def toParameter(row: InternalRow, schema: StructType): java.util.Map[String, Object] = options.query.queryType match {
    case QueryType.LABELS => toNode(row, schema)
    case QueryType.RELATIONSHIP => toRelationship(row, schema)
    case QueryType.QUERY => toQuery(row, schema)
  }

}