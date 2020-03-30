package org.neo4j.spark

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.dataframe.CypherTypes
import org.neo4j.spark.utils.Neo4jSessionAwareIterator

import scala.collection.JavaConverters._


object Executor {

  def toJava(parameters: Map[String, Any]): java.util.Map[String, Object] = {
    parameters.mapValues(toJava).asJava
  }

  private def toJava(x: Any): AnyRef = x match {
    case y: Seq[_] => y.asJava
    case _ => x.asInstanceOf[AnyRef]
  }

  val EMPTY = Array.empty[Any]

  val EMPTY_RESULT = new CypherResult(new StructType(), Iterator.empty)

  class CypherResult(val schema: StructType, val rows: Iterator[Array[Any]]) {
    def sparkRows: Iterator[Row] = rows.map(row => new GenericRowWithSchema(row, schema))

    def fields = schema.fieldNames
  }

  def execute(sc: SparkContext, query: String, parameters: Map[String, AnyRef]): CypherResult = {
    execute(Neo4jConfig(sc.getConf), query, parameters)
  }

  private def rows(result: Iterator[_]) = {
    var i = 0
    while (result.hasNext) i = i + 1
    i
  }

  def execute(config: Neo4jConfig, query: String, parameters: Map[String, Any], write: Boolean = false): CypherResult = {
    val result = new Neo4jSessionAwareIterator(config, query, toJava(parameters), write)
    if (!result.hasNext) {
      return EMPTY_RESULT
    }
    val peek = result.peek()
    val keyCount = peek.size()
    if (keyCount == 0) {
      return new CypherResult(new StructType(), Array.fill[Array[Any]](rows(result))(EMPTY).toIterator)
    }
    val keys = peek.keys().asScala
    val fields = keys.map(k => (k, peek.get(k).`type`())).map(keyType => CypherTypes.field(keyType))
    val schema = StructType(fields)
    val it = result.map(record => {
      val row = new Array[Any](keyCount)
      var i = 0
      while (i < keyCount) {
        val value = record.get(i).asObject() match {
          case it: util.Map[_, _] => it.asScala
          case it: util.Collection[_] => it.toArray()
          case x => x
        }
        row.update(i, value)
        i = i + 1
      }
      row
    })
    new CypherResult(schema, it)
  }
}
