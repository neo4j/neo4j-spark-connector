package org.neo4j.spark.service

import org.apache.spark.sql.SaveMode
import org.neo4j.spark.{Neo4jOptions, QueryType}
import org.neo4j.spark.util.Neo4jImplicits._

class Neo4jQueryWriteStrategy(private val saveMode: SaveMode) extends Neo4jQueryStrategy {
  override def createStatementForQuery(options: Neo4jOptions): String = throw new UnsupportedOperationException("TODO implement method")

  override def createStatementForRelationships(options: Neo4jOptions): String = throw new UnsupportedOperationException("TODO implement method")

  override def createStatementForNodes(options: Neo4jOptions): String = {
    val keyword = saveMode match {
      case SaveMode.Overwrite => "MERGE"
      case SaveMode.ErrorIfExists => "CREATE"
      case _ => throw new UnsupportedOperationException(s"SaveMode $saveMode not supported")
    }
    val labels = options.query.value
      .split(":")
      .map(_.trim)
      .filter(!_.isEmpty)
      .map(_.quote)
      .mkString(":")
    val keys = options.nodeMetadata.nodeKeys
      .map(_.quote)
      .map(k => s"$k: event.keys.$k")
      .mkString(", ")
    s"""UNWIND ${"$"}events AS event
       |$keyword (node${if (labels.isEmpty) "" else s":$labels"} ${if (keys.isEmpty) "" else s"{$keys}"})
       |SET node += event.properties
       |""".stripMargin
  }
}

abstract class Neo4jQueryStrategy {
  def createStatementForQuery(options: Neo4jOptions): String

  def createStatementForRelationships(options: Neo4jOptions): String

  def createStatementForNodes(options: Neo4jOptions): String
}

class Neo4jQueryService(private val options: Neo4jOptions, private val strategy: Neo4jQueryStrategy) {

  def createQuery(): String = options.query.queryType match {
    case QueryType.LABELS => strategy.createStatementForNodes(options)
    case QueryType.RELATIONSHIP => strategy.createStatementForRelationships(options)
    case QueryType.QUERY => strategy.createStatementForQuery(options)
    case _ => throw new UnsupportedOperationException(
      s"""Query Type not supported.
         |You provided ${options.query.queryType},
         |supported types: ${QueryType.values.mkString(",")}""".stripMargin)
  }
}
