package org.neo4j.spark.config

import org.apache.spark.sql.connector.expressions.SortOrder

case class TopN(limit: Int, orders: Array[SortOrder] = Array.empty) {

  def orderBy: String = {
    if (orders.isEmpty) {
      return ""
    }
    orders.map(foobar).mkString("ORDER BY ", ", ", "")
  }

  private def foobar(order: SortOrder): String = {
    order.toString
  }
}
