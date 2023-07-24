package org.neo4j.spark.config

import org.apache.spark.sql.connector.expressions.SortOrder

case class TopN(limit: Int, orders: Array[SortOrder] = Array.empty)
