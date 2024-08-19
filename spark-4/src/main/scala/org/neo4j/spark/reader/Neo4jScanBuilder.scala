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
package org.neo4j.spark.reader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates
import org.apache.spark.sql.connector.read.SupportsPushDownLimit
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns
import org.apache.spark.sql.connector.read.SupportsPushDownTopN
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.config.TopN
import org.neo4j.spark.util.Neo4jImplicits.AggregationImplicit
import org.neo4j.spark.util.Neo4jImplicits.CypherImplicits
import org.neo4j.spark.util.Neo4jImplicits.PredicateImplicit
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.QueryType

class Neo4jScanBuilder(neo4jOptions: Neo4jOptions, jobId: String, schema: StructType)
    extends SupportsPushDownV2Filters
    with SupportsPushDownAggregates
    with SupportsPushDownRequiredColumns
    with SupportsPushDownLimit
    with SupportsPushDownTopN
    with Logging {

  private var predicates: Array[Predicate] = Array.empty

  private var requiredSchema: StructType = schema
  private var requiredColumns: StructType = new StructType()

  private var aggregateColumns: Array[AggregateFunc] = Array.empty[AggregateFunc]
  private var limit: Option[Int] = None
  private var topN: Option[TopN] = None

  override def build(): Scan = {
    new Neo4jScan(
      neo4jOptions,
      jobId,
      requiredSchema,
      predicates.flatMap(_.toFilter),
      requiredColumns,
      aggregateColumns,
      topN.orElse(limit.map((limit: Int) => TopN(limit)))
    )
  }

  override def pushPredicates(predicatesArray: Array[Predicate]): Array[Predicate] = {
    if (neo4jOptions.pushdownFiltersEnabled) {
      predicates = predicatesArray
    }

    predicatesArray
  }

  override def pushedPredicates(): Array[Predicate] = predicates

  override def pruneColumns(newSchema: StructType): Unit = {
    if (!neo4jOptions.pushdownColumnsEnabled || neo4jOptions.relationshipMetadata.nodeMap) {
      new StructType()
    } else {
      requiredColumns = StructType(requiredSchema.filter(sf => newSchema.contains(sf)))
    }
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (
      !neo4jOptions.pushdownAggregateEnabled
      || aggregation.aggregateExpressions().isEmpty
      || neo4jOptions.query.queryType == QueryType.QUERY
    ) {
      return false
    }
    aggregateColumns = aggregation.aggregateExpressions()
    val groupByColumns: Set[String] = aggregation.groupByCols()
      .map(_.describe().unquote())
      .toSet
    requiredColumns = StructType(requiredSchema.filter(field => groupByColumns.contains(field.name)))

    aggregateColumns.foreach(af => {
      val fields =
        try {
          af.children()
            .toSet[Expression]
            .map(_.describe())
            .map(_.unquote())
        } catch {
          // for making it compatible with Spark 3.2
          case noSuchMethodException: NoSuchMethodError => Set(af.describe().unquote())
        }
      val dt = if (fields.nonEmpty) {
        requiredSchema.filter(field => fields.contains(field.name))
          .map(_.dataType)
          .toSet
          .headOption
          .getOrElse(LongType)
      } else {
        LongType
      }
      requiredColumns = requiredColumns.add(StructField(af.toString, dt))
    })
    requiredSchema = requiredColumns
    true
  }

  override def pushLimit(pushedLimit: Int): Boolean = {
    if (!neo4jOptions.pushdownLimitEnabled) {
      return false
    }
    if (neo4jOptions.partitions > 1) {
      logWarning(
        s"""Disabling pushed down limit support since it conflicts with partitioning.
           |Set the `${Neo4jOptions.PARTITIONS}` parameter value to 1
           | or set `${Neo4jOptions.PUSHDOWN_LIMIT_ENABLED}` to false to remove this warning.
           |""".stripMargin
      )
      return false
    }
    if (pushedLimit <= 0) {
      logWarning(s"Ignoring negative pushed down limit $pushedLimit.")
      return false
    }
    limit = Some(pushedLimit)
    true
  }

  override def pushTopN(orders: Array[SortOrder], limit: Int): Boolean = {
    if (!neo4jOptions.pushdownTopNEnabled) {
      return false
    }
    if (limit > 0 && neo4jOptions.partitions > 1) {
      logWarning("disabling pushed down top N support since it conflicts with partitioning." +
        "set the partition count to 1 or disable the pushdown limit support to remove this warning")
      return false
    }
    topN = Some(TopN(limit, orders))
    true
  }

  // otherwise doesn't compile in Scala 2.12
  override def isPartiallyPushed: Boolean = true
}
