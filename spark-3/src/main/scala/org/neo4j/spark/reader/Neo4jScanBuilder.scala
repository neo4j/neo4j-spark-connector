package org.neo4j.spark.reader

import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Aggregation}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownAggregates, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.neo4j.spark.util.Neo4jImplicits.{AggregationImplicit, CypherImplicits, PredicateImplicit}
import org.neo4j.spark.util.{Neo4jOptions, QueryType}

class Neo4jScanBuilder(neo4jOptions: Neo4jOptions, jobId: String, schema: StructType)
  extends SupportsPushDownV2Filters
  with SupportsPushDownAggregates
  with SupportsPushDownRequiredColumns {

  private var predicates: Array[Predicate] = Array.empty

  private var requiredSchema: StructType = schema
  private var requiredColumns: StructType = new StructType()

  private var aggregateColumns: Array[AggregateFunc] = Array.empty[AggregateFunc]

  override def build(): Scan = {
    new Neo4jScan(neo4jOptions, jobId, requiredSchema, predicates.flatMap(_.toFilter), requiredColumns, aggregateColumns)
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
    if (!neo4jOptions.pushdownAggregateEnabled
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
      val fields = try {
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
}
