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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.expressions.SortDirection
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.expressions.aggregate._
import org.apache.spark.sql.sources.And
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.Or
import org.neo4j.caniuse.Neo4j
import org.neo4j.cypherdsl.core._
import org.neo4j.spark.cypher.CypherPreamble.fullPreamble
import org.neo4j.spark.cypher.CypherRenderer
import org.neo4j.spark.service.Neo4jQueryStrategy.VARIABLE_EVENT
import org.neo4j.spark.service.Neo4jQueryStrategy.unwindEventsAsEvent
import org.neo4j.spark.util.Neo4jImplicits._
import org.neo4j.spark.util.Neo4jNodeMetadata
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.Neo4jRelationshipMetadata
import org.neo4j.spark.util.Neo4jTuningOptions
import org.neo4j.spark.util.Neo4jUtil
import org.neo4j.spark.util.NodeSaveMode
import org.neo4j.spark.util.QueryType

import scala.jdk.CollectionConverters.SeqHasAsJava

class Neo4jQueryWriteStrategy(
  private val neo4j: Neo4j,
  private val renderer: CypherRenderer,
  private val saveMode: SaveMode,
  private val withPreamble: Boolean = true
) extends Neo4jQueryStrategy {

  override def createStatementForQuery(options: Neo4jOptions): String = {
    val preamble = if (withPreamble) fullPreamble(neo4j, options) else ""
    val scriptResult = Neo4jQueryStrategy.scriptResultClause(options)
    preamble + scriptResult + unwindEventsAsEvent + options.query.value
  }

  override def createStatementForRelationships(options: Neo4jOptions): String = {
    val sourceNode = cypherNode(options.relationshipMetadata.source, Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS, "source")
    val targetNode = cypherNode(options.relationshipMetadata.target, Neo4jUtil.RELATIONSHIP_TARGET_ALIAS, "target")

    val relationshipRel =
      cypherRelationship(sourceNode, targetNode, options.relationshipMetadata, Neo4jUtil.RELATIONSHIP_ALIAS)

    val sourceNodeClause = options.relationshipMetadata.sourceSaveMode match {
      case NodeSaveMode.Overwrite => Cypher.merge(sourceNode).mutate(
          sourceNode,
          Cypher.property(Cypher.name(VARIABLE_EVENT), "source", "properties")
        )
      case NodeSaveMode.Append => Cypher.create(sourceNode).mutate(
          sourceNode,
          Cypher.property(Cypher.name(VARIABLE_EVENT), "source", "properties")
        )
      case NodeSaveMode.Match => Cypher.`match`(sourceNode)
    }

    val isSourceMatch = options.relationshipMetadata.sourceSaveMode == NodeSaveMode.Match
    val isTargetMatch = options.relationshipMetadata.targetSaveMode == NodeSaveMode.Match

    val bothNodesClause = if (!isSourceMatch && isTargetMatch) {
      val withClause = sourceNodeClause.`with`(Cypher.name("source"), Cypher.name("event"))

      options.relationshipMetadata.targetSaveMode match {
        case NodeSaveMode.Overwrite => withClause.merge(targetNode).mutate(
            targetNode,
            Cypher.property(Cypher.name(VARIABLE_EVENT), "target", "properties")
          )
        case NodeSaveMode.Append => withClause.create(sourceNode).mutate(
            sourceNode,
            Cypher.property(Cypher.name(VARIABLE_EVENT), "target", "properties")
          )
        case NodeSaveMode.Match => withClause.`match`(targetNode)
      }
    } else {
      options.relationshipMetadata.targetSaveMode match {
        case NodeSaveMode.Overwrite => sourceNodeClause.merge(targetNode).mutate(
            targetNode,
            Cypher.property(Cypher.name(VARIABLE_EVENT), "target", "properties")
          )
        case NodeSaveMode.Append => sourceNodeClause.create(targetNode).mutate(
            targetNode,
            Cypher.property(Cypher.name(VARIABLE_EVENT), "target", "properties")
          )
        case NodeSaveMode.Match if options.relationshipMetadata.sourceSaveMode == NodeSaveMode.Match =>
          Cypher.`match`(sourceNode, targetNode)
        case _ => throw new IllegalStateException(
            "Impossible state due to the way we build the internal query. Please report this bug."
          )
      }
    }

    val preamble = if (withPreamble) fullPreamble(neo4j, options) else ""

    val test = bothNodesClause
      .merge(relationshipRel)
      .mutate(relationshipRel, Cypher.property(Cypher.name(VARIABLE_EVENT), "rel", "properties"))
      .build()

    preamble + unwindEventsAsEvent + renderer.render(test)
  }

  override def createStatementForNodes(options: Neo4jOptions): String = {
    val node = cypherNode(options.nodeMetadata, "node")
    val nodeMatcher = matcher(node, saveMode)
    val preamble = if (withPreamble) fullPreamble(neo4j, options) else ""
    val query = renderer.render(nodeMatcher.mutate(node, Neo4jQueryStrategy.eventProperties).build())
    preamble + unwindEventsAsEvent + query
  }

  private def cypherNode(nodeData: Neo4jNodeMetadata, alias: String, propertyPrefix: String = ""): Node = {
    val n = Cypher.node(nodeData.labels.head, nodeData.labels.tail: _*).named(alias)

    val keyProperties = cypherNodeKeys(nodeData.nodeKeys.toSeq, propertyPrefix)

    if (keyProperties.nonEmpty) {
      n.withProperties(keyProperties: _*)
    } else {
      n
    }
  }

  private def cypherRelationship(
    source: Node,
    target: Node,
    relationshipData: Neo4jRelationshipMetadata,
    alias: String
  ): Relationship = {
    val r = source.relationshipTo(target, relationshipData.relationshipType).named(Cypher.name(alias))
    val keyProperties = cypherRelationshipKeys(relationshipData.relationshipKeys.toSeq)

    if (keyProperties.nonEmpty) {
      r.withProperties(keyProperties: _*)
    } else {
      r
    }
  }

  private def cypherNodeKeys(mappings: Seq[(String, String)], propertyPrefix: String): Seq[Object] = {
    mappings.flatMap { case (to, from) =>
      Seq(
        from,
        if (propertyPrefix.isBlank) {
          Cypher.property(Cypher.name(Neo4jQueryStrategy.VARIABLE_EVENT), Neo4jWriteMappingStrategy.KEYS, from)
        } else {
          Cypher.property(
            Cypher.name(Neo4jQueryStrategy.VARIABLE_EVENT),
            propertyPrefix,
            Neo4jWriteMappingStrategy.KEYS,
            from
          )
        }
      )
    }
  }

  // NOTE: I think this might be a bug because why would we map rel keys but not map node keys?
  // NOTE: But we still need to keep this becasue we need to refactor, that means bringing with all the bugs
  private def cypherRelationshipKeys(mappings: Seq[(String, String)]): Seq[Object] = {
    mappings.flatMap { case (to, from) =>
      Seq(
        from,
        Cypher.property(
          Cypher.name(Neo4jQueryStrategy.VARIABLE_EVENT),
          "rel",
          Neo4jWriteMappingStrategy.KEYS,
          to
        )
      )
    }
  }

  private def matcher(entity: PatternElement, saveMode: SaveMode): StatementBuilder.OngoingUpdate = {
    saveMode match {
      case SaveMode.Overwrite                       => Cypher.merge(entity)
      case SaveMode.Append | SaveMode.ErrorIfExists => Cypher.create(entity)
      case _ => throw new UnsupportedOperationException(s"SaveMode $saveMode not supported")
    }
  }

  override def createStatementForGDS(options: Neo4jOptions): String =
    throw new UnsupportedOperationException("Write operations with GDS are currently not supported")
}

class Neo4jQueryReadStrategy(
  private val neo4j: Neo4j,
  private val renderer: CypherRenderer,
  private val filters: Array[Filter] = Array.empty[Filter],
  private val partitionPagination: PartitionPagination = PartitionPagination.EMPTY,
  private val requiredColumns: Seq[String] = Seq.empty,
  private val aggregateColumns: Array[AggregateFunc] = Array.empty,
  private val jobId: String = "",
  private val withPreamble: Boolean = true
) extends Neo4jQueryStrategy with Logging {

  private val hasSkipLimit: Boolean = partitionPagination.skip != -1 && partitionPagination.topN.limit != -1

  override def createStatementForQuery(options: Neo4jOptions): String = {
    if (partitionPagination.topN.orders.nonEmpty) {
      logWarning(
        s"""Top N push-down optimizations with aggregations are not supported for custom queries.
           |\tThese aggregations are going to be ignored.
           |\tPlease specify the aggregations in the custom query directly""".stripMargin
      )
    }

    val limitedQuery = if (hasSkipLimit) {
      s"${options.query.value} SKIP ${partitionPagination.skip} LIMIT ${partitionPagination.topN.limit}"
    } else {
      s"${options.query.value}"
    }

    val scriptResult = Neo4jQueryStrategy.scriptResultClause(options)
    val preamble = if (withPreamble) fullPreamble(neo4j, options) else ""
    s"$preamble$scriptResult$limitedQuery"
  }

  override def createStatementForRelationships(options: Neo4jOptions): String = {
    val sourceNode = createNode(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS, options.relationshipMetadata.source.labels)
    val targetNode = createNode(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS, options.relationshipMetadata.target.labels)

    val relationship = sourceNode.relationshipTo(targetNode, options.relationshipMetadata.relationshipType)
      .named(Neo4jUtil.RELATIONSHIP_ALIAS)

    val matchQuery: StatementBuilder.OngoingReadingWithoutWhere =
      filterRelationship(sourceNode, targetNode, relationship)
    val returnExpressions: Seq[Expression] = buildReturnExpression(sourceNode, targetNode, relationship)
    val stmt = if (aggregateColumns.isEmpty) {
      val query = matchQuery.returning(returnExpressions: _*)
      buildStatement(options, query, relationship)
    } else {
      buildStatementAggregation(options, matchQuery, relationship, returnExpressions)
    }

    val preamble = if (withPreamble) fullPreamble(neo4j, options) else ""
    s"$preamble${renderer.render(stmt)}"
  }

  private def convertSort(entity: PropertyContainer, order: SortOrder): SortItem = {
    val sortExpression = order.expression().describe()

    val container: Option[PropertyContainer] = entity match {
      case relationship: Relationship =>
        if (sortExpression.contains(s"${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS}.")) {
          Some(relationship.getLeft)
        } else if (sortExpression.contains(s"${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS}.")) {
          Some(relationship.getRight)
        } else if (sortExpression.contains(s"${Neo4jUtil.RELATIONSHIP_ALIAS}.")) {
          Some(relationship)
        } else {
          None
        }
      case _ => Some(entity)
    }
    val direction =
      if (order.direction() == SortDirection.ASCENDING) SortItem.Direction.ASC else SortItem.Direction.DESC

    Cypher.sort(
      container
        .map(_.property(sortExpression.removeAlias()))
        .getOrElse(Cypher.name(sortExpression.unquote())),
      direction
    )
  }

  private def buildReturnExpression(sourceNode: Node, targetNode: Node, relationship: Relationship): Seq[Expression] = {
    if (requiredColumns.isEmpty) {
      Seq(
        relationship.getRequiredSymbolicName,
        sourceNode.as(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS),
        targetNode.as(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS)
      )
    } else {
      requiredColumns.map(column => {
        val splatColumn = column.split('.')
        val entityName = splatColumn.head

        val entity = if (entityName.contains(Neo4jUtil.RELATIONSHIP_ALIAS)) {
          relationship
        } else if (entityName.contains(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS)) {
          sourceNode
        } else if (entityName.contains(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS)) {
          targetNode
        } else {
          null
        }

        if (entity != null && splatColumn.length == 1) {
          entity match {
            case n: Node         => n.as(entityName.quote())
            case r: Relationship => r.getRequiredSymbolicName
          }
        } else {
          getCorrectProperty(column, entity)
        }
      })
    }
  }

  private def buildStatementAggregation(
    options: Neo4jOptions,
    query: StatementBuilder.OngoingReadingWithoutWhere,
    entity: PropertyContainer,
    fields: Seq[Expression]
  ): Statement = {
    val ret = if (hasSkipLimit) {
      val id = entity match {
        case node: Node        => Cypher.elementId(node)
        case rel: Relationship => Cypher.elementId(rel)
      }
      query
        .`with`(entity)
        // Spark does not push down limits/top N when aggregation is involved
        .orderBy(id)
        .skip(partitionPagination.skip)
        .limit(partitionPagination.topN.limit)
        .returning(fields: _*)
    } else {
      val orderByProp = options.streamingOrderBy
      if (StringUtils.isBlank(orderByProp)) {
        query.returning(fields: _*)
      } else {
        query
          .`with`(entity)
          .orderBy(entity.property(orderByProp))
          .ascending()
          .returning(fields: _*)
      }
    }
    ret.build()
  }

  private def buildStatement(
    options: Neo4jOptions,
    returning: StatementBuilder.TerminalExposesSkip
      with StatementBuilder.TerminalExposesLimit
      with StatementBuilder.TerminalExposesOrderBy
      with StatementBuilder.BuildableStatement[_],
    entity: PropertyContainer = null
  ): Statement = {

    def addSkipLimit(ret: StatementBuilder.TerminalExposesSkip
      with StatementBuilder.TerminalExposesLimit
      with StatementBuilder.BuildableStatement[_]) = {

      if (partitionPagination.skip == 0) {
        ret.limit(partitionPagination.topN.limit)
      } else {
        ret.skip(partitionPagination.skip)
          .limit(partitionPagination.topN.limit)
      }
    }

    val ret = if (entity == null) {
      if (hasSkipLimit) addSkipLimit(returning) else returning
    } else {
      if (hasSkipLimit) {
        if (options.partitions == 1 || partitionPagination.topN.orders.nonEmpty) {
          addSkipLimit(returning.orderBy(partitionPagination.topN.orders.map(order => convertSort(entity, order)): _*))
        } else {
          val id = entity match {
            case node: Node        => Cypher.elementId(node)
            case rel: Relationship => Cypher.elementId(rel)
          }
          addSkipLimit(returning.orderBy(id))
        }
      } else {
        val orderByProp = options.streamingOrderBy
        if (StringUtils.isBlank(orderByProp)) returning else returning.orderBy(entity.property(orderByProp))
      }
    }
    ret.build()
  }

  private def filterRelationship(sourceNode: Node, targetNode: Node, relationship: Relationship) = {
    val matchQuery = Cypher.`match`(sourceNode).`match`(targetNode).`match`(relationship)

    def getContainer(filter: Filter): PropertyContainer = {
      if (filter.isAttribute(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS)) {
        sourceNode
      } else if (filter.isAttribute(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS)) {
        targetNode
      } else if (filter.isAttribute(Neo4jUtil.RELATIONSHIP_ALIAS)) {
        relationship
      } else {
        throw new IllegalArgumentException(s"Attribute '${filter.getAttribute.get}' is not valid")
      }
    }

    if (filters.nonEmpty) {
      def mapFilter(filter: Filter): Condition = {
        filter match {
          case and: And => mapFilter(and.left).and(mapFilter(and.right))
          case or: Or   => mapFilter(or.left).or(mapFilter(or.right))
          case filter: Filter =>
            Neo4jUtil.mapSparkFiltersToCypher(filter, getContainer(filter), filter.getAttributeWithoutEntityName)
        }
      }

      val cypherFilters = filters.map(mapFilter)

      assembleConditionQuery(matchQuery, cypherFilters)
    }
    matchQuery
  }

  private def getCorrectProperty(column: String, entity: PropertyContainer): Expression = {
    def propertyOrSymbolicName(col: String) = {
      if (entity != null) entity.property(col) else Cypher.name(col)
    }

    column match {
      case Neo4jUtil.INTERNAL_ID_FIELD => Cypher.elementId(entity.asInstanceOf[Node]).as(Neo4jUtil.INTERNAL_ID_FIELD)
      case Neo4jUtil.INTERNAL_REL_ID_FIELD =>
        Cypher.elementId(entity.asInstanceOf[Relationship]).as(Neo4jUtil.INTERNAL_REL_ID_FIELD)
      case Neo4jUtil.INTERNAL_REL_SOURCE_ID_FIELD =>
        Cypher.elementId(entity.asInstanceOf[Node]).as(Neo4jUtil.INTERNAL_REL_SOURCE_ID_FIELD)
      case Neo4jUtil.INTERNAL_REL_TARGET_ID_FIELD =>
        Cypher.elementId(entity.asInstanceOf[Node]).as(Neo4jUtil.INTERNAL_REL_TARGET_ID_FIELD)
      case Neo4jUtil.INTERNAL_REL_TYPE_FIELD =>
        Cypher.`type`(entity.asInstanceOf[Relationship]).as(Neo4jUtil.INTERNAL_REL_TYPE_FIELD)
      case Neo4jUtil.INTERNAL_LABELS_FIELD =>
        Cypher.labels(entity.asInstanceOf[Node]).as(Neo4jUtil.INTERNAL_LABELS_FIELD)
      case Neo4jUtil.INTERNAL_REL_SOURCE_LABELS_FIELD =>
        Cypher.labels(entity.asInstanceOf[Node]).as(Neo4jUtil.INTERNAL_REL_SOURCE_LABELS_FIELD)
      case Neo4jUtil.INTERNAL_REL_TARGET_LABELS_FIELD =>
        Cypher.labels(entity.asInstanceOf[Node]).as(Neo4jUtil.INTERNAL_REL_TARGET_LABELS_FIELD)
      case "*" => Asterisk.INSTANCE
      case name => {
        val cleanedName = name.removeAlias()
        aggregateColumns.find(_.toString == name)
          .map {
            case count: Count => {
              val col = count.column().describe().unquote().removeAlias()
              val prop = propertyOrSymbolicName(col)
              if (count.isDistinct) {
                Cypher.countDistinct(prop).as(name)
              } else {
                Cypher.count(prop).as(name)
              }
            }
            case countStar: CountStar => Cypher.count(Asterisk.INSTANCE).as(name)
            case max: Max =>
              val col = max.column().describe().unquote().removeAlias()
              val prop = propertyOrSymbolicName(col)
              Cypher.max(prop).as(name)
            case min: Min =>
              val col = min.column().describe().unquote().removeAlias()
              val prop = propertyOrSymbolicName(col)
              Cypher.min(prop).as(name)
            case sum: Sum => {
              val col = sum.column().describe().unquote().removeAlias()
              val prop = propertyOrSymbolicName(col)
              if (sum.isDistinct) {
                Cypher.sumDistinct(prop).as(name)
              } else {
                Cypher.sum(prop).as(name)
              }
            }
          }
          .getOrElse(propertyOrSymbolicName(cleanedName).as(name))
          .asInstanceOf[Expression]
      }
    }
  }

  override def createStatementForNodes(options: Neo4jOptions): String = {
    val node = createNode(Neo4jUtil.NODE_ALIAS, options.nodeMetadata.labels)
    val matchQuery = filterNode(node)
    val expressions = requiredColumns.map(column => getCorrectProperty(column, node))
    val stmt = if (aggregateColumns.nonEmpty) {
      buildStatementAggregation(options, matchQuery, node, expressions)
    } else {
      val ret = if (requiredColumns.isEmpty) {
        matchQuery.returning(node)
      } else {
        matchQuery.returning(expressions: _*)
      }
      buildStatement(options, ret, node)
    }

    val preamble = if (withPreamble) fullPreamble(neo4j, options) else ""
    s"$preamble${renderer.render(stmt)}"
  }

  private def filterNode(node: Node) = {
    val matchQuery = Cypher.`match`(node)

    if (filters.nonEmpty) {
      def mapFilter(filter: Filter): Condition = {
        filter match {
          case and: And       => mapFilter(and.left).and(mapFilter(and.right))
          case or: Or         => mapFilter(or.left).or(mapFilter(or.right))
          case filter: Filter => Neo4jUtil.mapSparkFiltersToCypher(filter, node)
        }
      }

      val cypherFilters = filters.map(mapFilter)
      assembleConditionQuery(matchQuery, cypherFilters)
    }
    matchQuery
  }

  def createStatementForNodeCount(options: Neo4jOptions): String = {
    val node = createNode(Neo4jUtil.NODE_ALIAS, options.nodeMetadata.labels)
    val matchQuery = filterNode(node)
    renderer.render(buildStatement(options, matchQuery.returning(Cypher.count(node).as("count"))))
  }

  def createStatementForRelationshipCount(options: Neo4jOptions): String = {
    val sourceNode = createNode(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS, options.relationshipMetadata.source.labels)
    val targetNode = createNode(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS, options.relationshipMetadata.target.labels)

    val relationship = sourceNode.relationshipTo(targetNode, options.relationshipMetadata.relationshipType)
      .named(Neo4jUtil.RELATIONSHIP_ALIAS)

    val matchQuery: StatementBuilder.OngoingReadingWithoutWhere =
      filterRelationship(sourceNode, targetNode, relationship)

    renderer.render(buildStatement(
      options,
      matchQuery.returning(Cypher.count(sourceNode).as("count"))
    ))
  }

  private def assembleConditionQuery(
    matchQuery: StatementBuilder.OngoingReadingWithoutWhere,
    filters: Array[Condition]
  ): StatementBuilder.OngoingReadingWithWhere = {
    matchQuery.where(
      filters.fold(Cypher.noCondition()) { (a, b) => a.and(b) }
    )
  }

  private def createNode(name: String, labels: Seq[String]) = {
    val primaryLabel = labels.head
    val otherLabels = labels.tail
    if (labels.isEmpty) {
      Cypher.anyNode(name)
    } else {
      Cypher.node(primaryLabel, otherLabels.asJava).named(name)
    }
  }

  override def createStatementForGDS(options: Neo4jOptions): String = {
    if (options.tuning != Neo4jTuningOptions.empty) {
      throw new UnsupportedOperationException("Query tuning parameters are not supported for GDS queries")
    }

    val retCols = requiredColumns.map(column => getCorrectProperty(column, null))
    // we need it in order to parse the field YIELD by the GDS procedure...
    val (yieldFields, args) = Neo4jUtil.callSchemaService(
      neo4j,
      options,
      jobId,
      filters,
      { ss => (ss.struct().fieldNames, ss.inputForGDSProc(options.query.value)) }
    )

    val cypherParams = args
      .filter(t => {
        if (!t._2) {
          true
        } else {
          options.gdsMetadata.parameters.containsKey(t._1)
        }
      })
      .map(_._1)
      .map(Cypher.parameter)
    val statement = Cypher.call(options.query.value)
      .withArgs(cypherParams: _*)
      .`yield`(yieldFields: _*)
      .returning(retCols: _*)
      .build()
    renderer.render(statement)
  }
}

object Neo4jQueryStrategy {
  val VARIABLE_EVENT = "event"
  val VARIABLE_EVENTS = "events"
  val VARIABLE_SCRIPT_RESULT = "scriptResult"
  val VARIABLE_STREAM = "stream"

  def scriptResultClause(options: Neo4jOptions): String =
    if (options.script != null && options.script.nonEmpty)
      s"WITH $$scriptResult AS $VARIABLE_SCRIPT_RESULT "
    else
      ""

  val unwindEventsAsEvent: String = s"UNWIND $$$VARIABLE_EVENTS AS $VARIABLE_EVENT "

  val eventProperties: Property = Cypher.property(Cypher.name(VARIABLE_EVENT), Neo4jWriteMappingStrategy.PROPERTIES)
}

abstract class Neo4jQueryStrategy {

  def createStatementForQuery(options: Neo4jOptions): String

  def createStatementForRelationships(options: Neo4jOptions): String

  def createStatementForNodes(options: Neo4jOptions): String

  def createStatementForGDS(options: Neo4jOptions): String
}

class Neo4jQueryService(
  private val options: Neo4jOptions,
  val strategy: Neo4jQueryStrategy
) extends Serializable {

  def createQuery(): String = options.query.queryType match {
    case QueryType.LABELS       => strategy.createStatementForNodes(options)
    case QueryType.RELATIONSHIP => strategy.createStatementForRelationships(options)
    case QueryType.QUERY        => strategy.createStatementForQuery(options)
    case QueryType.GDS          => strategy.createStatementForGDS(options)
    case _ => throw new UnsupportedOperationException(
        s"""Query Type not supported.
           |You provided ${options.query.queryType},
           |supported types: ${QueryType.values.mkString(",")}""".stripMargin
      )
  }
}
