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
import org.neo4j.spark.cypher.QueryEmbedder
import org.neo4j.spark.service.Neo4jQueryStrategy.VARIABLE_EVENT
import org.neo4j.spark.service.Neo4jQueryStrategy.VARIABLE_EVENTS
import org.neo4j.spark.service.Neo4jQueryStrategy.eventProperties
import org.neo4j.spark.service.Neo4jQueryStrategy.relEventProperties
import org.neo4j.spark.service.Neo4jQueryStrategy.scriptResultClause
import org.neo4j.spark.service.Neo4jQueryStrategy.unwindEventsAsEvent
import org.neo4j.spark.service.Neo4jWriteMappingStrategy.PROPERTIES
import org.neo4j.spark.util.Neo4jImplicits._
import org.neo4j.spark.util.Neo4jNodeMetadata
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.Neo4jRelationshipMetadata
import org.neo4j.spark.util.Neo4jUtil
import org.neo4j.spark.util.Neo4jUtil.RELATIONSHIP_ALIAS
import org.neo4j.spark.util.Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS
import org.neo4j.spark.util.Neo4jUtil.RELATIONSHIP_TARGET_ALIAS
import org.neo4j.spark.util.NodeSaveMode
import org.neo4j.spark.util.QueryType

import scala.jdk.CollectionConverters.SeqHasAsJava

class Neo4jQueryWriteStrategy(
  private val neo4j: Neo4j,
  private val renderer: CypherRenderer,
  private val embedder: QueryEmbedder,
  private val saveMode: SaveMode,
  private val withPreamble: Boolean = true
) extends Neo4jQueryStrategy {

  override def createStatementForQuery(options: Neo4jOptions): String = {
    val preamble = if (withPreamble) fullPreamble(neo4j, options) else ""
    val scriptResult = scriptResultClause(options)

    val statement = embedder.embedWrite(options.query.value, VARIABLE_EVENTS, VARIABLE_EVENT, scriptResult)
    s"$preamble${renderer.render(statement.build())}"
  }

  override def createStatementForRelationships(options: Neo4jOptions): String = {
    val sourceNode = cypherNode(options.relationshipMetadata.source, RELATIONSHIP_SOURCE_ALIAS, prefix = true)
    val sourcePropsName = Cypher.property(Cypher.name(VARIABLE_EVENT), RELATIONSHIP_SOURCE_ALIAS, PROPERTIES)

    val targetNode = cypherNode(options.relationshipMetadata.target, RELATIONSHIP_TARGET_ALIAS, prefix = true)
    val targetPropsName = Cypher.property(Cypher.name(VARIABLE_EVENT), RELATIONSHIP_TARGET_ALIAS, PROPERTIES)

    val rel = cypherRelationship(sourceNode, targetNode, options.relationshipMetadata, RELATIONSHIP_ALIAS)

    val isSourceMatch = options.relationshipMetadata.sourceSaveMode == NodeSaveMode.Match
    val isTargetMatch = options.relationshipMetadata.targetSaveMode == NodeSaveMode.Match

    val sourceMatcher = options.relationshipMetadata.sourceSaveMode match {
      case NodeSaveMode.Overwrite => Cypher.merge(sourceNode).mutate(sourceNode, sourcePropsName)
      case NodeSaveMode.Append    => Cypher.create(sourceNode).mutate(sourceNode, sourcePropsName)
      case NodeSaveMode.Match     => Cypher.`match`(sourceNode)
    }

    val nodesMatcher = if (!isSourceMatch && isTargetMatch) {
      val sourceWith = sourceMatcher.`with`(Cypher.name(RELATIONSHIP_SOURCE_ALIAS), Cypher.name(VARIABLE_EVENT))

      options.relationshipMetadata.targetSaveMode match {
        case NodeSaveMode.Overwrite => sourceWith.merge(targetNode).mutate(targetNode, targetPropsName)
        case NodeSaveMode.Append    => sourceWith.create(targetNode).mutate(targetNode, targetPropsName)
        case NodeSaveMode.Match     => sourceWith.`match`(targetNode)
      }
    } else {
      options.relationshipMetadata.targetSaveMode match {
        case NodeSaveMode.Overwrite              => sourceMatcher.merge(targetNode).mutate(targetNode, targetPropsName)
        case NodeSaveMode.Append                 => sourceMatcher.create(targetNode).mutate(targetNode, targetPropsName)
        case NodeSaveMode.Match if isSourceMatch => Cypher.`match`(sourceNode).`match`(targetNode)
        case _ => throw new IllegalStateException("Impossible query state reached. Please report this as a bug.")
      }
    }

    val finalStatement = (saveMode match {
      case SaveMode.Overwrite => nodesMatcher.merge(rel)
      case SaveMode.Append    => nodesMatcher.create(rel)
      case _                  => throw new UnsupportedOperationException(s"SaveMode $saveMode not supported")
    })
      .mutate(rel, relEventProperties)
      .build()

    val preamble = if (withPreamble) fullPreamble(neo4j, options) else ""
    preamble + unwindEventsAsEvent + renderer.render(finalStatement)
  }

  override def createStatementForNodes(options: Neo4jOptions): String = {
    val node = cypherNode(options.nodeMetadata, "node")

    val nodeMatcher = saveMode match {
      case SaveMode.Overwrite => Cypher.merge(node)
      case SaveMode.Append    => Cypher.create(node)
      case _                  => throw new UnsupportedOperationException(s"SaveMode $saveMode not supported")
    }

    val preamble = if (withPreamble) fullPreamble(neo4j, options) else ""
    val query = renderer.render(nodeMatcher.mutate(node, eventProperties).build())

    preamble + unwindEventsAsEvent + query
  }

  private def cypherNode(nodeData: Neo4jNodeMetadata, alias: String, prefix: Boolean = false): Node = {
    val propertyPrefix = if (prefix) alias else StringUtils.EMPTY
    val node = Cypher.node(nodeData.labels.head, nodeData.labels.tail: _*).named(alias)
    val keyProperties = cypherNodeKeys(nodeData.nodeKeys.toSeq, propertyPrefix)

    if (keyProperties.nonEmpty) {
      node.withProperties(keyProperties: _*)
    } else {
      node
    }
  }

  private def cypherRelationship(
    source: Node,
    target: Node,
    relationshipData: Neo4jRelationshipMetadata,
    alias: String
  ): Relationship = {
    val rel = source.relationshipTo(target, relationshipData.relationshipType).named(Cypher.name(alias))
    val keyProperties = cypherRelationshipKeys(relationshipData.relationshipKeys.toSeq)

    if (keyProperties.nonEmpty) {
      rel.withProperties(keyProperties: _*)
    } else {
      rel
    }
  }

  private def cypherNodeKeys(mappings: Seq[(String, String)], propertyPrefix: String): Seq[Object] = {
    mappings.flatMap { case (_, from) =>
      Seq(
        from,
        if (propertyPrefix.isBlank) {
          Cypher.property(Cypher.name(VARIABLE_EVENT), Neo4jWriteMappingStrategy.KEYS, from)
        } else {
          Cypher.property(
            Cypher.name(VARIABLE_EVENT),
            propertyPrefix,
            Neo4jWriteMappingStrategy.KEYS,
            from
          )
        }
      )
    }
  }

  private def cypherRelationshipKeys(mappings: Seq[(String, String)]): Seq[Object] = {
    mappings.flatMap { case (to, from) =>
      Seq(
        from,
        Cypher.property(
          Cypher.name(VARIABLE_EVENT),
          "rel",
          Neo4jWriteMappingStrategy.KEYS,
          to
        )
      )
    }
  }

  override def createStatementForGDS(options: Neo4jOptions): String =
    throw new UnsupportedOperationException("Write operations with GDS are currently not supported")
}

class Neo4jQueryReadStrategy(
  private val neo4j: Neo4j,
  private val renderer: CypherRenderer,
  private val queryEmbedder: QueryEmbedder,
  private val filters: Array[Filter] = Array.empty[Filter],
  private val partitionPagination: PartitionPagination = PartitionPagination.EMPTY,
  private val requiredColumns: Seq[String] = Seq.empty,
  private val aggregateColumns: Array[AggregateFunc] = Array.empty,
  private val jobId: String = "",
  private val withPreamble: Boolean = true
) extends Neo4jQueryStrategy with Logging {

  private val hasSkipLimit: Boolean = partitionPagination.skip != -1 && partitionPagination.topN.limit != -1

  override def createStatementForQuery(options: Neo4jOptions): String = {
    val scriptResult = scriptResultClause(options)
    var statement: StatementBuilder.BuildableStatement[ResultStatement] =
      queryEmbedder.embedRead(options.query.value, scriptResult)
    if (partitionPagination.topN.orders.nonEmpty) {
      statement = statement
        .asInstanceOf[StatementBuilder.TerminalExposesOrderBy]
        .orderBy(partitionPagination.topN
          .orders
          .map(order => {
            convertSort(order)
          })
          .toSeq
          .asJava)
    }
    if (partitionPagination.skip != -1) {
      statement = statement.asInstanceOf[StatementBuilder.TerminalExposesSkip]
        .skip(partitionPagination.skip)
    }
    if (partitionPagination.topN.limit != -1) {
      statement = statement.asInstanceOf[StatementBuilder.TerminalExposesLimit]
        .limit(partitionPagination.topN.limit)
    }

    val preamble = if (withPreamble) fullPreamble(neo4j, options) else ""
    s"$preamble${renderer.render(statement.build())}"
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

    val container: Option[(PropertyContainer, Option[String])] = entity match {
      case relationship: Relationship => entityAlias(sortExpression)
          .map(alias => (relationshipContainer(alias, relationship), Some(alias)))
      case _ => Some((entity, None))
    }

    Cypher.sort(
      container
        .map { case (propertyContainer, alias) =>
          Neo4jUtil.getCorrectProperty(propertyContainer, sortExpression, alias)
        }
        .getOrElse(Cypher.name(sortExpression.unquote())),
      direction(order)
    )
  }

  private def convertSort(order: SortOrder): SortItem = {
    Cypher.sort(Cypher.name(order.expression().describe().unquote()), direction(order))
  }

  private def direction(order: SortOrder): SortItem.Direction = {
    if (order.direction() == SortDirection.ASCENDING) SortItem.Direction.ASC else SortItem.Direction.DESC
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
        // an aggregation is named after the function, e.g. `SUM(``target.price``)`, so the entity
        // it belongs to has to be resolved from the column the aggregation is computed on
        val alias = columnEntityAlias(aggregatedColumn(column).getOrElse(column))

        val entity = alias
          .map {
            case Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS => sourceNode
            case Neo4jUtil.RELATIONSHIP_TARGET_ALIAS => targetNode
            case _                                   => relationship
          }
          .orNull

        val name = column.unquote()
        if (entity != null && alias.exists(a => name == a || name == s"<$a>")) {
          entity match {
            case n: Node         => n.as(column.sanitizeSchemaName())
            case r: Relationship => r.getRequiredSymbolicName
          }
        } else {
          getCorrectProperty(column, entity, alias)
        }
      })
    }
  }

  private val relationshipAliases =
    Seq(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS, Neo4jUtil.RELATIONSHIP_TARGET_ALIAS, Neo4jUtil.RELATIONSHIP_ALIAS)

  /**
   * The column an aggregation is computed on, e.g. `` `target.price` `` for
   * `` SUM(`target.price`) ``. Aggregations over the whole row, like `COUNT(*)`, have none.
   */
  private def aggregatedColumn(column: String): Option[String] = aggregateColumns
    .find(_.toString == column)
    .flatMap {
      case count: Count => Some(count.column().describe())
      case max: Max     => Some(max.column().describe())
      case min: Min     => Some(min.column().describe())
      case sum: Sum     => Some(sum.column().describe())
      case _            => None
    }

  /**
   * Returns the alias of the relationship entity the given column is prefixed with, if any.
   */
  private def entityAlias(column: String): Option[String] = relationshipAliases.find(column.hasEntityAlias)

  /**
   * Same as [[entityAlias]], but it also recognises the internal fields, which wrap the alias in
   * angle brackets, e.g. `<source.elementId>` or `<source>`.
   */
  private def columnEntityAlias(column: String): Option[String] = {
    val name = column.unquote().stripPrefix("<")
    relationshipAliases.find(alias => name == alias || name == s"$alias>" || name.hasEntityAlias(alias))
  }

  private def relationshipContainer(alias: String, relationship: Relationship): PropertyContainer = alias match {
    case Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS => relationship.getLeft
    case Neo4jUtil.RELATIONSHIP_TARGET_ALIAS => relationship.getRight
    case _                                   => relationship
  }

  private def buildStatementAggregation(
    options: Neo4jOptions,
    query: StatementBuilder.OngoingReadingWithoutWhere,
    entity: PropertyContainer,
    fields: Seq[Expression]
  ): Statement = {
    val ret = if (hasSkipLimit) {
      val id: FunctionInvocation = entity match {
        case node: Node        => Cypher.elementId(node)
        case rel: Relationship => Cypher.elementId(rel)
      }

      val statement = query.`with`(entity)
      val orderedStatement: StatementBuilder.ExposesSkip = if (partitionPagination.topN.orders.nonEmpty) {
        statement.orderBy(partitionPagination.topN.orders.map(order => convertSort(entity, order)): _*)
      } else {
        statement.orderBy(id)
      }

      orderedStatement
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

    def getAlias(filter: Filter): String = relationshipAliases
      .find(filter.hasEntityAlias)
      .getOrElse(throw new IllegalArgumentException(s"Attribute '${filter.getAttribute.get}' is not valid"))

    def getContainer(alias: String): PropertyContainer = alias match {
      case Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS => sourceNode
      case Neo4jUtil.RELATIONSHIP_TARGET_ALIAS => targetNode
      case _                                   => relationship
    }

    if (filters.nonEmpty) {
      def mapFilter(filter: Filter): Condition = {
        filter match {
          case and: And => mapFilter(and.left).and(mapFilter(and.right))
          case or: Or   => mapFilter(or.left).or(mapFilter(or.right))
          case filter: Filter =>
            val alias = getAlias(filter)
            Neo4jUtil.mapSparkFiltersToCypher(filter, getContainer(alias), Some(alias))
        }
      }

      val cypherFilters = filters.map(mapFilter)

      assembleConditionQuery(matchQuery, cypherFilters)
    }
    matchQuery
  }

  /**
   * Builds the projection for a required column.
   *
   * `alias` must be set only when the column is prefixed with the alias of the entity it belongs
   * to, as it happens for relationship reads: node, query and GDS columns carry no alias, so a dot
   * there is part of the property name and must be kept.
   */
  private def getCorrectProperty(
    column: String,
    entity: PropertyContainer,
    alias: Option[String] = None
  ): Expression = {
    def propertyOrSymbolicName(col: String) = {
      if (entity != null) entity.property(col) else Cypher.name(col)
    }

    def withoutAlias(col: String) = alias.map(col.removeEntityAlias).getOrElse(col.unquote())

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
        aggregateColumns.find(_.toString == name)
          .map {
            case count: Count => {
              val prop = propertyOrSymbolicName(withoutAlias(count.column().describe()))
              if (count.isDistinct) {
                Cypher.countDistinct(prop).as(name)
              } else {
                Cypher.count(prop).as(name)
              }
            }
            case countStar: CountStar => Cypher.count(Asterisk.INSTANCE).as(name)
            case max: Max => Cypher.max(propertyOrSymbolicName(withoutAlias(max.column().describe()))).as(name)
            case min: Min => Cypher.min(propertyOrSymbolicName(withoutAlias(min.column().describe()))).as(name)
            case sum: Sum => {
              val prop = propertyOrSymbolicName(withoutAlias(sum.column().describe()))
              if (sum.isDistinct) {
                Cypher.sumDistinct(prop).as(name)
              } else {
                Cypher.sum(prop).as(name)
              }
            }
          }
          .getOrElse(propertyOrSymbolicName(withoutAlias(name)).as(name))
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
    if (options.tuning.nonEmpty) {
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
      s"WITH $$$VARIABLE_SCRIPT_RESULT AS $VARIABLE_SCRIPT_RESULT "
    else
      ""

  val unwindEventsAsEvent: String = s"UNWIND $$$VARIABLE_EVENTS AS $VARIABLE_EVENT "

  val eventProperties: Property = Cypher.property(Cypher.name(VARIABLE_EVENT), PROPERTIES)

  val relEventProperties: Property = Cypher.property(Cypher.name(VARIABLE_EVENT), RELATIONSHIP_ALIAS, PROPERTIES)
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
