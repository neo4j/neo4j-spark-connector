package org.neo4j.spark.service

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Count, CountStar, Max, Min, Sum}
import org.apache.spark.sql.sources.{And, Filter, Or}
import org.neo4j.cypherdsl.core._
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.spark.util.Neo4jImplicits._
import org.neo4j.spark.util.{Neo4jOptions, Neo4jUtil, NodeSaveMode, QueryType}

import scala.collection.JavaConverters._

class Neo4jQueryWriteStrategy(private val saveMode: SaveMode) extends Neo4jQueryStrategy {
  override def createStatementForQuery(options: Neo4jOptions): String =
    s"""WITH ${"$"}scriptResult AS ${Neo4jQueryStrategy.VARIABLE_SCRIPT_RESULT}
       |UNWIND ${"$"}events AS ${Neo4jQueryStrategy.VARIABLE_EVENT}
       |${options.query.value}
       |""".stripMargin

  private def createPropsList(props: Map[String, String], prefix: String): String = {
    props
      .map(key => {
        s"${key._2.quote()}: ${Neo4jQueryStrategy.VARIABLE_EVENT}.$prefix.${key._2.quote()}"
      }).mkString(", ")
  }

  private def keywordFromSaveMode(saveMode: Any): String = {
    saveMode match {
      case NodeSaveMode.Overwrite | SaveMode.Overwrite => "MERGE"
      case NodeSaveMode.ErrorIfExists | SaveMode.ErrorIfExists | SaveMode.Append | NodeSaveMode.Append => "CREATE"
      case NodeSaveMode.Match => "MATCH"
      case _ => throw new UnsupportedOperationException(s"SaveMode $saveMode not supported")
    }
  }

  private def createQueryPart(keyword: String, labels: String, keys: String, alias: String): String = {
    val setStatement = if (!keyword.equals("MATCH")) s" SET $alias += ${Neo4jQueryStrategy.VARIABLE_EVENT}.$alias.${Neo4jWriteMappingStrategy.PROPERTIES}" else ""
    s"""$keyword ($alias${if (labels.isEmpty) "" else s":$labels"} ${if (keys.isEmpty) "" else s"{$keys}"})$setStatement""".stripMargin
  }

  override def createStatementForRelationships(options: Neo4jOptions): String = {
    val relationshipKeyword = keywordFromSaveMode(saveMode)
    val sourceKeyword = keywordFromSaveMode(options.relationshipMetadata.sourceSaveMode)
    val targetKeyword = keywordFromSaveMode(options.relationshipMetadata.targetSaveMode)

    val relationship = options.relationshipMetadata.relationshipType.quote()

    val sourceLabels = options.relationshipMetadata.source.labels
      .map(_.quote())
      .mkString(":")

    val targetLabels = options.relationshipMetadata.target.labels
      .map(_.quote())
      .mkString(":")

    val sourceKeys = createPropsList(
      options.relationshipMetadata.source.nodeKeys,
      s"source.${Neo4jWriteMappingStrategy.KEYS}"
    )
    val targetKeys = createPropsList(
      options.relationshipMetadata.target.nodeKeys,
      s"target.${Neo4jWriteMappingStrategy.KEYS}"
    )
    val sourceQueryPart = createQueryPart(sourceKeyword, sourceLabels, sourceKeys, Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS)
    val targetQueryPart = createQueryPart(targetKeyword, targetLabels, targetKeys, Neo4jUtil.RELATIONSHIP_TARGET_ALIAS)

    val withQueryPart = if (sourceKeyword != "MATCH" && targetKeyword == "MATCH")
      "\nWITH source, event"
    else {
      ""
    }

    s"""UNWIND ${"$"}events AS ${Neo4jQueryStrategy.VARIABLE_EVENT}
       |$sourceQueryPart$withQueryPart
       |$targetQueryPart
       |$relationshipKeyword (${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS})-[${Neo4jUtil.RELATIONSHIP_ALIAS}:$relationship]->(${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS})
       |SET ${Neo4jUtil.RELATIONSHIP_ALIAS} += ${Neo4jQueryStrategy.VARIABLE_EVENT}.${Neo4jUtil.RELATIONSHIP_ALIAS}.${Neo4jWriteMappingStrategy.PROPERTIES}
       |""".stripMargin
  }

  override def createStatementForNodes(options: Neo4jOptions): String = {
    val keyword = keywordFromSaveMode(saveMode)

    val labels = options.nodeMetadata.labels
      .map(_.quote())
      .mkString(":")

    val keys = createPropsList(
      options.nodeMetadata.nodeKeys,
      Neo4jWriteMappingStrategy.KEYS
    )

    s"""UNWIND ${"$"}events AS ${Neo4jQueryStrategy.VARIABLE_EVENT}
       |$keyword (node${if (labels.isEmpty) "" else s":$labels"} ${if (keys.isEmpty) "" else s"{$keys}"})
       |SET node += ${Neo4jQueryStrategy.VARIABLE_EVENT}.${Neo4jWriteMappingStrategy.PROPERTIES}
       |""".stripMargin
  }

  override def createStatementForGDS(options: Neo4jOptions): String =
    throw new UnsupportedOperationException("Write operations with GDS are currently not supported")
}

class Neo4jQueryReadStrategy(filters: Array[Filter] = Array.empty[Filter],
                             partitionSkipLimit: PartitionSkipLimit = PartitionSkipLimit.EMPTY,
                             requiredColumns: Seq[String] = Seq.empty,
                             aggregateColumns: Array[AggregateFunc] = Array.empty,
                             jobId: String = "") extends Neo4jQueryStrategy {
  private val renderer: Renderer = Renderer.getDefaultRenderer

  private val hasPartitons: Boolean = partitionSkipLimit.skip != -1 && partitionSkipLimit.limit != -1

  override def createStatementForQuery(options: Neo4jOptions): String = {
    val limitedQuery = if (hasPartitons) {
      s"""${options.query.value}
         |SKIP ${partitionSkipLimit.skip} LIMIT ${partitionSkipLimit.limit}
         |""".stripMargin
    } else {
      options.query.value
    }
    s"""WITH ${"$"}scriptResult AS ${Neo4jQueryStrategy.VARIABLE_SCRIPT_RESULT}
       |$limitedQuery""".stripMargin
  }

  override def createStatementForRelationships(options: Neo4jOptions): String = {
    val sourceNode = createNode(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS, options.relationshipMetadata.source.labels)
    val targetNode = createNode(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS, options.relationshipMetadata.target.labels)

    val relationship = sourceNode.relationshipTo(targetNode, options.relationshipMetadata.relationshipType)
      .named(Neo4jUtil.RELATIONSHIP_ALIAS)

    val matchQuery: StatementBuilder.OngoingReadingWithoutWhere = filterRelationship(sourceNode, targetNode, relationship)

    val returnExpressions: Seq[Expression] = buildReturnExpression(sourceNode, targetNode, relationship)
    val stmt = if (aggregateColumns.isEmpty) {
      buildStatement(options, matchQuery.returning(returnExpressions : _*), relationship)
    } else {
      buildStatementAggregation(options, matchQuery, relationship, returnExpressions)
    }
    renderer.render(stmt)
  }

  private def buildReturnExpression(sourceNode: Node, targetNode: Node, relationship: Relationship): Seq[Expression] = {
    if (requiredColumns.isEmpty) {
      Seq(relationship.getRequiredSymbolicName, sourceNode.as(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS), targetNode.as(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS))
    }
    else {
      requiredColumns.map(column => {
        val splatColumn = column.split('.')
        val entityName = splatColumn.head

        val entity = if (entityName.contains(Neo4jUtil.RELATIONSHIP_ALIAS)) {
          relationship
        }
        else if (entityName.contains(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS)) {
          sourceNode
        }
        else if (entityName.contains(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS)) {
          targetNode
        }
        else {
          null
        }

        if (entity != null && splatColumn.length == 1) {
          entity match {
            case n: Node => n.as(entityName.quote())
            case r: Relationship => r.getRequiredSymbolicName
          }
        }
        else {
          getCorrectProperty(column, entity)
        }
      })
    }
  }

  private def buildStatementAggregation(options: Neo4jOptions,
                                        query: StatementBuilder.OngoingReadingWithoutWhere,
                                        entity: PropertyContainer,
                                        fields: Seq[Expression]): Statement = {
    val ret = if (hasPartitons) {
      val id = entity match {
        case node: Node => Functions.id(node)
        case rel: Relationship => Functions.id(rel)
      }
      query
        .`with`(entity)
        .orderBy(id)
        .skip(partitionSkipLimit.skip)
        .limit(partitionSkipLimit.limit)
        .returning(fields: _*)
    } else {
      val orderByProp = options.orderBy
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

  private def buildStatement(options: Neo4jOptions,
                             returning: StatementBuilder.OngoingReadingAndReturn,
                             entity: PropertyContainer = null): Statement = {

    def addSkipLimit(ret: StatementBuilder.TerminalExposesSkip
        with StatementBuilder.TerminalExposesLimit
        with StatementBuilder.BuildableStatement[_]) = ret
      .skip(partitionSkipLimit.skip).asInstanceOf[StatementBuilder.TerminalExposesLimit].limit(partitionSkipLimit.limit)

    val ret = if (entity == null) {
      if (hasPartitons) addSkipLimit(returning) else returning
    } else {
      if (hasPartitons) {
        val id = entity match {
          case node: Node => Functions.id(node)
          case rel: Relationship => Functions.id(rel)
        }
        addSkipLimit(returning.orderBy(id))
      } else {
        val orderByProp = options.orderBy
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
      }
      else if (filter.isAttribute(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS)) {
        targetNode
      }
      else if (filter.isAttribute(Neo4jUtil.RELATIONSHIP_ALIAS)) {
        relationship
      }
      else {
        throw new IllegalArgumentException(s"Attribute '${filter.getAttribute.get}' is not valid")
      }
    }

    if (filters.nonEmpty) {
      def mapFilter(filter: Filter): Condition = {
        filter match {
          case and: And => mapFilter(and.left).and(mapFilter(and.right))
          case or: Or => mapFilter(or.left).or(mapFilter(or.right))
          case filter: Filter => Neo4jUtil.mapSparkFiltersToCypher(filter, getContainer(filter), filter.getAttributeWithoutEntityName)
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
      case Neo4jUtil.INTERNAL_ID_FIELD => Functions.id(entity.asInstanceOf[Node]).as(Neo4jUtil.INTERNAL_ID_FIELD)
      case Neo4jUtil.INTERNAL_REL_ID_FIELD => Functions.id(entity.asInstanceOf[Relationship]).as(Neo4jUtil.INTERNAL_REL_ID_FIELD)
      case Neo4jUtil.INTERNAL_REL_SOURCE_ID_FIELD => Functions.id(entity.asInstanceOf[Node]).as(Neo4jUtil.INTERNAL_REL_SOURCE_ID_FIELD)
      case Neo4jUtil.INTERNAL_REL_TARGET_ID_FIELD => Functions.id(entity.asInstanceOf[Node]).as(Neo4jUtil.INTERNAL_REL_TARGET_ID_FIELD)
      case Neo4jUtil.INTERNAL_REL_TYPE_FIELD => Functions.`type`(entity.asInstanceOf[Relationship]).as(Neo4jUtil.INTERNAL_REL_TYPE_FIELD)
      case Neo4jUtil.INTERNAL_LABELS_FIELD => Functions.labels(entity.asInstanceOf[Node]).as(Neo4jUtil.INTERNAL_LABELS_FIELD)
      case Neo4jUtil.INTERNAL_REL_SOURCE_LABELS_FIELD => Functions.labels(entity.asInstanceOf[Node]).as(Neo4jUtil.INTERNAL_REL_SOURCE_LABELS_FIELD)
      case Neo4jUtil.INTERNAL_REL_TARGET_LABELS_FIELD => Functions.labels(entity.asInstanceOf[Node]).as(Neo4jUtil.INTERNAL_REL_TARGET_LABELS_FIELD)
      case "*" => Asterisk.INSTANCE
      case name => {
        val cleanedName = name.removeAlias()
        aggregateColumns.find(_.toString == name)
          .map {
            case count: Count => {
              val col = count.column().describe().unquote().removeAlias()
              val prop = propertyOrSymbolicName(col)
              if (count.isDistinct) {
                Functions.countDistinct(prop).as(name)
              } else {
                Functions.count(prop).as(name)
              }
            }
            case countStar: CountStar => Functions.count(Asterisk.INSTANCE).as(name)
            case max: Max =>
              val col = max.column().describe().unquote().removeAlias()
              val prop = propertyOrSymbolicName(col)
              Functions.max(prop).as(name)
            case min: Min =>
              val col = min.column().describe().unquote().removeAlias()
              val prop = propertyOrSymbolicName(col)
              Functions.min(prop).as(name)
            case sum: Sum => {
              val col = sum.column().describe().unquote().removeAlias()
              val prop = propertyOrSymbolicName(col)
              if (sum.isDistinct) {
                Functions.sumDistinct(prop).as(name)
              } else {
                Functions.sum(prop).as(name)
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
        matchQuery.returning(expressions : _*)
      }
      buildStatement(options, ret, node)
    }
    renderer.render(stmt)
  }

  private def filterNode(node: Node) = {
    val matchQuery = Cypher.`match`(node)

    if (filters.nonEmpty) {
      def mapFilter(filter: Filter): Condition = {
        filter match {
          case and: And => mapFilter(and.left).and(mapFilter(and.right))
          case or: Or => mapFilter(or.left).or(mapFilter(or.right))
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
    renderer.render(buildStatement(options, matchQuery.returning(Functions.count(node).as("count"))))
  }

  def createStatementForRelationshipCount(options: Neo4jOptions): String = {
    val sourceNode = createNode(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS, options.relationshipMetadata.source.labels)
    val targetNode = createNode(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS, options.relationshipMetadata.target.labels)

    val relationship = sourceNode.relationshipTo(targetNode, options.relationshipMetadata.relationshipType)
      .named(Neo4jUtil.RELATIONSHIP_ALIAS)

    val matchQuery: StatementBuilder.OngoingReadingWithoutWhere = filterRelationship(sourceNode, targetNode, relationship)

    renderer.render(buildStatement(options, matchQuery.returning(Functions.count(sourceNode).as("count"))))
  }

  private def assembleConditionQuery(matchQuery: StatementBuilder.OngoingReadingWithoutWhere, filters: Array[Condition]): StatementBuilder.OngoingReadingWithWhere = {
    matchQuery.where(
      filters.fold(Conditions.noCondition()) { (a, b) => a.and(b) }
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
    val retCols = requiredColumns.map(column => getCorrectProperty(column, null))
    // we need it in order to parse the field YIELD by the GDS procedure...
    val (yieldFields, args) = Neo4jUtil.callSchemaService(options, jobId, filters,
      { ss => (ss.struct().fieldNames, ss.inputForGDSProc(options.query.value)) })

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
      .withArgs(cypherParams : _*)
      .`yield`(yieldFields : _*)
      .returning(retCols : _*)
      .build()
    renderer.render(statement)
  }
}

object Neo4jQueryStrategy {
  val VARIABLE_EVENT = "event"
  val VARIABLE_EVENTS = "events"
  val VARIABLE_SCRIPT_RESULT = "scriptResult"
  val VARIABLE_STREAM = "stream"
}

abstract class Neo4jQueryStrategy {

  def createStatementForQuery(options: Neo4jOptions): String

  def createStatementForRelationships(options: Neo4jOptions): String

  def createStatementForNodes(options: Neo4jOptions): String

  def createStatementForGDS(options: Neo4jOptions): String
}

class Neo4jQueryService(private val options: Neo4jOptions,
                        val strategy: Neo4jQueryStrategy) extends Serializable {

  def createQuery(): String = options.query.queryType match {
    case QueryType.LABELS => strategy.createStatementForNodes(options)
    case QueryType.RELATIONSHIP => strategy.createStatementForRelationships(options)
    case QueryType.QUERY => strategy.createStatementForQuery(options)
    case QueryType.GDS => strategy.createStatementForGDS(options)
    case _ => throw new UnsupportedOperationException(s"""Query Type not supported.
         |You provided ${options.query.queryType},
         |supported types: ${QueryType.values.mkString(",")}""".stripMargin)
  }
}
