package org.neo4j.spark.service

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.{And, Filter, Or}
import org.neo4j.cypherdsl.core.StatementBuilder.{BuildableStatement, TerminalExposesLimit}
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
}

class Neo4jQueryReadStrategy(filters: Array[Filter] = Array.empty[Filter],
                             partitionSkipLimit: PartitionSkipLimit = PartitionSkipLimit.EMPTY,
                             requiredColumns: Seq[String] = Seq.empty) extends Neo4jQueryStrategy {
  private val renderer: Renderer = Renderer.getDefaultRenderer

  override def createStatementForQuery(options: Neo4jOptions): String = {
    val limitedQuery = if (partitionSkipLimit.skip != -1 && partitionSkipLimit.limit != -1) {
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

    val returnExpressions: Seq[Expression] = buildReturnExpression(sourceNode,targetNode, relationship)

    renderer.render(buildStatement(matchQuery.returning(returnExpressions : _*)))
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
          throw new IllegalArgumentException(s"`$column` is not a valid column.`")
        }

        if (splatColumn.length == 1) {
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

  private def buildStatement(returning: StatementBuilder.OngoingReadingAndReturn) =
    if (partitionSkipLimit.skip != -1 && partitionSkipLimit.limit != -1) {
      returning.skip[TerminalExposesLimit with BuildableStatement](partitionSkipLimit.skip)
        .limit(partitionSkipLimit.limit)
        .build()
    } else {
      returning.build()
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

  private def returnRequiredColumns(entity: PropertyContainer, matchQuery: StatementBuilder.OngoingReading): StatementBuilder.OngoingReadingAndReturn = {
    if (requiredColumns.isEmpty) {
      matchQuery.returning(entity)
    }
    else {
      matchQuery.returning(requiredColumns.map(column => getCorrectProperty(column, entity)): _*)
    }
  }

  private def getCorrectProperty(column: String, entity: PropertyContainer): Expression = {
    column match {
      case Neo4jUtil.INTERNAL_ID_FIELD => Functions.id(entity.asInstanceOf[Node]).as(Neo4jUtil.INTERNAL_ID_FIELD)
      case Neo4jUtil.INTERNAL_REL_ID_FIELD => Functions.id(entity.asInstanceOf[Relationship]).as(Neo4jUtil.INTERNAL_REL_ID_FIELD)
      case Neo4jUtil.INTERNAL_REL_SOURCE_ID_FIELD => Functions.id(entity.asInstanceOf[Node]).as(Neo4jUtil.INTERNAL_REL_SOURCE_ID_FIELD)
      case Neo4jUtil.INTERNAL_REL_TARGET_ID_FIELD => Functions.id(entity.asInstanceOf[Node]).as(Neo4jUtil.INTERNAL_REL_TARGET_ID_FIELD)
      case Neo4jUtil.INTERNAL_REL_TYPE_FIELD => Functions.`type`(entity.asInstanceOf[Relationship]).as(Neo4jUtil.INTERNAL_REL_TYPE_FIELD)
      case Neo4jUtil.INTERNAL_LABELS_FIELD => Functions.labels(entity.asInstanceOf[Node]).as(Neo4jUtil.INTERNAL_LABELS_FIELD)
      case name => entity.property(name.removeAlias()).as(name)
    }
  }

  override def createStatementForNodes(options: Neo4jOptions): String = {
    val node = createNode(Neo4jUtil.NODE_ALIAS, options.nodeMetadata.labels)
    val matchQuery = filterNode(node)
    val returning = returnRequiredColumns(node, matchQuery)
    renderer.render(buildStatement(returning))
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
    renderer.render(buildStatement(matchQuery.returning(Functions.count(node).as("count"))))
  }

  def createStatementForRelationshipCount(options: Neo4jOptions): String = {
    val sourceNode = createNode(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS, options.relationshipMetadata.source.labels)
    val targetNode = createNode(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS, options.relationshipMetadata.target.labels)

    val relationship = sourceNode.relationshipTo(targetNode, options.relationshipMetadata.relationshipType)
      .named(Neo4jUtil.RELATIONSHIP_ALIAS)

    val matchQuery: StatementBuilder.OngoingReadingWithoutWhere = filterRelationship(sourceNode, targetNode, relationship)

    renderer.render(buildStatement(matchQuery.returning(Functions.count(sourceNode).as("count"))))
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
}

class Neo4jQueryService(private val options: Neo4jOptions,
                        val strategy: Neo4jQueryStrategy) extends Serializable {

  def createQuery(): String = options.query.queryType match {
    case QueryType.LABELS => strategy.createStatementForNodes(options)
    case QueryType.RELATIONSHIP => strategy.createStatementForRelationships(options)
    case QueryType.QUERY => strategy.createStatementForQuery(options)
    case _ => throw new UnsupportedOperationException(s"""Query Type not supported.
         |You provided ${options.query.queryType},
         |supported types: ${QueryType.values.mkString(",")}""".stripMargin)
  }
}

case class Neo4jSparkFieldMapping(neo4jFieldName: String, sparkFieldName: String)
