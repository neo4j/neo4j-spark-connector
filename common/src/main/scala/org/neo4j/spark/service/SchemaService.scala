package org.neo4j.spark.service

import java.util
import java.util.Collections
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.types.Entity
import org.neo4j.driver.{Record, Session, Transaction, TransactionWork, Value, summary}
import org.neo4j.spark.service.SchemaService.{cypherToSparkType, normalizedClassName, normalizedClassNameFromGraphEntity}
import org.neo4j.spark.util.Neo4jImplicits.{CypherImplicits, EntityImplicits}
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Neo4jUtil, OptimizationType, QueryType, SchemaStrategy, ValidationUtil}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object PartitionSkipLimit {
  val EMPTY = PartitionSkipLimit(0, -1, -1)
}

case class PartitionSkipLimit(partitionNumber: Int, skip: Long, limit: Long)

case class Neo4jVersion(name: String, versions: Seq[String], edition: String)

class SchemaService(private val options: Neo4jOptions, private val driverCache: DriverCache, private val filters: Array[Filter] = Array.empty)
  extends AutoCloseable with Logging {

  private val queryReadStrategy = new Neo4jQueryReadStrategy(filters)

  private val session: Session = driverCache.getOrCreate().session(options.session.toNeo4jSession)

  private def structForNode(labels: Seq[String] = options.nodeMetadata.labels): StructType = {
    var structFields: mutable.Buffer[StructField] = (try {
      val query =
        """CALL apoc.meta.nodeTypeProperties($config)
          |YIELD propertyName, propertyTypes
          |WITH DISTINCT propertyName, propertyTypes
          |WITH propertyName, collect(propertyTypes) AS propertyTypes
          |RETURN propertyName, reduce(acc = [], elem IN propertyTypes | acc + elem) AS propertyTypes
          |""".stripMargin
      val apocConfig = options.apocConfig.procedureConfigMap
        .getOrElse("apoc.meta.nodeTypeProperties", Map.empty[String, AnyRef])
        .asInstanceOf[Map[String, AnyRef]] ++ Map[String, AnyRef]("includeLabels" -> labels.asJava)
      retrieveSchemaFromApoc(query, Collections.singletonMap("config", apocConfig.asJava))
    } catch {
      case e: ClientException =>
        logSchemaResolutionChange(e)
        // TODO get back to Cypher DSL when rand function will be available
        val query =
          s"""MATCH (${Neo4jUtil.NODE_ALIAS}:${labels.map(_.quote()).mkString(":")})
             |RETURN ${Neo4jUtil.NODE_ALIAS}
             |ORDER BY rand()
             |LIMIT ${options.schemaMetadata.flattenLimit}
             |""".stripMargin
        val params = Collections.emptyMap[String, AnyRef]()
        retrieveSchema(query, params, { record => record.get(Neo4jUtil.NODE_ALIAS).asNode.asMap.asScala.toMap })
    })
      .sortBy(t => t.name)

    structFields += StructField(Neo4jUtil.INTERNAL_LABELS_FIELD, DataTypes.createArrayType(DataTypes.StringType), nullable = true)
    structFields += StructField(Neo4jUtil.INTERNAL_ID_FIELD, DataTypes.LongType, nullable = false)
    StructType(structFields.reverse)
  }

  private def retrieveSchemaFromApoc(query: String, params: java.util.Map[String, AnyRef]): mutable.Buffer[StructField] = {
    session.run(query, params)
      .list
      .asScala
      .filter(record => !record.get("propertyName").isNull && !record.get("propertyName").isEmpty)
      .map(record => {
        val fieldTypesList = record.get("propertyTypes")
          .asList(new java.util.function.Function[Value, String]() {
            override def apply(v: Value): String = v.asString()
          })
          .asScala
        val fieldType: String = if (fieldTypesList.size > 1) {
          log.warn(s"""
               |The field ${record.get("propertyName")} has different types: $fieldTypesList
               |Every value will be casted to string.
               |""".stripMargin)
          "String"
        }
        else {
          fieldTypesList.head
        }

        StructField(record.get("propertyName").asString,
          cypherToSparkType(fieldType))
      })
  }

  private def retrieveSchema(query: String,
                             params: java.util.Map[String, AnyRef],
                             extractFunction: Record => Map[String, AnyRef]): mutable.Buffer[StructField] = {
    session.run(query, params).list.asScala
      .flatMap(extractFunction)
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .map(t => options.schemaMetadata.strategy match {
        case SchemaStrategy.SAMPLE => {
          val types = t._2.map(value => {
            if (options.query.queryType == QueryType.QUERY) {
              normalizedClassName(value)
            } else {
              normalizedClassNameFromGraphEntity(value)
            }
          }).toSet

          if (types.size > 1) {
            log.warn(
              s"""
                 |The field ${t._1} has different types: ${types.toString}
                 |Every value will be casted to string.
                 |""".stripMargin)
            StructField(t._1, DataTypes.StringType)
          }
          else {
            val value = t._2.head
            StructField(t._1, cypherToSparkType(types.head, value))
          }
        }
        case SchemaStrategy.STRING => StructField(t._1, DataTypes.StringType)
      })
      .toBuffer
  }

  private def mapStructField(alias: String, field: StructField): StructField = {
    val name = field.name match {
      case Neo4jUtil.INTERNAL_ID_FIELD | Neo4jUtil.INTERNAL_LABELS_FIELD =>
        s"<$alias.${field.name.replaceAll("[<|>]", "")}>"
      case _ => s"$alias.${field.name}"
    }
    StructField(name, field.dataType, field.nullable, field.metadata)
  }

  def structForRelationship(): StructType = {
    var structFields: mutable.Buffer[StructField] = ArrayBuffer(
      StructField(Neo4jUtil.INTERNAL_REL_ID_FIELD, DataTypes.LongType, false),
      StructField(Neo4jUtil.INTERNAL_REL_TYPE_FIELD, DataTypes.StringType, false))

    if (options.relationshipMetadata.nodeMap) {
      structFields += StructField(s"<${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS}>",
        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false)
      structFields += StructField(s"<${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS}>",
        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false)
    } else {
      structFields ++= structForNode(options.relationshipMetadata.source.labels)
        .map(field => mapStructField(Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS, field))
      structFields ++= structForNode(options.relationshipMetadata.target.labels)
        .map(field => mapStructField(Neo4jUtil.RELATIONSHIP_TARGET_ALIAS, field))
    }

    structFields ++= (try {
      val query =
        """CALL apoc.meta.relTypeProperties($config) YIELD sourceNodeLabels, targetNodeLabels,
          | propertyName, propertyTypes
          |WITH *
          |WHERE sourceNodeLabels = $sourceLabels AND targetNodeLabels = $targetLabels
          |RETURN *
          |""".stripMargin
      val apocConfig = options.apocConfig.procedureConfigMap
        .getOrElse("apoc.meta.relTypeProperties", Map.empty[String, AnyRef])
        .asInstanceOf[Map[String, AnyRef]]
      val config = apocConfig ++ Map("includeRels" -> Seq(options.relationshipMetadata.relationshipType).asJava)
      val params = Map[String, AnyRef]("config" -> config,
        "sourceLabels" -> options.relationshipMetadata.source.labels.asJava,
        "targetLabels" -> options.relationshipMetadata.target.labels.asJava)
        .asJava
      retrieveSchemaFromApoc(query, params)
    } catch {
      case e: ClientException =>
        logSchemaResolutionChange(e)
        // TODO get back to Cypher DSL when rand function will be available
        val query =
          s"""MATCH (${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS}:${options.relationshipMetadata.source.labels.map(_.quote()).mkString(":")})
             |MATCH (${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS}:${options.relationshipMetadata.target.labels.map(_.quote()).mkString(":")})
             |MATCH (${Neo4jUtil.RELATIONSHIP_SOURCE_ALIAS})-[${Neo4jUtil.RELATIONSHIP_ALIAS}:${options.relationshipMetadata.relationshipType}]->(${Neo4jUtil.RELATIONSHIP_TARGET_ALIAS})
             |RETURN ${Neo4jUtil.RELATIONSHIP_ALIAS}
             |ORDER BY rand()
             |LIMIT ${options.schemaMetadata.flattenLimit}
             |""".stripMargin
        val params = Collections.emptyMap[String, AnyRef]()
        retrieveSchema(query, params, { record => record.get(Neo4jUtil.RELATIONSHIP_ALIAS).asRelationship.asMap.asScala.toMap })
    })
      .map(field => StructField(s"rel.${field.name}", field.dataType, field.nullable, field.metadata))
      .sortBy(t => t.name)
    StructType(structFields)
  }

  def structForQuery(): StructType = {
    val query = queryReadStrategy.createStatementForQuery(options)

    if (!isValidQuery(query, summary.QueryType.READ_ONLY)) {
      return new StructType()
    }

    val params = Collections.singletonMap[String, AnyRef](Neo4jQueryStrategy.VARIABLE_SCRIPT_RESULT, Collections.emptyList())
    val structFields = retrieveSchema(query, params, { record => record.asMap.asScala.toMap })

    val columns = getReturnedColumns(query)
    if (columns.isEmpty && structFields.isEmpty) {
      throw new ClientException("Unable to compute the resulting schema; this may mean your result set is empty or your version of Neo4j does not permit schema inference for empty sets")
    }

    if (columns.isEmpty) {
      return StructType(structFields)
    }

    val sortedStructFields = if (structFields.isEmpty) {
      // df: we arrived here because there are no data returned by the query
      // so we want to return an empty dataset which schema is equals to the columns
      // specified by the RETURN statement
      columns.map(StructField(_, DataTypes.StringType))
    } else {
      try {
        columns.map(column => structFields.find(_.name.quote() == column.quote()).orNull).filter(_ != null)
      } catch {
        case _: Throwable => structFields.toArray
      }
    }

    StructType(sortedStructFields)
  }

  private def getReturnedColumns(query: String): Array[String] = {
    val plan = session.run(s"EXPLAIN $query").consume().plan()

    if (plan.arguments().containsKey("Details")) {
      plan.arguments()
        .get("Details")
        .asString()
        .replaceAll("\"", "")
        .split(',')
        .map(_.trim)
    }
    else {
      val lastChild = plan.children().get(0)

      lastChild.operatorType() match {
        case "EagerAggregation" => lastChild.identifiers().asScala.toArray
        case "ProcedureCall" => plan.identifiers().asScala.toArray
        case _ =>
          try {
            val expressions = lastChild.arguments().get("Expressions").asString()
            val firstLevelExpressions = "\\{(.*?)}".r.replaceAllIn(expressions.substring(1,expressions.length-1),"_")

            "([^,:]*?):".r.findAllMatchIn(firstLevelExpressions).map(_.group(1).trim).toArray
          } catch {
            case e: Exception =>
              log.warn(s"I was unable to understand the returned column using EXPLAIN due to '${e.getMessage}'")
              Array.empty
          }
      }
    }
  }

  def struct(): StructType = {
    val struct = options.query.queryType match {
      case QueryType.LABELS => structForNode()
      case QueryType.RELATIONSHIP => structForRelationship()
      case QueryType.QUERY => structForQuery()
    }

    struct
  }

  def countForNodeWithQuery(filters: Array[Filter]): Long = {
    val query = if (filters.isEmpty) {
      options.nodeMetadata.labels
        .map(_.quote())
        .map(label =>
          s"""
             |MATCH (:$label)
             |RETURN count(*) AS count""".stripMargin)
        .mkString(" UNION ALL ")
    } else {
      queryReadStrategy.createStatementForNodeCount(options)
    }
    log.info(s"Executing the following counting query on Neo4j: $query")
    session.run(query)
      .list()
      .asScala
      .map(_.get("count"))
      .map(count => if (count.isNull) 0L else count.asLong())
      .min
  }

  def countForRelationshipWithQuery(filters: Array[Filter]): Long = {
    val query = if (filters.isEmpty) {
      val sourceQueries = options.relationshipMetadata.source.labels
        .map(_.quote())
        .map(label =>
          s"""MATCH (:$label)-[${Neo4jUtil.RELATIONSHIP_ALIAS}:${options.relationshipMetadata.relationshipType.quote()}]->()
             |RETURN count(${Neo4jUtil.RELATIONSHIP_ALIAS}) AS count
             |""".stripMargin)
      val targetQueries = options.relationshipMetadata.target.labels
        .map(_.quote())
        .map(label =>
          s"""MATCH ()-[${Neo4jUtil.RELATIONSHIP_ALIAS}:${options.relationshipMetadata.relationshipType.quote()}]->(:$label)
             |RETURN count(${Neo4jUtil.RELATIONSHIP_ALIAS}) AS count
             |""".stripMargin)
      (sourceQueries ++ targetQueries)
        .mkString(" UNION ALL ")
    } else {
      queryReadStrategy.createStatementForRelationshipCount(options)
    }
    log.info(s"Executing the following counting query on Neo4j: $query")
    session.run(query)
      .list()
      .asScala
      .map(_.get("count"))
      .map(count => if (count.isNull) 0L else count.asLong())
      .min
  }

  def countForNode(filters: Array[Filter]): Long = try {
    /*
     * we try to leverage the count store in order to have the faster response possible
     * https://neo4j.com/developer/kb/fast-counts-using-the-count-store/
     * so in this scenario we have some limitations given the fact that we get the min
     * for the sequence of counts returned
     */
    if (filters.isEmpty) {
      val query = "CALL apoc.meta.stats() yield labels RETURN labels"
      val map = session.run(query).single()
        .asMap()
        .asScala
        .get("labels")
        .getOrElse(Collections.emptyMap())
        .asInstanceOf[util.Map[String, Long]].asScala
      map.filterKeys(k => options.nodeMetadata.labels.contains(k))
        .values.min
    } else {
      countForNodeWithQuery(filters)
    }
  } catch {
    case e: ClientException => {
      log.warn("Switching to query count resolution because of the following exception:", e)
      countForNodeWithQuery(filters)
    }
    case e: Throwable => logExceptionForCount(e)
  }

  def countForRelationship(filters: Array[Filter]): Long = try {
    if (filters.isEmpty) {
      val query = "CALL apoc.meta.stats() yield relTypes RETURN relTypes"
      val map = session.run(query).single()
        .asMap()
        .asScala
        .get("relTypes")
        .getOrElse(Collections.emptyMap())
        .asInstanceOf[util.Map[String, Long]]
        .asScala
      val minFromSource = options.relationshipMetadata.source.labels
        .map(_.quote())
        .map(label => map.get(s"(:$label)-[:${options.relationshipMetadata.relationshipType}]->()").getOrElse(Long.MaxValue))
        .min
      val minFromTarget = options.relationshipMetadata.target.labels
        .map(_.quote())
        .map(label => map.get(s"()-[:${options.relationshipMetadata.relationshipType}]->(:$label)").getOrElse(Long.MaxValue))
        .min
      Math.min(minFromSource, minFromTarget)
    } else {
      countForRelationshipWithQuery(filters)
    }
  } catch {
    case e: ClientException => {
      log.warn("Switching to query count resolution because of the following exception:", e)
      countForRelationshipWithQuery(filters)
    }
    case e: Throwable => logExceptionForCount(e)
  }

  private def logExceptionForCount(e: Throwable): Long = {
    log.error("Cannot compute the count because the following exception:", e)
    -1
  }

  def countForQuery(): Long = {
    val queryCount: String = options.queryMetadata.queryCount
    if (Neo4jUtil.isLong(queryCount)) {
      queryCount.toLong
    } else {
      val query = if (queryCount.nonEmpty) {
        options.queryMetadata.queryCount
      } else {
        s"""CALL { ${options.query.value} }
           |RETURN count(*) AS count
           |""".stripMargin
      }
      session.run(query).single().get("count").asLong()
    }
  }

  def count(filters: Array[Filter] = Array.empty[Filter]): Long = options.query.queryType match {
    case QueryType.LABELS => countForNode(filters)
    case QueryType.RELATIONSHIP => countForRelationship(filters)
    case QueryType.QUERY => countForQuery()
  }

  def skipLimitFromPartition(): Seq[PartitionSkipLimit] = if (options.partitions == 1) {
    Seq(PartitionSkipLimit.EMPTY)
  } else {
    val count: Long = this.count()
    if (count <= 0) {
      Seq(PartitionSkipLimit.EMPTY)
    } else {
      val partitionSize = Math.ceil(count.toDouble / options.partitions).toLong
      val partitions = options.query.queryType match {
        case QueryType.QUERY => if (options.queryMetadata.queryCount.nonEmpty) {
          options.partitions // for custom query count we overfetch
        } else {
          options.partitions - 1
        }
        case _ => options.partitions - 1
      }
      (0 to partitions)
        .map(index => PartitionSkipLimit(index, index * partitionSize, partitionSize))
    }
  }

  def isValidQuery(query: String, expectedQueryTypes: org.neo4j.driver.summary.QueryType*): Boolean = try {
    val queryType = session.run(s"EXPLAIN $query").consume().queryType()
    expectedQueryTypes.size == 0 || expectedQueryTypes.contains(queryType)
  } catch {
    case e: Throwable => {
      log.error("Query not compiled because of the following exception:", e)
      false
    }
  }

  def isValidQueryCount(query: String): Boolean = {
    try {
      val resultSummary = session.run(s"EXPLAIN $query").consume()
      val queryType = resultSummary.queryType()
      val plan = resultSummary.plan()
      val isReadOnly = queryType == org.neo4j.driver.summary.QueryType.READ_ONLY || queryType == org.neo4j.driver.summary.QueryType.SCHEMA_WRITE
      val hasCountIdentifier = plan.identifiers().asScala.toSet == Set("count")
      isReadOnly && hasCountIdentifier
    } catch {
      case e: Throwable => {
        log.error("Query not compiled because of the following exception:", e)
        false
      }
    }
  }

  def neo4jVersion() = session
    .run("CALL dbms.components()")
    .single()
    .asMap()
    .asScala
    .mapResult[Neo4jVersion](m => Neo4jVersion(m("name").asInstanceOf[String],
      m("versions").asInstanceOf[util.List[String]].asScala,
      m("edition").asInstanceOf[String]))
    .result()


  private def createIndexOrConstraint(action: OptimizationType.Value, label: String, props: Seq[String]): Unit = action match {
    case OptimizationType.NONE => log.info("No optimization type provided")
    case _ => {
      try {
        val isNeo4j35 = neo4jVersion().versions(0).startsWith("3.5")
        val quotedLabel = label.quote()
        val quotedProps = props
          .map(prop => s"${Neo4jUtil.NODE_ALIAS}.${prop.quote()}")
          .mkString(", ")
        val (querySuffix, uniqueness) = action match {
          case OptimizationType.INDEX => {
            if (isNeo4j35) {
              (s"ON :$quotedLabel(${props.map(_.quote()).mkString(",")})", "node_label_property")
            } else {
              (s"FOR (${Neo4jUtil.NODE_ALIAS}:$quotedLabel) ON ($quotedProps)", "NONUNIQUE")
            }
          }
          case OptimizationType.NODE_CONSTRAINTS => {
            val assertType = if (props.size > 1) "NODE KEY" else "UNIQUE"
            val uniquenessValue = if (isNeo4j35) {
              "node_unique_property"
            } else {
              "UNIQUE"
            }
            (s"ON (${Neo4jUtil.NODE_ALIAS}:$quotedLabel) ASSERT ($quotedProps) IS $assertType", uniquenessValue)
          }
        }
        val (labelIndexFieldName, uniqueFieldName, actionName) = if (isNeo4j35) {
          ("tokenNames", "type", "")
        } else {
          ("labelsOrTypes", "uniqueness", s"spark_${action.toString}_$label".quote())
        }
        val queryPrefix = action match {
          case OptimizationType.INDEX => s"CREATE INDEX $actionName"
          case OptimizationType.NODE_CONSTRAINTS => s"CREATE CONSTRAINT $actionName"
        }
        val queryCheck =
          s"""CALL db.indexes() YIELD $labelIndexFieldName, properties, $uniqueFieldName
             |WHERE $labelIndexFieldName = ${'$'}labels
             |AND properties = ${'$'}properties
             |AND $uniqueFieldName = ${'$'}uniqueness
             |RETURN count(*) > 0 AS isPresent""".stripMargin
        val isPresent = session.run(queryCheck, Map("labels" -> Seq(label).asJava,
          "properties" -> props.asJava,
          "uniqueness" -> uniqueness).asJava)
          .single()
          .get("isPresent")
          .asBoolean()

        val status = if (isPresent) {
          "KEPT"
        } else {
          val query = s"$queryPrefix $querySuffix"
          log.info(s"Performing the following schema query: $query")
          session.run(query)
          "CREATED"
        }
        log.info(s"Status for $action named with label $quotedLabel and props $quotedProps is: $status")
      } catch {
        case e: Throwable => log.info("Cannot perform the optimization query because of the following exception:", e)
      }
    }
  }

  private def createOptimizationsForNode(): Unit = options.schemaMetadata.optimizationType match {
    case OptimizationType.INDEX | OptimizationType.NODE_CONSTRAINTS => {
      createIndexOrConstraint(options.schemaMetadata.optimizationType,
        options.nodeMetadata.labels.head,
        options.nodeMetadata.nodeKeys.values.toSeq)
    }
    case _ => // do nothing
  }

  private def createOptimizationsForRelationship(): Unit = options.schemaMetadata.optimizationType match {
    case OptimizationType.INDEX | OptimizationType.NODE_CONSTRAINTS => {
      createIndexOrConstraint(options.schemaMetadata.optimizationType,
        options.relationshipMetadata.source.labels.head,
        options.relationshipMetadata.source.nodeKeys.values.toSeq)
      createIndexOrConstraint(options.schemaMetadata.optimizationType,
        options.relationshipMetadata.target.labels.head,
        options.relationshipMetadata.target.nodeKeys.values.toSeq)
    }
    case _ => // do nothing
  }

  def createOptimizations(): Unit = {
    options.query.queryType match {
      case QueryType.LABELS => createOptimizationsForNode()
      case QueryType.RELATIONSHIP => createOptimizationsForRelationship()
      case _ => // do nothing
    }
  }

  def execute(queries: Seq[String]): util.List[util.Map[String, AnyRef]] = {
    val queryMap = queries
      .map(query => {
        (session.run(s"EXPLAIN $query").consume().queryType(), query)
      })
      .groupBy(_._1)
      .mapValues(_.map(_._2))
    val schemaQueries = queryMap.getOrElse(org.neo4j.driver.summary.QueryType.SCHEMA_WRITE, Seq.empty[String])
    schemaQueries.foreach(session.run(_))
    val others = queryMap
      .filterKeys(key => key != org.neo4j.driver.summary.QueryType.SCHEMA_WRITE)
      .values
      .flatten
      .toSeq
    if (others.isEmpty) {
      Collections.emptyList()
    } else {
      session
        .writeTransaction(new TransactionWork[util.List[java.util.Map[String, AnyRef]]] {
          override def execute(transaction: Transaction): util.List[util.Map[String, AnyRef]] = {
            others.size match {
              case 1 => transaction.run(others(0)).list()
                .asScala
                .map(_.asMap())
                .asJava
              case _ => {
                others
                  .slice(0, queries.size - 1)
                  .foreach(transaction.run(_))
                val result = transaction.run(others.last).list()
                  .asScala
                  .map(_.asMap())
                  .asJava
                result
              }
            }
          }
        })
    }
  }

  private def logSchemaResolutionChange(e: ClientException): Unit = {
    log.warn(s"Switching to query schema resolution")
    if(!e.code().equals("Neo.ClientError.Procedure.ProcedureNotFound")) {
      log.warn(s"For the following exception", e)
    }
  }

  override def close(): Unit = {
    Neo4jUtil.closeSafety(session, log)
  }
}

object SchemaService {
  val POINT_TYPE_2D = "point-2d"
  val POINT_TYPE_3D = "point-3d"

  val TIME_TYPE_OFFSET = "offset-time"
  val TIME_TYPE_LOCAL = "local-time"

  val DURATION_TYPE = "duration"

  val durationType: DataType = DataTypes.createStructType(Array(
    DataTypes.createStructField("type", DataTypes.StringType, false),
    DataTypes.createStructField("months", DataTypes.LongType, false),
    DataTypes.createStructField("days", DataTypes.LongType, false),
    DataTypes.createStructField("seconds", DataTypes.LongType, false),
    DataTypes.createStructField("nanoseconds", DataTypes.IntegerType, false),
    DataTypes.createStructField("value", DataTypes.StringType, false)
  ))

  val pointType: DataType = DataTypes.createStructType(Array(
    DataTypes.createStructField("type", DataTypes.StringType, false),
    DataTypes.createStructField("srid", DataTypes.IntegerType, false),
    DataTypes.createStructField("x", DataTypes.DoubleType, false),
    DataTypes.createStructField("y", DataTypes.DoubleType, false),
    DataTypes.createStructField("z", DataTypes.DoubleType, true)
  ))

  val timeType: DataType = DataTypes.createStructType(Array(
    DataTypes.createStructField("type", DataTypes.StringType, false),
    DataTypes.createStructField("value", DataTypes.StringType, false)
  ))

  private val cleanTerms = "Unmodifiable|Internal|Iso|2D|3D|Offset|Local|Zoned"

  def normalizedClassName(value: AnyRef): String = value match {
    case list: java.util.List[_] => "Array"
    case map: java.util.Map[String, _] => "Map"
    case null => "String"
    case _ => value.getClass.getSimpleName
  }

  // from nodes and relationships we cannot have maps as properties and elements in collections are the same type
  def normalizedClassNameFromGraphEntity(value: AnyRef): String = value match {
    case list: java.util.List[_] => s"${list.get(0).getClass.getSimpleName}Array"
    case null => "String"
    case _ => value.getClass.getSimpleName
  }

  def cypherToSparkType(cypherType: String, value: Any = null): DataType = {
    cypherType.replaceAll(cleanTerms, "") match {
      case "Node" | "Relationship" => if (value != null) value.asInstanceOf[Entity].toStruct() else DataTypes.NullType
      case "NodeArray" | "RelationshipArray" => if (value != null) DataTypes.createArrayType(value.asInstanceOf[Entity].toStruct()) else DataTypes.NullType
      case "Boolean" => DataTypes.BooleanType
      case "Long" => DataTypes.LongType
      case "Double" => DataTypes.DoubleType
      case "Point" => pointType
      case "DateTime" => DataTypes.TimestampType
      case "Time" => timeType
      case "Date" => DataTypes.DateType
      case "Duration" => durationType
      case "Map" => {
        val valueType = if (value == null) {
          DataTypes.NullType
        } else {
          val map = value.asInstanceOf[java.util.Map[String, AnyRef]].asScala
          val types = map.values
            .map(normalizedClassName)
            .toSet
          if (types.size == 1) cypherToSparkType(types.head, map.values.head) else DataTypes.StringType
        }
        DataTypes.createMapType(DataTypes.StringType, valueType)
      }
      case "Array" => {
        val valueType = if (value == null) {
          DataTypes.NullType
        } else {
          val list = value.asInstanceOf[java.util.List[AnyRef]].asScala
          val types = list
            .map(normalizedClassName)
            .toSet
          if (types.size == 1) cypherToSparkType(types.head, list.head) else DataTypes.StringType
        }
        DataTypes.createArrayType(valueType)
      }
      // These are from APOC
      case "StringArray" => DataTypes.createArrayType(DataTypes.StringType)
      case "LongArray" => DataTypes.createArrayType(DataTypes.LongType)
      case "DoubleArray" => DataTypes.createArrayType(DataTypes.DoubleType)
      case "BooleanArray" => DataTypes.createArrayType(DataTypes.BooleanType)
      case "PointArray" => DataTypes.createArrayType(pointType)
      case "DateTimeArray" => DataTypes.createArrayType(DataTypes.TimestampType)
      case "TimeArray" => DataTypes.createArrayType(timeType)
      case "DateArray" => DataTypes.createArrayType(DataTypes.DateType)
      case "DurationArray" => DataTypes.createArrayType(durationType)
      // Default is String
      case _ => DataTypes.StringType
    }
  }
}