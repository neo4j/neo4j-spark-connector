package org.neo4j.spark.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.neo4j.driver.{AccessMode, Session, summary}
import org.neo4j.spark.service.{Neo4jQueryStrategy, SchemaService}
import org.neo4j.spark.streaming.Neo4jAccumulator
import org.neo4j.spark.util
import org.neo4j.spark.util.Neo4jImplicits.StructTypeImplicit

import java.util.Locale

object Validations {
  def validate(validations: Validation*): Unit = validations.toSet[Validation].foreach(_.validate())
}

trait Validation extends Logging {
  def validate(): Unit
  def ignoreOption(ignoredOption: String, primaryOption: String): Unit =
    logWarning(s"Option `$ignoredOption` is not compatible with `$primaryOption` and will be ignored")
}

case class ValidateSchemaOptions(neo4jOptions: Neo4jOptions, schema: StructType) extends Validation {
  override def validate(): Unit = {
    val missingFieldsMap = Map(
      Neo4jOptions.NODE_KEYS -> schema.getMissingFields(neo4jOptions.nodeMetadata.nodeKeys.keySet),
      Neo4jOptions.NODE_PROPS -> schema.getMissingFields(neo4jOptions.nodeMetadata.nodeProps.keySet),
      Neo4jOptions.RELATIONSHIP_PROPERTIES -> schema.getMissingFields(neo4jOptions.relationshipMetadata.properties.keySet),
      Neo4jOptions.RELATIONSHIP_SOURCE_NODE_PROPS -> schema.getMissingFields(neo4jOptions.relationshipMetadata.source.nodeProps.keySet),
      Neo4jOptions.RELATIONSHIP_SOURCE_NODE_KEYS -> schema.getMissingFields(neo4jOptions.relationshipMetadata.source.nodeKeys.keySet),
      Neo4jOptions.RELATIONSHIP_TARGET_NODE_PROPS -> schema.getMissingFields(neo4jOptions.relationshipMetadata.target.nodeProps.keySet),
      Neo4jOptions.RELATIONSHIP_TARGET_NODE_KEYS -> schema.getMissingFields(neo4jOptions.relationshipMetadata.target.nodeKeys.keySet)
    )

    val optionsWithMissingFields = missingFieldsMap.filter(_._2.nonEmpty)

    if (optionsWithMissingFields.nonEmpty) {
      throw new IllegalArgumentException(
        s"""Write failed due to the following errors:
           |${optionsWithMissingFields.map(field => s" - Schema is missing ${field._2.mkString(", ")} from option `${field._1}`").mkString("\n")}
           |
           |The option key and value might be inverted.""".stripMargin)
    }
  }
}

case class ValidateSparkMinVersion(supportedVersions: String*) extends Validation {
  override def validate(): Unit = {
    val sparkVersion = SparkSession.getActiveSession
      .map(_.version)
      .getOrElse("UNKNOWN")

    ValidationUtil.isTrue(
      isSupported(sparkVersion),
      s"""Your current Spark version $sparkVersion is not supported by the current connector.
         |Please visit https://neo4j.com/developer/spark/overview/#_spark_compatibility to know which connector version you need.
         |""".stripMargin
    )
  }

  def isSupported(sparkVersion: String): Boolean = {
    val splittedVersion = sparkVersion.split("\\.")
    if (sparkVersion == "UNKNOWN") return true
    val versions = supportedVersions
      .flatMap(_.split("\\.").zip(splittedVersion))
      .map(t =>
        try {
          (t._1.toInt, t._2.toInt)
        } catch {
          case _: NumberFormatException => null
        })
      .filter(p => p != null)
    for (t <- versions) {
      val curr = t._2
      val supported = t._1
      if (curr > supported) { // if Spark current version (step) is greater than the supported one
        return true // we can assume that the current version is a greater version, so it's supported
      }
      if (curr < supported) { // if Spark current version (step) is lower than the supported one
        return false // we can assume that the current version is a greater version, so it's supported
      }
      // if the versions are equal we can check the next step
    }
    true // this happens if the two versions are equal
  }
}

case class ValidateConnection(neo4jOptions: Neo4jOptions,
                              jobId: String) extends Validation {
  override def validate(): Unit = {
    var driverCache: DriverCache = null
    var session: Session = null
    var hasError = false
    try {
      driverCache = new DriverCache(neo4jOptions.connection, jobId)
      session = driverCache.getOrCreate().session(neo4jOptions.session.toNeo4jSession())
      session.run("EXPLAIN RETURN 1").consume()
    } catch {
      case e: Throwable => {
        hasError = true
        throw e
      }
    } finally {
      Neo4jUtil.closeSafety(session)
      if (hasError) {
        Neo4jUtil.closeSafety(driverCache)
      }
    }
  }
}

case class ValidateSaveMode(saveMode: String) extends Validation {
  override def validate(): Unit = {
    ValidationUtil.isTrue(Neo4jOptions.SUPPORTED_SAVE_MODES.contains(SaveMode.valueOf(saveMode)),
      s"""Unsupported SaveMode.
         |You provided $saveMode, supported are:
         |${Neo4jOptions.SUPPORTED_SAVE_MODES.mkString(",")}
         |""".stripMargin)
  }
}

case class ValidateWrite(neo4jOptions: Neo4jOptions,
                         jobId: String,
                         saveMode: SaveMode,
                         customValidation: Neo4jOptions => Unit = _ => ()) extends Validation {
  override def validate(): Unit = {
    ValidationUtil.isFalse(neo4jOptions.session.accessMode == AccessMode.READ,
      s"Mode READ not supported for Data Source writer")
    val cache = new DriverCache(neo4jOptions.connection, jobId)
    val schemaService = new SchemaService(neo4jOptions, cache)
    try {
      ValidateConnection(neo4jOptions, jobId).validate()
      ValidateNeo4jOptionsConsistency(neo4jOptions).validate()

      neo4jOptions.query.queryType match {
        case QueryType.QUERY => {
          ValidationUtil.isTrue(schemaService.isValidQuery(
            s"""WITH {} AS ${Neo4jQueryStrategy.VARIABLE_EVENT}, [] as ${Neo4jQueryStrategy.VARIABLE_SCRIPT_RESULT}
               |${neo4jOptions.query.value}
               |""".stripMargin,
            org.neo4j.driver.summary.QueryType.WRITE_ONLY, org.neo4j.driver.summary.QueryType.READ_WRITE),
            "Please provide a valid WRITE query")
          neo4jOptions.schemaMetadata.optimizationType match {
            case OptimizationType.NONE => // are valid
            case _ => ValidationUtil.isNotValid(
              s"""With Query Type ${neo4jOptions.query.queryType} you can
                 |only use `${Neo4jOptions.SCHEMA_OPTIMIZATION_TYPE}`
                 |`${OptimizationType.NONE}`
                 |""".stripMargin)
          }
        }
        case QueryType.LABELS => {
          saveMode match {
            case SaveMode.Overwrite => {
              ValidationUtil.isNotEmpty(neo4jOptions.nodeMetadata.nodeKeys,
                s"${Neo4jOptions.NODE_KEYS} is required when Save Mode is Overwrite")
            }
            case _ => ()
          }
        }
        case QueryType.RELATIONSHIP => {
          ValidationUtil.isNotEmpty(neo4jOptions.relationshipMetadata.target.labels,
            s"${Neo4jOptions.RELATIONSHIP_SOURCE_LABELS} is required when Save Mode is Overwrite")
          ValidationUtil.isNotEmpty(neo4jOptions.relationshipMetadata.target.labels,
            s"${Neo4jOptions.RELATIONSHIP_TARGET_LABELS} is required when Save Mode is Overwrite")
        }
      }
      neo4jOptions.schemaMetadata.optimizationType match {
        case OptimizationType.NONE => // skip it
        case _ => neo4jOptions.query.queryType match {
          case QueryType.LABELS => ValidationUtil.isTrue(saveMode == SaveMode.Overwrite, "This works only with `mode` `SaveMode.Overwrite`")
          case QueryType.RELATIONSHIP => {
            ValidationUtil.isTrue(neo4jOptions.relationshipMetadata.sourceSaveMode == NodeSaveMode.Overwrite,
              s"This works only with `${Neo4jOptions.RELATIONSHIP_SOURCE_SAVE_MODE}` `Overwrite`")
            ValidationUtil.isTrue(neo4jOptions.relationshipMetadata.targetSaveMode == NodeSaveMode.Overwrite,
              s"This works only with `${Neo4jOptions.RELATIONSHIP_TARGET_SAVE_MODE}` `Overwrite`")
          }
        }
      }
      neo4jOptions.script.foreach(query => ValidationUtil.isTrue(schemaService.isValidQuery(query),
        s"The following query inside the `${Neo4jOptions.SCRIPT}` is not valid, please check the syntax: $query"))

      customValidation(neo4jOptions)
    } finally {
      schemaService.close()
      cache.close()
    }
  }
}

case class ValidateRead(neo4jOptions: Neo4jOptions, jobId: String) extends Validation {
  override def validate(): Unit = {
    val cache = new DriverCache(neo4jOptions.connection, jobId)
    val schemaService = new SchemaService(neo4jOptions, cache)
    try {
      ValidateConnection(neo4jOptions, jobId).validate()
      ValidateNeo4jOptionsConsistency(neo4jOptions).validate()

      neo4jOptions.query.queryType match {
        case QueryType.LABELS => {
          ValidationUtil.isNotEmpty(neo4jOptions.nodeMetadata.labels,
            s"You need to set the ${QueryType.LABELS.toString.toLowerCase} option")
        }
        case QueryType.RELATIONSHIP => {
          ValidationUtil.isNotBlank(neo4jOptions.relationshipMetadata.relationshipType,
            s"You need to set the ${QueryType.RELATIONSHIP.toString.toLowerCase} option")

          ValidationUtil.isNotEmpty(neo4jOptions.relationshipMetadata.source.labels,
            s"You need to set the ${Neo4jOptions.RELATIONSHIP_SOURCE_LABELS} option")

          ValidationUtil.isNotEmpty(neo4jOptions.relationshipMetadata.target.labels,
            s"You need to set the ${Neo4jOptions.RELATIONSHIP_TARGET_LABELS} option")
        }
        case QueryType.QUERY => {
          ValidationUtil.isFalse(neo4jOptions.query.value.matches("(?si).*(LIMIT \\d+|SKIP ?\\d+)\\s*\\z"),
            "SKIP/LIMIT are not allowed at the end of the query")
          ValidationUtil.isTrue(schemaService.isValidQuery(s"""WITH [] as ${Neo4jQueryStrategy.VARIABLE_SCRIPT_RESULT}
                                                              |${neo4jOptions.query.value}
                                                              |""".stripMargin, org.neo4j.driver.summary.QueryType.READ_ONLY),
            "Please provide a valid READ query")
          if (neo4jOptions.queryMetadata.queryCount.nonEmpty) {
            if (!Neo4jUtil.isLong(neo4jOptions.queryMetadata.queryCount)) {
              ValidationUtil.isTrue(schemaService.isValidQueryCount(neo4jOptions.queryMetadata.queryCount),
                "Please provide a valid READ query count")
            }
          }
        }
        case QueryType.GDS => {
          ValidationUtil.isFalse(neo4jOptions.query.value.contains(".mutate") || neo4jOptions.query.value.contains(".write"),
            "You cannot execute GDS mutate or write procedure in a read query")
          ValidationUtil.isTrue(schemaService.isGdsProcedure(neo4jOptions.query.value), s"GDS procedure ${neo4jOptions.query.value} does not exist")
          ValidationUtil.isTrue(neo4jOptions.partitions == 1, "For GDS queries we support only one partition")
          Validations.validate(ValidateGdsMetadata(neo4jOptions.gdsMetadata))
        }
      }
      neo4jOptions.script.foreach(query => ValidationUtil.isTrue(schemaService.isValidQuery(query),
        s"The following query inside the `${Neo4jOptions.SCRIPT}` is not valid, please check the syntax: $query"))
    } finally {
      schemaService.close()
      cache.close()
    }
  }
}

case class ValidateReadNotStreaming(neo4jOptions: Neo4jOptions, jobId: String) extends Validation {
  override def validate(): Unit = {
    ValidationUtil.isBlank(neo4jOptions.streamingOptions.propertyName,
      s"You don't need to set the `${Neo4jOptions.STREAMING_PROPERTY_NAME}` option")
  }
}

/**
 * df: this method checks for inconsistencies between provided options.
 * Ex: if we use the QueryType.LABELS, we will ignore any relationship options.
 *
 * Plus it throws an exception if no QueryType is provided.
 */
case class ValidateNeo4jOptionsConsistency(neo4jOptions: Neo4jOptions) extends Validation {
  override def validate(): Unit = {
    if (neo4jOptions.query.value.isEmpty) {
      val reqTypes = QueryType.values.map(qt => s"`${qt.toString}`").mkString(", ")
      throw new IllegalArgumentException(s"No valid option found. One of $reqTypes is required")
    }

    neo4jOptions.query.queryType match {
      case QueryType.LABELS =>
        ignoreQueryMetadata(QueryType.LABELS)
        ignoreRelMetadata(QueryType.LABELS)
        ignoreGdsMetadata(QueryType.LABELS)
      case QueryType.RELATIONSHIP  =>
        ignoreQueryMetadata(QueryType.RELATIONSHIP)
        ignoreNodeMetadata(QueryType.RELATIONSHIP)
        ignoreGdsMetadata(QueryType.RELATIONSHIP)
      case QueryType.QUERY => {
        ignoreNodeMetadata(QueryType.QUERY)
        ignoreRelMetadata(QueryType.QUERY)
        ignoreGdsMetadata(QueryType.QUERY)
      }
      case QueryType.GDS =>
        ignoreQueryMetadata(QueryType.GDS)
        ignoreNodeMetadata(QueryType.GDS)
        ignoreRelMetadata(QueryType.GDS)
    }
  }

  private def ignoreGdsMetadata(queryType: QueryType.Value): Unit = {
    if (!neo4jOptions.gdsMetadata.parameters.isEmpty) {
      ignoreOption(Neo4jOptions.QUERY_COUNT, queryType.toString.toLowerCase(Locale.ENGLISH))
    }
  }

  private def ignoreQueryMetadata(queryType: QueryType.Value): Unit = {
    if (neo4jOptions.queryMetadata.queryCount.nonEmpty) {
      ignoreOption(Neo4jOptions.QUERY_COUNT, queryType.toString.toLowerCase(Locale.ENGLISH))
    }
  }

  private def ignoreRelMetadata(queryType: QueryType.Value): Unit = {
    val optName = queryTypeAsOptionString(queryType)
    if (neo4jOptions.relationshipMetadata.source.labels.nonEmpty) {
      ignoreOption(Neo4jOptions.RELATIONSHIP_SOURCE_LABELS, optName)
    }
    if (neo4jOptions.relationshipMetadata.source.nodeProps.nonEmpty) {
      ignoreOption(Neo4jOptions.RELATIONSHIP_SOURCE_NODE_PROPS, optName)
    }
    if (neo4jOptions.relationshipMetadata.source.nodeKeys.nonEmpty) {
      ignoreOption(Neo4jOptions.RELATIONSHIP_SOURCE_NODE_KEYS, optName)
    }
    if (neo4jOptions.relationshipMetadata.target.labels.nonEmpty) {
      ignoreOption(Neo4jOptions.RELATIONSHIP_TARGET_LABELS, optName)
    }
    if (neo4jOptions.relationshipMetadata.target.nodeProps.nonEmpty) {
      ignoreOption(Neo4jOptions.RELATIONSHIP_TARGET_NODE_PROPS, optName)
    }
    if (neo4jOptions.relationshipMetadata.target.nodeKeys.nonEmpty) {
      ignoreOption(Neo4jOptions.RELATIONSHIP_TARGET_NODE_KEYS, optName)
    }
  }

  private def queryTypeAsOptionString(queryType: util.QueryType.Value): String =
    queryType.toString.toLowerCase(Locale.ENGLISH)

  private def ignoreNodeMetadata(queryType: QueryType.Value): Unit = {
    val optName = queryTypeAsOptionString(queryType)
    if (neo4jOptions.nodeMetadata.nodeKeys.nonEmpty) {
      ignoreOption(Neo4jOptions.NODE_KEYS, optName)
    }
    if (neo4jOptions.nodeMetadata.nodeProps.nonEmpty) {
      ignoreOption(Neo4jOptions.NODE_PROPS, optName)
    }
  }
}

case class ValidateGdsMetadata(neo4jGdsMetadata: Neo4jGdsMetadata) extends Validation {
  override def validate(): Unit = {
    val hasGraphName = neo4jGdsMetadata.parameters.get("graphName") != null
    val hasGraphNameOrConfiguration = neo4jGdsMetadata.parameters.get("graphNameOrConfiguration") != null
    ValidationUtil.isTrue(hasGraphName || hasGraphNameOrConfiguration,
      "One between gds.graphName or gds.graphNameOrConfiguration is required")
  }
}

case class ValidateReadStreaming(neo4jOptions: Neo4jOptions, jobId: String) extends Validation {
  override def validate(): Unit = {
    val cache = new DriverCache(neo4jOptions.connection, jobId)
    val schemaService = new SchemaService(neo4jOptions, cache)
    try {
      neo4jOptions.streamingOptions.storageType match {
        case StorageType.NEO4J => {
            ValidationUtil.isTrue(schemaService.checkIndex(OptimizationType.NODE_CONSTRAINTS, Neo4jAccumulator.LABEL, Seq(Neo4jAccumulator.KEY)),
              s"""
                |The connector need to store intermediate results
                |for pushing the data into Streaming tables.
                |Please define a constraint into your Neo4j instance in this way:
                |`CREATE CONSTRAINT FOR (n:${Neo4jAccumulator.LABEL}) REQUIRE (n.${Neo4jAccumulator.KEY}) IS UNIQUE`
                |""".stripMargin)
        }
        case _ =>
      }
      ValidationUtil.isTrue(neo4jOptions.partitions == 1, "For Spark Structured Streaming we support only one partition")
      neo4jOptions.query.queryType match {
        case QueryType.QUERY => {
          ValidationUtil.isTrue(schemaService.isValidQuery(neo4jOptions.streamingOptions.queryOffset, summary.QueryType.READ_ONLY),
          """
              |Please set `streaming.query.offset` with a valid Cypher READ_ONLY query
              |that returns a long value i.e.
              |MATCH (p:MyLabel)
              |RETURN max(p.timestamp)
              |""".stripMargin)
        }
        case _ =>
      }
    } finally {
      schemaService.close()
      cache.close()
    }
  }
}
