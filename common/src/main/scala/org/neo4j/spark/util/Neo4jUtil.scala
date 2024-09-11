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
package org.neo4j.spark.util

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import org.apache.spark.sql.sources._
import org.neo4j.cypherdsl.core._
import org.neo4j.driver.Session
import org.neo4j.driver.Transaction
import org.neo4j.driver.internal.retry.ExponentialBackoffRetryLogic
import org.neo4j.driver.types.Entity
import org.neo4j.driver.types.Path
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.Neo4jImplicits.EntityImplicits
import org.neo4j.spark.util.Neo4jImplicits._
import org.slf4j.Logger

import java.time.temporal.Temporal
import java.util.Properties

import scala.annotation.tailrec

object Neo4jUtil {

  val NODE_ALIAS = "n"
  private val INTERNAL_ID_FIELD_NAME = "id"
  val INTERNAL_ID_FIELD = s"<${INTERNAL_ID_FIELD_NAME}>"
  private val INTERNAL_LABELS_FIELD_NAME = "labels"
  val INTERNAL_LABELS_FIELD = s"<${INTERNAL_LABELS_FIELD_NAME}>"
  val INTERNAL_REL_ID_FIELD = s"<rel.${INTERNAL_ID_FIELD_NAME}>"
  val INTERNAL_REL_TYPE_FIELD = "<rel.type>"
  val RELATIONSHIP_SOURCE_ALIAS = "source"
  val RELATIONSHIP_TARGET_ALIAS = "target"
  val INTERNAL_REL_SOURCE_ID_FIELD = s"<${RELATIONSHIP_SOURCE_ALIAS}.${INTERNAL_ID_FIELD_NAME}>"
  val INTERNAL_REL_TARGET_ID_FIELD = s"<${RELATIONSHIP_TARGET_ALIAS}.${INTERNAL_ID_FIELD_NAME}>"
  val INTERNAL_REL_SOURCE_LABELS_FIELD = s"<${RELATIONSHIP_SOURCE_ALIAS}.${INTERNAL_LABELS_FIELD_NAME}>"
  val INTERNAL_REL_TARGET_LABELS_FIELD = s"<${RELATIONSHIP_TARGET_ALIAS}.${INTERNAL_LABELS_FIELD_NAME}>"
  val RELATIONSHIP_ALIAS = "rel"

  private val properties = new Properties()
  properties.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("neo4j-spark-connector.properties"))

  def closeSafely(autoCloseable: AutoCloseable, logger: Logger = null): Unit = {
    try {
      autoCloseable match {
        case s: Session     => if (s.isOpen) s.close()
        case t: Transaction => if (t.isOpen) t.close()
        case null           => ()
        case _              => autoCloseable.close()
      }
    } catch {
      case t: Throwable => if (logger != null) logger
          .warn(s"Cannot close ${autoCloseable.getClass.getSimpleName} because of the following exception:", t)
    }
  }

  val mapper = new ObjectMapper()
  private val module = new SimpleModule("Neo4jApocSerializer")

  module.addSerializer(
    classOf[Path],
    new JsonSerializer[Path]() {

      override def serialize(path: Path, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider): Unit =
        jsonGenerator.writeString(path.toString)
    }
  )

  module.addSerializer(
    classOf[Entity],
    new JsonSerializer[Entity]() {

      override def serialize(
        entity: Entity,
        jsonGenerator: JsonGenerator,
        serializerProvider: SerializerProvider
      ): Unit = jsonGenerator.writeObject(entity.toMap)
    }
  )

  module.addSerializer(
    classOf[Temporal],
    new JsonSerializer[Temporal]() {

      override def serialize(
        entity: Temporal,
        jsonGenerator: JsonGenerator,
        serializerProvider: SerializerProvider
      ): Unit = jsonGenerator.writeRaw(entity.toString)
    }
  )
  mapper.registerModule(module)

  def isLong(str: String): Boolean = {
    if (str == null) {
      false
    } else {
      try {
        str.trim.toLong
        true
      } catch {
        case nfe: NumberFormatException => false
        case t: Throwable               => throw t
      }
    }
  }

  def connectorVersion: String = properties.getOrDefault("version", "UNKNOWN").toString

  def connectorEnv: String = Option(System.getenv("DATABRICKS_RUNTIME_VERSION"))
    .map(_ => "databricks")
    .getOrElse("spark")

  def getCorrectProperty(container: PropertyContainer, attribute: String): Property = {
    container.property(attribute.split('.'): _*)
  }

  def paramsFromFilters(filters: Array[Filter]): Map[String, Any] = {
    filters.flatMap(f => f.flattenFilters).map(_.getAttributeAndValue)
      .filter(_.nonEmpty)
      .map(valAndAtt => valAndAtt.head.toString.unquote() -> toParamValue(valAndAtt(1)))
      .toMap
  }

  def toParamValue(value: Any): Any = {
    value match {
      case date: java.sql.Date           => date.toString
      case timestamp: java.sql.Timestamp => timestamp.toLocalDateTime
      case _                             => value
    }
  }

  def valueToCypherExpression(attribute: String, value: Any): Expression = {
    val parameter = Cypher.parameter(attribute.toParameterName(value))
    value match {
      case d: java.sql.Date      => Functions.date(parameter)
      case t: java.sql.Timestamp => Functions.localdatetime(parameter)
      case _                     => parameter
    }
  }

  def mapSparkFiltersToCypher(
    filter: Filter,
    container: PropertyContainer,
    attributeAlias: Option[String] = None
  ): Condition = {
    filter match {
      case eqns: EqualNullSafe =>
        val parameter = valueToCypherExpression(eqns.attribute, eqns.value)
        val property = getCorrectProperty(container, attributeAlias.getOrElse(eqns.attribute))
        property.isNull.and(parameter.isNull)
          .or(property.isEqualTo(parameter))
      case eq: EqualTo =>
        getCorrectProperty(container, attributeAlias.getOrElse(eq.attribute))
          .isEqualTo(valueToCypherExpression(eq.attribute, eq.value))
      case gt: GreaterThan =>
        getCorrectProperty(container, attributeAlias.getOrElse(gt.attribute))
          .gt(valueToCypherExpression(gt.attribute, gt.value))
      case gte: GreaterThanOrEqual =>
        getCorrectProperty(container, attributeAlias.getOrElse(gte.attribute))
          .gte(valueToCypherExpression(gte.attribute, gte.value))
      case lt: LessThan =>
        getCorrectProperty(container, attributeAlias.getOrElse(lt.attribute))
          .lt(valueToCypherExpression(lt.attribute, lt.value))
      case lte: LessThanOrEqual =>
        getCorrectProperty(container, attributeAlias.getOrElse(lte.attribute))
          .lte(valueToCypherExpression(lte.attribute, lte.value))
      case in: In =>
        getCorrectProperty(container, attributeAlias.getOrElse(in.attribute))
          .in(valueToCypherExpression(in.attribute, in.values))
      case startWith: StringStartsWith =>
        getCorrectProperty(container, attributeAlias.getOrElse(startWith.attribute))
          .startsWith(valueToCypherExpression(startWith.attribute, startWith.value))
      case endsWith: StringEndsWith =>
        getCorrectProperty(container, attributeAlias.getOrElse(endsWith.attribute))
          .endsWith(valueToCypherExpression(endsWith.attribute, endsWith.value))
      case contains: StringContains =>
        getCorrectProperty(container, attributeAlias.getOrElse(contains.attribute))
          .contains(valueToCypherExpression(contains.attribute, contains.value))
      case notNull: IsNotNull   => getCorrectProperty(container, attributeAlias.getOrElse(notNull.attribute)).isNotNull
      case isNull: IsNull       => getCorrectProperty(container, attributeAlias.getOrElse(isNull.attribute)).isNull
      case not: Not             => mapSparkFiltersToCypher(not.child, container, attributeAlias).not()
      case filter @ (_: Filter) => throw new IllegalArgumentException(s"Filter of type `$filter` is not supported.")
    }
  }

  def getStreamingPropertyName(options: Neo4jOptions): String = options.query.queryType match {
    case QueryType.RELATIONSHIP => s"rel.${options.streamingOptions.propertyName}"
    case _                      => options.streamingOptions.propertyName
  }

  def callSchemaService[T](
    neo4jOptions: Neo4jOptions,
    jobId: String,
    filters: Array[Filter],
    function: SchemaService => T
  ): T = {
    val driverCache = new DriverCache(neo4jOptions.connection, jobId)
    val schemaService = new SchemaService(neo4jOptions, driverCache, filters)
    var hasError = false
    try {
      function(schemaService)
    } catch {
      case e: Throwable => {
        hasError = true
        throw e
      }
    } finally {
      schemaService.close()
      if (hasError) {
        driverCache.close()
      }
    }
  }

  @tailrec
  def isRetryableException(exception: Throwable): Boolean = {
    if (exception == null) {
      false
    } else
      ExponentialBackoffRetryLogic.isRetryable(exception) || isRetryableException(
        exception.getCause
      )
  }

}
