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
package org.neo4j.spark.streaming

import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.GreaterThan
import org.apache.spark.sql.sources.LessThanOrEqual
import org.apache.spark.sql.types.StructType
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.spark.reader.BasePartitionReader
import org.neo4j.spark.service.Neo4jQueryStrategy
import org.neo4j.spark.service.PartitionPagination
import org.neo4j.spark.streaming.BaseStreamingPartitionReader.offsetUsagePatterns
import org.neo4j.spark.util.Neo4jImplicits._
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.Neo4jUtil
import org.neo4j.spark.util.QueryType._
import org.neo4j.spark.util.StreamingFrom

import java.util
import java.util.function.Predicate
import java.util.regex.Pattern

import scala.collection.JavaConverters.mapAsJavaMapConverter

class BaseStreamingPartitionReader(
  private val options: Neo4jOptions,
  private val filters: Array[Filter],
  private val schema: StructType,
  private val jobId: String,
  private val partitionSkipLimit: PartitionPagination,
  private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
  private val requiredColumns: StructType,
  private val aggregateColumns: Array[AggregateFunc]
) extends BasePartitionReader(
      options,
      filters,
      schema,
      jobId,
      partitionSkipLimit,
      scriptResult,
      requiredColumns,
      aggregateColumns
    ) {

  private val streamingPropertyName = Neo4jUtil.getStreamingPropertyName(options)

  private val streamStart =
    filters.find(f => f.getAttribute.contains(streamingPropertyName) && f.isInstanceOf[GreaterThan])

  private val streamEnd =
    filters.find(f => f.getAttribute.contains(streamingPropertyName) && f.isInstanceOf[LessThanOrEqual])

  logInfo(s"Creating Streaming Partition reader $name")

  private lazy val values = {
    val map = new util.HashMap[String, Any](super.queryParameters)
    val start: Long = streamStart
      .flatMap(f => f.getValue)
      .getOrElse(StreamingFrom.ALL.value())
      .asInstanceOf[Long]
    val end: Long = streamEnd
      .flatMap(f => f.getValue)
      .get
      .asInstanceOf[Long]
    map.put(Neo4jQueryStrategy.VARIABLE_STREAM, Map("offset" -> start, "from" -> start, "to" -> end).asJava)
    map
  }

  override def close(): Unit = {
    logInfo(s"Closing Partition reader $name ${if (hasError()) "with error " else ""}")
    super.close()
  }

  override protected def query: String = {
    options.query.queryType match {
      case QUERY =>
        val originalQuery = super.query

        if (offsetUsagePatterns.exists(_.test(originalQuery))) {
          logWarning(
            "Usage of '$stream.offset' is deprecated in favor of '$stream.from' and '$stream.to' parameters which "
              + "describes the range of changes the micro batch refers to. Please update your queries accordingly."
          )
        }

        val property = Cypher.name(streamingPropertyName)
        val stream = Cypher.parameter("stream")

        // rewrite query for adding $stream.from and $stream.to filters
        Cypher.callRawCypher(originalQuery)
          .`with`(Cypher.asterisk())
          .where(
            property.gt(stream.property("from")).and(property.lte(stream.property("to")))
          )
          .returning(Cypher.asterisk())
          .build()
          .getCypher
      // we don't need to rewrite the queries for LABELS and RELATIONSHIPS because spark filters already cover our
      // criteria which are added to the query text in Neo4jQueryService
      case LABELS       => super.query
      case RELATIONSHIP => super.query
      case GDS =>
        throw new UnsupportedOperationException("GDS strategy is not supported in structured streaming use cases.")
    }
  }

  override protected def queryParameters: util.Map[String, Any] = values

}

object BaseStreamingPartitionReader {

  private val offsetUsagePatterns: Seq[Predicate[String]] = Seq(
    Pattern.compile("\\$stream\\.offset").asPredicate(),
    Pattern.compile("\\$`stream`\\.offset").asPredicate(),
    Pattern.compile("\\$stream\\.`offset`").asPredicate(),
    Pattern.compile("\\$`stream`\\.`offset`").asPredicate()
  )

}
