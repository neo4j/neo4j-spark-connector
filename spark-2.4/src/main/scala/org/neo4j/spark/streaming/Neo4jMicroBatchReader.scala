package org.neo4j.spark.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.reader.{InputPartition, SupportsPushDownFilters}
import org.apache.spark.sql.sources.{Filter, GreaterThan}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Neo4jUtil, StreamingFrom, ValidateRead, ValidateReadStreaming, Validations}

import java.util.function.Supplier
import java.util.{Collections, Optional}
import java.{lang, util}
import scala.collection.JavaConverters._

class Neo4jMicroBatchReader(private val optionalSchema: Optional[StructType],
                            private val options: DataSourceOptions,
                            private val jobId: String)
  extends MicroBatchReader
    with SupportsPushDownFilters
    with Logging {

  private val neo4jOptions: Neo4jOptions = new Neo4jOptions(options.asMap())
  Validations.validate(ValidateRead(neo4jOptions, jobId), ValidateReadStreaming(neo4jOptions, jobId))

  private val offsetAccumulator: OffsetStorage[java.lang.Long, java.lang.Long] = OffsetStorage.register(jobId, null, neo4jOptions)

  private var filters: Array[Filter] = Array[Filter]()

  private var startOffset: Neo4jOffset24 = null

  private var endOffset: Neo4jOffset24 = null

  private var startedExecution: Boolean = false

  if (neo4jOptions.streamingOptions.cleanStructTypeStorage) {
    StructTypeStreamingStorage.clear()
  }

  private val schema = StructTypeStreamingStorage.setAndGetStructTypeByJobId(jobId, optionalSchema)
    .orElseGet(new Supplier[StructType] {
      override def get(): StructType = Neo4jUtil.callSchemaService(neo4jOptions, jobId, filters,
        { schemaService => schemaService.struct() })
    })

  override def readSchema(): StructType = schema

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    setStartOffset(start)
    setEndOffset(end)
  }

  private def setEndOffset(end: Optional[Offset]) = {
    // if in the last cycle the partition returned
    // an empty result this means that start will be set equal end,
    // so we check if
    val lastReadOffset: lang.Long = offsetAccumulator.value
    endOffset = if (lastReadOffset == null) {
      end
        .orElseGet(new Supplier[Offset] {
          override def get(): Offset = {
            Neo4jOffset24(startOffset.offset)
          }
        })
        .asInstanceOf[Neo4jOffset24]
    } else {
      Neo4jOffset24(lastReadOffset)
    }

    // if in the last cycle the partition returned
    // an empty result this means that start will be set equal end,
    // so we check if
    if (startOffset.offset == endOffset.offset) {
      // there is a database change by invoking the last offset inserted
      val lastNeo4jOffset = Neo4jUtil.callSchemaService[Long](neo4jOptions, jobId, filters, {
        schemaService =>
          try {
            schemaService.lastOffset()
          } catch {
            case _ => -1L
          }
      })
      // if a the last offset into the database is changed
      if (lastNeo4jOffset > endOffset.offset) {
        // we just increment the end offset in order to push spark to do a new query over the database
        endOffset = Neo4jOffset24(endOffset.offset + 1)
      }
    }
  }

  private def setStartOffset(start: Optional[Offset]) = {
    startOffset = start
      .orElseGet(new Supplier[Offset] {
        override def get(): Offset = Neo4jOffset24(neo4jOptions.streamingOptions.from.value())
      })
      .asInstanceOf[Neo4jOffset24]
  }

  override def getStartOffset: Offset = startOffset

  override def getEndOffset: Offset = endOffset

  override def deserializeOffset(json: String): Offset = Neo4jOffset24(json.toLong)

  override def commit(end: Offset): Unit = ()

  override def planInputPartitions: util.ArrayList[InputPartition[InternalRow]] = {
    startedExecution = true
    val filters = if (startOffset.offset != StreamingFrom.ALL.value()) {
      val prop = Neo4jUtil.getStreamingPropertyName(neo4jOptions)
      this.filters :+ GreaterThan(prop, getEndOffset.asInstanceOf[Neo4jOffset24].offset)
    } else {
      this.filters
    }
    val numPartitions = Neo4jUtil.callSchemaService(neo4jOptions, jobId, filters,
      { schemaService => schemaService.skipLimitFromPartition() })

    val partitions = numPartitions
      .map(partitionSkipLimit => new Neo4jStreamingInputPartition(neo4jOptions, filters, schema, jobId,
        partitionSkipLimit, Collections.emptyList(), offsetAccumulator, new StructType()))
      .toList
      .asJavaCollection
    new util.ArrayList[InputPartition[InternalRow]](partitions)
  }

  override def stop(): Unit = {
    if (startedExecution) {
      StructTypeStreamingStorage.clearForJobId(jobId)
    }
    offsetAccumulator.close()
    new DriverCache(neo4jOptions.connection, jobId).close()
  }

  override def pushFilters(filtersArray: Array[Filter]): Array[Filter] = {
    if (neo4jOptions.pushdownFiltersEnabled) {
      filters = filtersArray
    }

    filtersArray
  }

  override def pushedFilters(): Array[Filter] = filters
}
