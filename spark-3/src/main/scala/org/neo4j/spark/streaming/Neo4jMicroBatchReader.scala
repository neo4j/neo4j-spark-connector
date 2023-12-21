package org.neo4j.spark.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.sources.{Filter, GreaterThan, LessThanOrEqual}
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util._

import java.util.Optional

class Neo4jMicroBatchReader(private val optionalSchema: Optional[StructType],
                            private val neo4jOptions: Neo4jOptions,
                            private val jobId: String,
                            private val aggregateColumns: Array[AggregateFunc],
                            private val checkpointLocation: String)
  extends MicroBatchStream
    with Logging {

  private lazy val storage = new OffsetStorage(checkpointLocation, Neo4jOffset.from(neo4jOptions))

  private val driverCache = new DriverCache(neo4jOptions.connection, jobId)

  private lazy val scriptResult = {
    val schemaService = new SchemaService(neo4jOptions, driverCache)
    schemaService.createOptimizations()
    val scriptResult = schemaService.execute(neo4jOptions.script)
    schemaService.close()
    scriptResult
  }

  private var lastUsedOffset: Neo4jOffset = null

  private var lastEndOffset: Neo4jOffset = null

  private var filters: Array[Filter] = Array.empty[Filter]

  override def deserializeOffset(json: String): Offset = Neo4jOffset.from(json)

  override def commit(end: Offset): Unit = storage.commit(end.asInstanceOf[Neo4jOffset])

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    lastEndOffset = end.asInstanceOf[Neo4jOffset]
    val prop = Neo4jUtil.getStreamingPropertyName(neo4jOptions)
    this.filters = Array(
      GreaterThan(prop, start.asInstanceOf[Neo4jOffset].value),
      LessThanOrEqual(prop, end.asInstanceOf[Neo4jOffset].value)
    )

    val partitions = Neo4jUtil.callSchemaService(
      neo4jOptions, jobId, filters,
      { schemaService => schemaService.skipLimitFromPartition(None) }
    )

    partitions
      .map(p => Neo4jStreamingPartition(p, filters))
      .toArray
  }

  override def stop(): Unit = {
    storage.close()
    new DriverCache(neo4jOptions.connection, jobId).close()
  }

  override def latestOffset(): Offset = {
    // if in the last cycle the partition returned
    // an empty result this means that start will be set equal end,
    // so we check if
    val currOffset = if (lastUsedOffset == null) {
      initialOffset().asInstanceOf[Neo4jOffset]
    } else if (lastUsedOffset == lastEndOffset) {
      // there is a database change by invoking the last offset inserted
      val neo4jOffset = Neo4jUtil.callSchemaService[Any](neo4jOptions, jobId, Array.empty, {
        schemaService =>
          try {
            schemaService.lastOffset()
          } catch {
            case _: Throwable => lastUsedOffset.value
          }
      })
      Neo4jOffset.fromOffset(neo4jOffset)
    } else {
      lastEndOffset
    }
    lastUsedOffset = currOffset
    lastUsedOffset
  }

  override def initialOffset(): Offset = storage.initialOffset()

  override def createReaderFactory(): PartitionReaderFactory = {
    new Neo4jStreamingPartitionReaderFactory(
      neo4jOptions, optionalSchema.orElse(new StructType()), jobId, scriptResult, aggregateColumns
    )
  }
}
