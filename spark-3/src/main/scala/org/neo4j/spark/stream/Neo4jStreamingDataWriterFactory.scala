package org.neo4j.spark.stream

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.util.{DriverCache, Neo4jOptions}
import org.neo4j.spark.writer.Neo4jDataWriter

class Neo4jStreamingDataWriterFactory(jobId: String,
                                      structType: StructType,
                                      saveMode: SaveMode,
                                      options: Neo4jOptions,
                                      scriptResult: java.util.List[java.util.Map[String, AnyRef]])
  extends  StreamingDataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = new Neo4jDataWriter(
    jobId,
    partitionId,
    structType,
    saveMode,
    options,
    scriptResult
  )
}
