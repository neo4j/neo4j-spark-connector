package org.neo4j.spark.writer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.util.Neo4jOptions

class Neo4jDataWriter(
  jobId: String,
  partitionId: Int,
  schema: StructType,
  saveMode: SaveMode,
  options: Neo4jOptions,
  scriptResult: java.util.List[java.util.Map[String, AnyRef]]
) extends BaseDataWriter(jobId, partitionId, schema, saveMode, options, scriptResult)
    with DataWriter[InternalRow] {}
