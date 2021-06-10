package org.neo4j.spark.streaming

import org.apache.spark.sql.types.StructType

import java.util.Optional
import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiFunction

object StructTypeStreamingStorage {

  private val structTypeCache = new ConcurrentHashMap[String, Optional[StructType]]()

  def getStructTypeByJobId(jobId: String) = structTypeCache
    .compute(jobId, new BiFunction[String, Optional[StructType], Optional[StructType]] {
      override def apply(key: String, value: Optional[StructType]): Optional[StructType] = {
        if (value == null) {
          Optional.empty()
        } else {
          value
        }
      }
    })

  def setAndGetStructTypeByJobId(jobId: String, optionalSchema: Optional[StructType]) = structTypeCache
    .compute(jobId, new BiFunction[String, Optional[StructType], Optional[StructType]] {
      override def apply(key: String, value: Optional[StructType]): Optional[StructType] = {
        if (value != null && value.isPresent && !optionalSchema.isPresent) {
          value
        } else {
          optionalSchema
        }
      }
    })

  def clearForJobId(jobId: String) = structTypeCache.remove(jobId)

  def size() = structTypeCache.size()

  def clear() = structTypeCache.clear()

}
