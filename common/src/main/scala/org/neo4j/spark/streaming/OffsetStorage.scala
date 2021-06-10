package org.neo4j.spark.streaming

import java.util.concurrent.ConcurrentHashMap
import java.util.function

object OffsetStorage {

  private val cache = new ConcurrentHashMap[String, java.lang.Long]()

  def getLastOffset(jobId: String): java.lang.Long = cache.get(jobId)

  def setLastOffset(jobId: String,
                    neo4jOffset: java.lang.Long) = cache.compute(jobId, new function.BiFunction[String, java.lang.Long, java.lang.Long] {
    override def apply(id: String, offset: java.lang.Long): java.lang.Long = {
      if (offset == null || offset < neo4jOffset) {
        neo4jOffset
      } else {
        offset
      }
    }
  })

  def clearForJobId(jobId: String) = cache.remove(jobId)


  def size() = cache.size()

}
