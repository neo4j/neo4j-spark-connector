package org.neo4j.spark.util

import org.neo4j.driver.{Driver, GraphDatabase}
import org.neo4j.spark.util.DriverCache.{cache, jobIdCache}

import java.util.concurrent.ConcurrentHashMap
import java.util.{Collections, function}

object DriverCache {
  private val cache: ConcurrentHashMap[Neo4jDriverOptions, Driver] = new ConcurrentHashMap[Neo4jDriverOptions, Driver]
  private val jobIdCache = Collections.newSetFromMap[String](new ConcurrentHashMap[String, java.lang.Boolean]())
}

class DriverCache(private val options: Neo4jDriverOptions, private val jobId: String) extends Serializable with AutoCloseable {
  def getOrCreate(): Driver = {
    this.synchronized {
      jobIdCache.add(jobId)
      cache.computeIfAbsent(options, (t: Neo4jDriverOptions) => t.createDriver())
    }
  }

  def close(): Unit = {
    this.synchronized {
      jobIdCache.remove(jobId)
      if (jobIdCache.isEmpty) {
        val driver = cache.remove(options)
        if (driver != null) {
          Neo4jUtil.closeSafely(driver)
        }
      }
    }
  }
}
