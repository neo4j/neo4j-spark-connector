package org.neo4j.spark.util

import org.apache.spark.internal.Logging
import org.neo4j.driver.{Driver, GraphDatabase, SessionConfig}
import org.neo4j.spark.util.DriverCache.{cache, jobIdCache, serverConfig}

import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}

object DriverCache {
  private val cache: ConcurrentHashMap[Neo4jDriverOptions, Driver] = new ConcurrentHashMap[Neo4jDriverOptions, Driver]
  private val serverConfig: ConcurrentHashMap[Neo4jDriverOptions, Map[String, String]] = new ConcurrentHashMap[Neo4jDriverOptions, Map[String, String]]
  private val jobIdCache = Collections.newSetFromMap[String](new ConcurrentHashMap[String, java.lang.Boolean]())
}

class DriverCache(private val options: Neo4jDriverOptions, private val jobId: String) extends Serializable
  with AutoCloseable
  with Logging {
  def getOrCreate(): Driver = {
    this.synchronized {
      jobIdCache.add(jobId)
      cache.computeIfAbsent(options, (t: Neo4jDriverOptions) =>
        GraphDatabase.driver(t.url, t.toNeo4jAuth, t.toDriverConfig))
    }
  }

  def getServerConfig: Map[String, String] = {
    val session = getOrCreate().session(SessionConfig.forDatabase("system"))
    try {
      serverConfig.computeIfAbsent(options, (t: Neo4jDriverOptions) =>
        session.run("CALL dbms.listConfig() YIELD name, value RETURN *")
          .list()
          .asScala
          .map(_.asMap())
          .map(_.asScala)
          .map(m => (m("name").toString, m("value").toString))
          .toMap[String, String]
      )
    } finally {
      Neo4jUtil.closeSafety(session)
    }
  }

  def close(): Unit = {
    this.synchronized {
      jobIdCache.remove(jobId)
      if (jobIdCache.isEmpty) {
        serverConfig.remove(options)
        val driver = cache.remove(options)
        if (driver != null) {
          Neo4jUtil.closeSafety(driver)
        }
      }
    }
  }
}
