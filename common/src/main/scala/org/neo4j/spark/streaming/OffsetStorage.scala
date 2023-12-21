package org.neo4j.spark.streaming

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

import java.net.URI
import java.nio.charset.StandardCharsets



class OffsetStorage(checkpointLocation: String, private val initOffset: Neo4jOffset) extends Logging with AutoCloseable {

  private val checkpointPath = new Path(URI.create(checkpointLocation))

  private val fs = FileSystem.get(URI.create(checkpointLocation), SparkContext.getOrCreate().hadoopConfiguration)

  def initialOffset(): Neo4jOffset = {
    if (exists) {
      try {
        val in = fs.open(checkpointPath)
        try {
          val fromOffset = Neo4jOffset.from(in)
          logInfo(s"Retrieving last committed offset: $fromOffset")
          fromOffset
        } catch {
          case e: Throwable => throw new RuntimeException(e)
        } finally {
          if (in != null) in.close()
        }
      }
    } else {
      commit(initOffset)
      logInfo(s"Initial offset: $initOffset")
      initOffset
    }
  }

  private def exists: Boolean = {
    try {
      fs.exists(checkpointPath)
    } catch {
      case e: Throwable => throw new RuntimeException(e)
    }
  }

  def commit(offset: Neo4jOffset): Unit = {
    val out = fs.create(checkpointPath, true)
    try {
      out.write(offset.json().getBytes(StandardCharsets.UTF_8))
      out.hflush()
    } catch {
      case e: Throwable => new RuntimeException(e)
    } finally {
      if (out != null) out.close()
    }
  }

  override def close(): Unit = fs.close()
}
