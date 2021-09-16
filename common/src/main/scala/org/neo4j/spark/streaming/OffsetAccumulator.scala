package org.neo4j.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

object OffsetAccumulator {
  def register(jobId: String): OffsetAccumulator = {
    val accumulator = new OffsetAccumulator()
    val sparkSession = SparkSession.getActiveSession
      .getOrElse(throw new RuntimeException(s"""
         |Cannot register OffsetAccumulator for $jobId,
         |there is no Spark Session active
         |""".stripMargin))
    sparkSession.sparkContext.register(accumulator, jobId)
    accumulator
  }
}

class OffsetAccumulator extends AccumulatorV2[java.lang.Long, java.lang.Long] {
  @volatile
  private var offset: java.lang.Long = null

  override def isZero: Boolean = offset == null

  override def copy(): AccumulatorV2[java.lang.Long, java.lang.Long] = {
    val copy = new OffsetAccumulator()
    copy.add(offset)
    copy
  }

  override def reset(): Unit = offset = null

  override def add(v: java.lang.Long): Unit = if (v != null && (offset == null || v > offset)) {
    offset = v
  }

  override def merge(other: AccumulatorV2[java.lang.Long, java.lang.Long]): Unit = add(other.value)

  override def value: java.lang.Long = offset
}
