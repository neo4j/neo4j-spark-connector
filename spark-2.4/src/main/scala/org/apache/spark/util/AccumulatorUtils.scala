package org.apache.spark.util

import org.neo4j.spark.streaming.OffsetAccumulator

object AccumulatorUtils {

  def get(id: Long): OffsetAccumulator = AccumulatorContext.get(id)
    .get
    .asInstanceOf[OffsetAccumulator]
}
