package org.neo4j.spark.dsl

import org.neo4j.spark.Neo4j

trait PartitionsDsl {
  def partitions(partitions : Long) : Neo4j
  def batch(batch : Long): Neo4j
  def rows(rows : Long): Neo4j
}
