package org.neo4j.spark

case class Partitions(partitions: Long = 1, batchSize: Long = Neo4j.UNDEFINED, rows: Long = Neo4j.UNDEFINED, rowSource: Option[() => Long] = None) {
  def upper(v1: Long, v2: Long): Long = v1 / v2 + Math.signum(v1 % v2).asInstanceOf[Long]

  def effective(): Partitions = {
    //      if (this.rows == Neo4j.UNDEFINED) this.rows = rowSource.getOrElse(() -> Neo4j.UNDEFINED)
    if (this.batchSize == Neo4j.UNDEFINED && this.rows == Neo4j.UNDEFINED) return Partitions()
    if (this.batchSize == Neo4j.UNDEFINED) return this.copy(batchSize = upper(rows, partitions))
    if (this.rows == Neo4j.UNDEFINED) return this.copy(rows = this.batchSize * this.partitions)
    if (this.partitions == 1) return this.copy(partitions = upper(rows, batchSize))
    this
  }

  def skip(index: Int) = index * batchSize

  // if the last batch is smaller to fit the total rows
  def limit(index: Int) = {
    val remainder = rows % batchSize
    if (index < partitions - 1 || remainder == 0) batchSize else remainder
  }

  // todo move into a partitions object
  /*
      if (this.batch == Neo4j.UNDEFINED) {
    this.batch = rows / partitions + Math.signum(rows % partitions).asInstanceOf[Int]
  }
  if (rows == Neo4j.UNDEFINED) rows = partitions * batch
  else
  if (partitions == 1)
    partitions = rows / batch + Math.signum(rows % batch).asInstanceOf[Int]

  if (this.batch == Neo4j.UNDEFINED && this.rows > 0) {
    this.batch = this.rows / partitions
    if (this.rows % partitions > 0) this.batch += 1
  }
  var c = rows
  val actualBatch = if (batch == Neo4j.UNDEFINED)
    if (partitions > 1) {
      // computation callback
      if (c == Neo4j.UNDEFINED) c = new Neo4jRDD(sc, queries._2).first().getLong(0)
      (c / partitions) + Math.signum(c % partitions).toLong
    } else Neo4j.UNDEFINED
  else batch
  */
}
