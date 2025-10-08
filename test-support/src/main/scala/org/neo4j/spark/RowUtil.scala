package org.neo4j.spark

import org.apache.spark.sql.Row

object RowUtil {
  def getByName[T](row: Row, name: String): T = row.getAs[T](row.fieldIndex(name))
}
