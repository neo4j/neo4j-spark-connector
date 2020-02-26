package org.neo4j.spark.rdd

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.neo4j.driver.{Transaction, TransactionWork}
import org.neo4j.spark.Neo4jConfig
import org.neo4j.spark.utils.Neo4jUtils._

import scala.collection.JavaConverters._

class Neo4jRowRDD(@transient sc: SparkContext, val query: String, val parameters: Seq[(String, Any)])
  extends RDD[Row](sc, Nil) {

  private val config = Neo4jConfig(sc.getConf)

  def convert(value: AnyRef) = value match {
    case m: java.util.Map[_, _] => m.asScala
    case _ => value
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val driver = config.driver()
    val session = driver.session(config.sessionConfig())
    try {
      val txWork = new TransactionWork[Iterator[Row]] {
        override def execute(tx: Transaction): Iterator[Row] = {
          val result = tx.run(query, parameters.toMap.mapValues(_.asInstanceOf[AnyRef]).asJava)
          result.list().asScala.map(record => {
            val keyCount = record.size()
            val res = if (keyCount == 0) Row.empty
            else if (keyCount == 1) Row(convert(record.get(0).asObject()))
            else {
              val row = new Array[Any](keyCount)
              var i = 0
              while (i < keyCount) {
                row.update(i, convert(record.get(i).asObject()))
                i = i + 1
              }
              Row.fromSeq(row.toSeq)
            }
            res
          }).iterator
        }
      }
      session.readTransaction(txWork)
    } finally {
      close(driver, session)
    }
  }

  override protected def getPartitions: Array[Partition] = Array(new Neo4jPartition())
}

object Neo4jRowRDD {
  def apply(sc: SparkContext, query: String, parameters: Seq[(String, Any)] = Seq.empty) = new Neo4jRowRDD(sc, query, parameters)
}
