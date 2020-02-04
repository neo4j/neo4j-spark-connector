package org.neo4j.spark.rdd

import java.util

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.neo4j.driver.{Driver, Result, Transaction, TransactionWork}
import org.neo4j.spark.Executor.{CypherResult, EMPTY, rows, toJava}
import org.neo4j.spark.Neo4jConfig
import org.neo4j.spark.dataframe.CypherTypes

import scala.collection.JavaConverters._

class Neo4jTupleRDD(@transient sc: SparkContext, val query: String, val parameters: Seq[(String, AnyRef)])
  extends RDD[Seq[(String, AnyRef)]](sc, Nil) {

  private val config = Neo4jConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[Seq[(String, AnyRef)]] = {
    val driver: Driver = config.driver()
    val session = driver.session(config.sessionConfig())
    try {
      val runner = new TransactionWork[Iterator[Seq[(String, AnyRef)]]]() {
        override def execute(tx: Transaction): Iterator[Seq[(String, AnyRef)]] = {
          val result: Result = tx.run(query, parameters.toMap.asJava)
          result.list().asScala.map((record) => {
            record.asMap().asScala.toSeq
          })
        }.iterator
      }
      session.readTransaction(runner)
    } finally {
      if (session.isOpen) session.close()
      driver.close()
    }
  }

  override protected def getPartitions: Array[Partition] = Array(new Neo4jPartition())
}

object Neo4jTupleRDD {
  def apply(sc: SparkContext, query: String, parameters: Seq[(String, AnyRef)] = Seq.empty) = new Neo4jTupleRDD(sc, query, parameters)
}


