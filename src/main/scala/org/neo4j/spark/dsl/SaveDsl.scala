package org.neo4j.spark.dsl

import org.apache.spark.graphx.Graph
import org.neo4j.spark.cypher.Pattern

import scala.reflect.ClassTag

trait SaveDsl { // todo update statistics
  //    def storeRdd[T:ClassTag](rdd:RDD[T]) : Long
  //    def storeRowRdd(rowRdd:RDD[Row]) : Long
  //    def storeNodeRdds(nodesRdd: RDD[Row]) : Long
  //    def storeRelRdd(relRdd: RDD[Row]) : Long
  def saveGraph[VD:ClassTag,ED:ClassTag](graph: Graph[VD, ED], nodeProp : String = null, pattern: Pattern = null, merge:Boolean = false) : Long
  //    def storeGraphFrame[VD:ClassTag,ED:ClassTag](graphFrame:GraphFrame) : Long
  //    def storeDataFrame(dataFrame:DataFrame) : Long
}
