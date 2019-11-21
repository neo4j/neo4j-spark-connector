package org.neo4j.spark.dsl

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.graphframes.GraphFrame

import scala.reflect.ClassTag

trait LoadDsl {
  def loadRdd[T:ClassTag] : RDD[T]
  def loadRowRdd : RDD[Row]
  def loadNodeRdds : RDD[Row]
  def loadRelRdd : RDD[Row]
  def loadGraph[VD:ClassTag,ED:ClassTag] : Graph[VD,ED]
  def loadGraphFrame[VD:ClassTag,ED:ClassTag] : GraphFrame
  def loadDataFrame : DataFrame
  def loadDataFrame(schema : (String,String)*) : DataFrame
}
