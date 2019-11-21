package org.neo4j.spark.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SQLContext
import org.neo4j.spark.Neo4jGraph

import org.neo4j.spark.cypher.CypherHelpers._

/**
 * @author mh
 * @since 19.03.16
 */
object Neo4jGraphFrame {

  def apply(sqlContext: SQLContext, src: (String, String), edge: (String, String), dst: (String, String)) = {
    def nodeStmt(s: (String, String)) = s"MATCH (n:${s._1.quote}) RETURN id(n) as id, n.${s._2.quote} as prop"

    val edgeProp = if (edge._2 == null) "" else s", r.${edge._2.quote} as prop"
    val edgeStmt = s"MATCH (n:${src._1.quote})-[r:${edge._1.quote}]->(m:${dst._1.quote}) RETURN id(n) as src, id(m) as dst" + edgeProp

    val vertices1 = Neo4jDataFrame(sqlContext, nodeStmt(src), Seq.empty, ("id", "integer"), ("prop", "string"))
    val vertices2 = Neo4jDataFrame(sqlContext, nodeStmt(dst), Seq.empty, ("id", "integer"), ("prop", "string"))
    val schema = Seq(("src", "integer"), ("dst", "integer")) ++ (if (edge._2 != null) Some("prop", "string") else None)
    val edges = Neo4jDataFrame(sqlContext, edgeStmt, Seq.empty, schema: _*)

    org.graphframes.GraphFrame(vertices1.union(vertices2).distinct(), edges)
  }

  def fromGraphX(sc: SparkContext, label1: String, rels: Seq[String], label2: String) = {
    val g: Graph[Any, Int] = Neo4jGraph.loadGraph(sc, label1, rels, label2)
    org.graphframes.GraphFrame.fromGraphX(g)
  }

  def fromEdges(sqlContext: SQLContext, label1: String, rels: Seq[String], label2: String) = {
    val relTypes = rels.map(_.quote).mkString("|")
    val edgeStmt = s"MATCH (n:${label1.quote})-[r:$relTypes]->(m:${label2.quote}) RETURN id(n) as src, id(m) as dst"
    val edges = Neo4jDataFrame(sqlContext, edgeStmt, Seq.empty, ("src", "integer"), ("dst", "integer"))
    org.graphframes.GraphFrame.fromEdges(edges)
  }
}
