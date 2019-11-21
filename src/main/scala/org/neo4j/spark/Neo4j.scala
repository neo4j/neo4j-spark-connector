package org.neo4j.spark

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.SparkContext
import org.graphframes.GraphFrame
import org.neo4j.spark.Neo4j.Query
import org.neo4j.spark.cypher.{NameProp, Pattern}
import org.neo4j.spark.dataframe.CypherTypes
import org.neo4j.spark.dsl.{LoadDsl, PartitionsDsl, QueriesDsl, SaveDsl}
import org.neo4j.spark.rdd.Neo4jRDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

object Neo4j extends Serializable {

  val UNDEFINED = Long.MaxValue

  implicit def apply(sc: SparkContext): Neo4j = {
    new Neo4j(sc)
  }

  case class Stats(nodes: Long = 0, rels: Long = 0, properties: Long = 0, indexes: Long = 0, constraints: Long = 0)

  case class Updates(created: Stats, updated: Stats, deleted: Stats)

  case class Query(query: String, params: Map[String, Any] = Map.empty) {
    def paramsSeq = params.toSeq

    def isEmpty = query == null
  }

}


class Neo4j(val sc: SparkContext) extends QueriesDsl with PartitionsDsl with LoadDsl with SaveDsl with Serializable {

  // todo
  private def sqlContext: SQLContext = new SQLContext(sc)

  var pattern: Pattern = null
  var nodes: Query = Query(null)
  var rels: Query = Query(null)

  // todo case/base class for partitions, rows, batch
  var partitions = Partitions()
  var defaultRelValue: Any = null


  // --- configure plain query

  override def cypher(cypher: String, params: Map[String, Any] = Map.empty): Neo4j = {
    this.nodes = Query(cypher, this.nodes.params ++ params)
    this
  }

  override def param(key: String, value: Any): Neo4j = {
    this.nodes = this.nodes.copy(params = this.nodes.params + (key -> value))
    this
  }

  override def params(params: Map[String, Any]): Neo4j = {
    this.nodes = this.nodes.copy(params = this.nodes.params ++ params)
    this
  }

  override def nodes(cypher: String, params: Map[String, Any]) = this.cypher(cypher, params)

  override def rels(cypher: String, params: Map[String, Any] = Map.empty) = {
    this.rels = Query(cypher, params)
    this
  }

  // --- configuring pattern

  override def pattern(source: (String, String), edge: (String, String), target: (String, String)) = {
    this.pattern = new Pattern(source, Seq(edge), target)
    this
  }

  override def pattern(source: String, edges: Seq[String], target: String) = {
    this.pattern = new Pattern(source, edges, target)
    this
  }

  // --- configure partitions

  override def rows(rows: Long) = {
    assert(rows > 0)
    this.partitions = partitions.copy(rows = rows)
    this
  }

  override def batch(batch: Long) = {
    assert(batch > 0)
    this.partitions = partitions.copy(batchSize = batch)
    this
  }

  // todo for partitions > 1, generate a batched query SKIP {_skip} LIMIT {_limit}
  // batch could be hard-coded in query, so we only have to pass skip
  override def partitions(partitions: Long): Neo4j = {
    assert(partitions > 0)
    this.partitions = this.partitions.copy(partitions = partitions)
    this
  }

  // -- output

  def loadRelRdd: RDD[Row] = {
    if (pattern != null) {
      val queries: Seq[(String, List[String])] = pattern.relQueries
      val rdds: Seq[RDD[Row]] = queries.map(query => {
        //        val maxCountQuery: () => Long = () => { query._2.map(countQuery => new Neo4jRDD(sc, countQuery).first().getLong(0)).max }
        new Neo4jRDD(sc, query._1, rels.params, partitions) // .copy(rowSource = Option(maxCountQuery)))
      })
      rdds.reduce((a, b) => a.union(b)).distinct()
    } else if (!rels.isEmpty) {
      new Neo4jRDD(sc, rels.query, rels.params, partitions)
    } else {
      throw new RuntimeException("No relationship query provided either as pattern or with rels()")
    }
  }

  private def loadNodeRdds(node: NameProp, params: Map[String, Any], partitions: Partitions) = {
    // todo use count queries
    val queries = pattern.nodeQuery(node)

    new Neo4jRDD(sc, queries._1, params, partitions)
  }


  def loadNodeRdds: RDD[Row] = {
    if (pattern != null) {
      loadNodeRdds(pattern.source, nodes.params, partitions)
        .union(loadNodeRdds(pattern.target, nodes.params, partitions)).distinct()
    } else if (!nodes.isEmpty) {
      new Neo4jRDD(sc, nodes.query, nodes.params, partitions)
    } else if (!rels.isEmpty) {
      new Neo4jRDD(sc, rels.query, rels.params, partitions)
    } else {
      throw new RuntimeException("No relationship query provided either as pattern or with cypher() or nodes()")
    }
  }

  override def loadRowRdd: RDD[Row] = loadNodeRdds

  override def loadGraph[VD: ClassTag, ED: ClassTag]: Graph[VD, ED] = {
    val nodeDefault = null.asInstanceOf[VD]
    val relDefault = defaultRelValue.asInstanceOf[ED]
    val nodeRdds: RDD[Row] = loadNodeRdds
    val rels: RDD[Edge[ED]] = loadRelRdd.map(row => new Edge[ED](row.getLong(0), row.getLong(1), if (row.size == 2) relDefault else row.getAs[ED](2)))
    if (nodeRdds == null) {
      Graph.fromEdges(rels, nodeDefault)
    } else {
      val nodes: RDD[(VertexId, VD)] = nodeRdds.map(row => (row.getLong(0), if (row.size == 1) nodeDefault else row.getAs[VD](1)))
      Graph[VD, ED](nodes, rels)
    }
  }

  override def saveGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], nodeProp: String = null, pattern: Pattern = pattern, merge: Boolean = false): Long = {
    val result = Neo4jGraph.saveGraph[VD, ED](sc, graph, merge = merge,
      nodeProp = nodeProp,
      relTypeProp = pattern.edges.head.asTuple,
      mainLabelIdProp = Some(pattern.source.asTuple),
      secondLabelIdProp = Some(pattern.target.asTuple))
    result._1 + result._2
  }

  override def loadGraphFrame[VD: ClassTag, ED: ClassTag]: GraphFrame = {
    val nodeRdds: RDD[Row] = loadRowRdd
    // todo check value type from pattern
    // val nodeSchema: StructType = CypherTypes.schemaFromNamedType(Seq(("id","integer"),("value",asInstanceOf[VD].getClass.getSimpleName)))
    val nodes: DataFrame = sqlContext.createDataFrame(nodeRdds, nodeRdds.first().schema)

    val relRdd: RDD[Row] = loadRelRdd
    // todo check value type from pattern
    // val relSchema: StructType = CypherTypes.schemaFromNamedType(Seq(("src","long"),("dst","long"),("value", asInstanceOf[ED].getClass.getSimpleName)))
    val rels: DataFrame = sqlContext.createDataFrame(relRdd, relRdd.first().schema)
    org.graphframes.GraphFrame(nodes, rels)

    /*
        val vertices1 = Neo4jDataFrame(sqlContext, nodeStmt(src),Seq.empty,("id","integer"),("prop","string"))
        val vertices2 = Neo4jDataFrame(sqlContext, nodeStmt(dst), Seq.empty, ("id", "integer"), ("prop", "string"))
        val schema = Seq(("src","integer"),("dst","integer")) ++ (if (edge._2 != null) Some("prop", "string") else None)
        val edges = Neo4jDataFrame(sqlContext, edgeStmt,Seq.empty,schema:_*)

        org.graphframes.GraphFrame(vertices1.union(vertices2).distinct(), edges)
    */

    /*
    if (pattern.source.property == null || pattern.target.property == null)
      Neo4jGraphFrame.fromEdges(sqlContext,pattern.source.name,pattern.edges.map(_.name),pattern.target.name)
    else
      Neo4jGraphFrame(sqlContext,pattern.source.asTuple,pattern.edges.head.asTuple,pattern.target.asTuple)
//    Neo4jGraphFrame.fromGraphX(sc, pattern.source.name,pattern.edges.map(_.name),pattern.target.name)
    */
  }

  override def loadDataFrame(schema: (String, String)*): DataFrame = {
    sqlContext.createDataFrame(loadRowRdd, CypherTypes.schemaFromNamedType(schema))
  }

  override def loadDataFrame: DataFrame = {
    val rowRdd: RDD[Row] = loadRowRdd
    if (rowRdd.isEmpty()) throw new RuntimeException("Cannot infer schema-types from empty result, please use loadDataFrame(schema: (String,String)*)")
    sqlContext.createDataFrame(rowRdd, rowRdd.first().schema) // todo does it empty the RDD ??
  }

  override def loadRdd[T: ClassTag]: RDD[T] = {
    loadRowRdd.map(_.getAs[T](0))
  }
}
