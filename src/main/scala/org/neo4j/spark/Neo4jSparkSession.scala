package org.neo4j.spark

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.{types, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
 * @author Mageswaran Dhandapani
 * @since 09.07.16
 */
class Neo4jSparkSession private (sc: SparkContext,
                                 sqlContext: SQLContext) {

  /**
   *
   * Contains all Neo4j RDD operations
   */
  object RDD {

    /**
     * Constructs a Tuple RDD
     * @param cquery Cypher query with in-build optional parameters
     * @param parameters
     * @example  import org.neo4j.spark.Neo4jSparkSession
     *           Neo4jSparkSession.builder.getOrCreate().RDD.toTuple("MATCH (n) return id(n)",Seq.empty).count
     * @return Seq[(String,AnyRef)] per row
     */
    def toTuple(cquery: String, parameters: Seq[(String, AnyRef)]) = {
      Neo4jTupleRDD(sc, cquery, parameters)
    }

    /**
     * Constructs Spark SQL Row
     * @param cquery Cypher query with in-build optional parameters
     * @param parameters
     * @example Neo4jSparkSession.builder.getOrCreate().RDD.toRow("MATCH (n) where id(n) < {maxId} return id(n)",Seq("maxId" -> 100000)).count
     * @return spark-sql Row per row
     * @example  import org.neo4j.spark.Neo4jSparkSession
     *           Neo4jSparkSession.builder.getOrCreate().RDD.
     *             toRow("MATCH (n) where id(n) < {maxId} return id(n)",Seq("maxId" -> 100000)).count
     */
    def toRow(cquery: String, parameters: Seq[(String, Any)]) = {
      Neo4jRowRDD(sc, cquery, parameters)
    }
  }


  /**
   * Constructs SparkSQL DataFrame either with explicit type information about result
   * names and types or inferred from the first result-row
   *
   * Contains all Neo4j DataFrame operations
   */
  object DF {
    /**
     * @param cquery Cypher query with in-build optional parameters
     * @param parameters
     * @param schema
     * @return
     * @example  import org.apache.spark.sql.types._
     *           import org.neo4j.spark.Neo4jSparkSession
     *           Neo4jSparkSession.builder.getOrCreate().DF.withDataType("MATCH (n) return id(n) as id",Seq.empty, "id" -> LongType)
     */
    def withSQLType(cquery: String, parameters: Seq[(String, Any)], schema: (String, types.DataType)) = {
      Neo4jDataFrame.withDataType(sqlContext, cquery, parameters, schema)
    }

    /**
     *
     * @param cquery Cypher query with in-build optional parameters
     * @param parameters
     * @param schema
     * @return
     * @example     import org.apache.spark.sql.types._
     *              import org.neo4j.spark.Neo4jSparkSession
     *              Neo4jSparkSession.builder.getOrCreate().DF.
     *                query("MATCH (n) WHERE id(n) < {maxId} return n.name as name",Seq("maxId" -> 100000),"name" -> "string").count
     */
    def query(cquery: String, parameters: Seq[(String, Any)], schema: (String, String)) = {
      Neo4jDataFrame(sqlContext, cquery, parameters, schema)
    }

  }

  /**
   * Contains all Neo4j Spark Graph operations
   */
  object Graph{
    def loadGraph[VD:ClassTag,ED:ClassTag](nodeStmt: (String,Seq[(String, AnyRef)]), relStmt: (String,Seq[(String, AnyRef)])) :Graph[VD,ED] = {
      Neo4jGraph.loadGraph(sc, nodeStmt, relStmt)
    }

    def loadGraph[VD:ClassTag,ED:ClassTag](nodeStmt: String, relStmt: String) :Graph[VD,ED] = {
      Neo4jGraph.loadGraph(sc,nodeStmt, relStmt)
    }


    def loadGraph(label1: String, relTypes: Seq[String], label2: String) : Graph[Any,Int] = {
      Neo4jGraph.loadGraph(sc,label1, relTypes, label2)
    }

    def loadGraphFromRels[VD:ClassTag,ED:ClassTag](statement: String, parameters: Seq[(String, AnyRef)], defaultValue : VD = Nil) :Graph[VD,ED] = {
      Neo4jGraph.loadGraphFromRels(sc, statement, parameters, defaultValue)
    }

    def loadGraphFromNodePairs[VD:ClassTag](statement: String, parameters: Seq[(String, AnyRef)] = Seq.empty, defaultValue : VD = Nil) :Graph[VD, Int] = {
      Neo4jGraph.loadGraphFromNodePairs(sc, statement, parameters, defaultValue)
    }

    def saveGraph[VD:ClassTag,ED:ClassTag](graph: Graph[VD,ED], nodeProp : String = null, relProp: String = null) : (Long,Long) = {
      Neo4jGraph.saveGraph(sc, graph, nodeProp, relProp)
    }

  }
}

/**
 * @author Mageswaran Dhandapani
 * @since 09.07.16
 *
 * Case 1: Retrieves spark-shell context
 * Case 2: Retrieves application context
 * Case 3: New context is created with user given bolt password
 *
 * @example val ns = Neo4jSparkSession.builder.sparkContext(sc).boltPassword("neo4j").getOrCreate()
 */
object Neo4jSparkSession {

  class Builder {

    val session: Option[Neo4jSparkSession] = None

    private[this] var userSpecifiedContext: Option[SparkContext] = None
    private[this] var userSpecifiedSqlContext: Option[SQLContext] = None
    private[this] var userSpecifiedSparkConf: Option[SparkConf] = None
    private[this] var userSpecifiedBoltPassword: Option[String] = None
    //private[this] var userSpecifiedSession: Option[SparkSession] = None //TODO SparkSession

    val sparkContext = userSpecifiedContext.getOrElse {
      val sparkConf =userSpecifiedSparkConf.getOrElse {
        //User password is used when started inside an application
        new SparkConf().set("spark.neo4j.bolt.password", userSpecifiedBoltPassword.getOrElse(""))
      }

      SparkContext.getOrCreate{
        sparkConf
      }
    }

    val sqlContext = userSpecifiedSqlContext.getOrElse {
      new SQLContext(sparkContext)
    }

    def sparkContext(sc: SparkContext) = synchronized {
      userSpecifiedContext = Option(sc)
      this
    }

    def sqlContext(sqlContext: SQLContext) = synchronized {
      userSpecifiedSqlContext = Option(sqlContext)
      this
    }

    def sparkConfig(config: SparkConf) = {
      userSpecifiedSparkConf = Option(config)
      this
    }

    def boltPassword(pw: String) = {
      userSpecifiedBoltPassword = Option(pw)
      this
    }

    def getOrCreate() = synchronized {
      session.getOrElse(new Neo4jSparkSession(sparkContext, sqlContext))
    }

  }

  val builder: Builder = new Builder

}
