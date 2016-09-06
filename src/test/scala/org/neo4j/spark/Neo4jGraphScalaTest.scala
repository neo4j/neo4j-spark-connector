package org.neo4j.spark

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit._
import org.neo4j.harness.{ServerControls, TestServerBuilders}
import scala.collection.JavaConverters._


/**
  * @author mh
  * @since 17.07.16
  */
class Neo4jGraphScalaTest {
  val FIXTURE: String = "CREATE (:A)-[:REL {foo:'bar'}]->(:B)"
  private var conf: SparkConf = null
  private var sc: JavaSparkContext = null
  private var server: ServerControls = null

  @Before
  @throws[Exception]
  def setUp {
    server = TestServerBuilders.newInProcessBuilder.withConfig("dbms.security.auth_enabled", "false").withFixture(FIXTURE).newServer
    conf = new SparkConf().setAppName("neoTest").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true").set("spark.neo4j.bolt.url", server.boltURI.toString)
    sc = SparkContext.getOrCreate(conf)
  }

  @After def tearDown {
    server.close
    sc.close
  }

  @Test def runCypherQueryWithParams {
    val data = List(Map("id"->1,"name"->"Test").asJava).asJava
    Neo4jGraph.execute(sc.sc, "UNWIND {data} as row CREATE (n:Test {id:row.id}) SET n.name = row.name", Seq(("data",data)))
  }
  @Test def runMatrixQuery {
    val graph = Neo4jGraph.loadGraph(sc.sc, "A", Seq.empty, "B")
    assertEquals(2, graph.vertices.count)
    assertEquals(1, graph.edges.count)
  }
  @Test def saveGraph {
    val edges : RDD[Edge[Long]] = sc.makeRDD(Seq(Edge(0,1,42L)))
    val graph = Graph.fromEdges(edges,-1)
    assertEquals(2, graph.vertices.count)
    assertEquals(1, graph.edges.count)
    Neo4jGraph.saveGraph(sc,graph,null,"test")
  }
}
