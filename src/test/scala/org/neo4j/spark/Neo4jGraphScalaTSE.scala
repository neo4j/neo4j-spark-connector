package org.neo4j.spark

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.junit.Assert._
import org.junit._

import scala.collection.JavaConverters._

object Neo4jGraphScalaTSE {

}

/**
  * @author mh
  * @since 17.07.16
  */
class Neo4jGraphScalaTSE extends SparkConnectorScalaBaseTSE {
  val FIXTURE: String = "CREATE (s:A {a:0})-[r:REL {foo:'bar'}]->(t:B {b:1}) RETURN id(s) AS source, id(t) AS target"

  private var source: Long = _
  private var target: Long = _

  @Before
  @throws[Exception]
  def setUp {
    val map = session().run(FIXTURE).single()
      .asMap()
    source = map.get("source").asInstanceOf[Long]
    target = map.get("target").asInstanceOf[Long]
  }

  private def assertGraph(graph: Graph[_, _], expectedNodes: Long, expectedRels: Long) = {
    assertEquals(expectedNodes, graph.vertices.count)
    assertEquals(expectedRels, graph.edges.count)
  }

  @Test def runCypherQueryWithParams {
    val data = List(Map("id"->1,"name"->"Test").asJava).asJava
    Executor.execute(sc, "UNWIND $data as row CREATE (n:Test {id:row.id}) SET n.name = row.name", Map(("data",data)))
  }
  @Test def runMatrixQuery {
    val graph = Neo4jGraph.loadGraph(sc, "A", Seq.empty, "B")
    assertGraph(graph, 2, 1)
  }

  @Test def saveGraph {
    val edges : RDD[Edge[VertexId]] = sc.makeRDD(Seq(Edge(source,target,42L)))
    val graph = Graph.fromEdges(edges,-1)
    assertGraph(graph, 2, 1)
    Neo4jGraph.saveGraph(sc,graph,null,("REL","test"))
    assertEquals(42L, session().run("MATCH (:A)-[rel:REL]->(:B) RETURN rel.test as prop").single().get("prop").asLong())
  }

  @Test def saveGraphMerge {
    val edges : RDD[Edge[Long]] = sc.makeRDD(Seq(Edge(source,target,42L)))
    val graph = Graph.fromEdges(edges,13L)
    assertGraph(graph, 2, 1)
    Neo4jGraph.saveGraph(sc,graph,"value",("FOOBAR","test"),Option("Foo","id"),Option("Bar","id"),merge = true)
    assertEquals(Map("fid"->source,"bid"->target,"rv"->42L,"fv"->13L,"bv"->13L).asJava,session().run("MATCH (foo:Foo)-[rel:FOOBAR]->(bar:Bar) RETURN {fid: foo.id, fv:foo.value, rv:rel.test,bid:bar.id,bv:bar.value} as data").single().get("data").asMap())
  }
  @Test def saveGraphByNodeLabel {
    val edges : RDD[Edge[VertexId]] = sc.makeRDD(Seq(Edge(0,1,42L)))
    val graph = Graph.fromEdges(edges,-1)
    assertGraph(graph, 2, 1)
    Neo4jGraph.saveGraph(sc,graph,null,("REL","test"),Option(("A","a")),Option(("B","b")))
    assertEquals(42L,session().run("MATCH (:A)-[rel:REL]->(:B) RETURN rel.test as prop").single().get("prop").asLong())
  }
  @Test def mergeGraphByNodeLabel {
    val edges : RDD[Edge[VertexId]] = sc.makeRDD(Seq(Edge(source,target,42L)))
    val graph = Graph.fromEdges(edges,-1)
    assertGraph(graph, 2, 1)
    Neo4jGraph.saveGraph(sc,graph,null,("REL2","test"),merge = true)
    assertEquals(42L,session().run("MATCH (:A)-[rel:REL2]->(:B) RETURN rel.test as prop").single().get("prop").asLong())
  }

  @Test def saveGraphNodes {
    val nodes : RDD[(VertexId, Long)] = sc.makeRDD(Seq((source,10L),(target,20L)))
    val edges : RDD[Edge[Long]] = sc.makeRDD(Seq())
    val graph = Graph[Long,Long](nodes,edges,-1)
    assertGraph(graph, 2, 0)
    Neo4jGraph.saveGraph(sc,graph,"prop")
    assertEquals(10L,session().run(s"MATCH (a:A) WHERE id(a) = $source RETURN a.prop as prop").single().get("prop").asLong())
    assertEquals(20L,session().run(s"MATCH (b:B) WHERE id(b) = $target RETURN b.prop as prop").single().get("prop").asLong())
  }
}
