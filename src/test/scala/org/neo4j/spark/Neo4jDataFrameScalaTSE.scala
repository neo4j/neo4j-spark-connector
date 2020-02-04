package org.neo4j.spark

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.junit.Assert._
import org.junit._
import org.neo4j.spark.dataframe.Neo4jDataFrame


/**
  * @author mh
  * @since 17.07.16
  */

class Neo4jDataFrameScalaTSE extends SparkConnectorScalaBaseTSE {
  val FIXTURE: String = "CREATE (:A)-[:REL {foo:'bar'}]->(:B)"

  @Before
  @throws[Exception]
  def setUp {
    session().run(FIXTURE).consume()
  }

  @Test def mergeEdgeList {
    val rows = sc.makeRDD(Seq(Row("Keanu", "Matrix")))
    val schema = StructType(Seq(StructField("name", DataTypes.StringType), StructField("title", DataTypes.StringType)))
    val df = new SQLContext(sc).createDataFrame(rows, schema)
    Neo4jDataFrame.mergeEdgeList(sc, df, ("Person",Seq("name")),("ACTED_IN",Seq.empty),("Movie",Seq("title")))

    val count = session().run("MATCH (:Person {name:'Keanu'})-[:ACTED_IN]->(:Movie {title:'Matrix'}) RETURN count(*) as c").single().get("c").asLong()
    assertEquals(1L, count)
  }

  @Test def mergeEdgeListWithRename {
    val rows = sc.makeRDD(Seq(Row("Carrie-Anne", "Foster")))
    val schema = StructType(Seq(StructField("src_name", DataTypes.StringType), StructField("dst_name", DataTypes.StringType)))
    val df = new SQLContext(sc).createDataFrame(rows, schema)
    val rename = Map("src_name" -> "name", "dst_name" -> "name")
    Neo4jDataFrame.mergeEdgeList(sc, df, ("Person",Seq("src_name")),("ACTED_WITH",Seq.empty),("Person",Seq("dst_name")),rename)

    val count = session().run("MATCH p=(:Person {name:'Carrie-Anne'})-[:ACTED_WITH]->(:Person {name:'Foster'}) RETURN count(*) as c").single().get("c").asLong()
    assertEquals(1L, count)
  }

  @Test def mergeEdgeListWithRelProperties {
    val rows = sc.makeRDD(Seq(Row(Seq("Laurence"), Seq("Keanu"), "Mentor", Seq("1980"))))
    val schema = StructType(Seq(
      StructField("src_name", DataTypes.createArrayType(DataTypes.StringType, false)),
      StructField("dst_name", DataTypes.createArrayType(DataTypes.StringType, false)),
      StructField("screen", DataTypes.StringType),
      StructField("met", DataTypes.createArrayType(DataTypes.StringType, false))
    ))
    val df = new SQLContext(sc).createDataFrame(rows, schema)
    val rename = Map("src_name" -> "name", "dst_name" -> "name")
    Neo4jDataFrame.mergeEdgeList(sc, df, ("Person", Seq("src_name")), ("ACTED_WITH", Seq("screen", "met")), ("Person", Seq("dst_name")), rename)

    val count = session().run("MATCH p=(:Person {name: ['Laurence']})-[:ACTED_WITH {screen: 'Mentor', met: ['1980']}]->(:Person {name:['Keanu']}) RETURN count(*) as c").single().get("c").asLong()
    assertEquals(1L, count)
  }

  @Test def createNodes {
    val rows = sc.makeRDD(Seq(Row("Laurence", "Fishburne")))
    val schema = StructType(Seq(StructField("name", DataTypes.StringType), StructField("lastname", DataTypes.StringType)))
    val df = new SQLContext(sc).createDataFrame(rows, schema)
    Neo4jDataFrame.createNodes(sc, df, ("Person",Seq("name","lastname")))

    val count = session().run("MATCH (:Person {name:'Laurence', lastname: 'Fishburne'}) RETURN count(*) as c").single().get("c").asLong()
    assertEquals(1L, count)
  }

  @Test def createNodesComplexProperties {
    val rows = sc.makeRDD(Seq(Row(Seq("Laurence", "Fishburne"))))
    val schema = StructType(Seq(StructField("names", DataTypes.createArrayType(DataTypes.StringType, false))))
    val df = new SQLContext(sc).createDataFrame(rows, schema)
    Neo4jDataFrame.createNodes(sc, df, ("Person",Seq("names")))

    val count = session().run("MATCH (:Person {names: ['Laurence', 'Fishburne']}) RETURN count(*) as c").single().get("c").asLong()
    assertEquals(1L, count)
  }

  @Test def createNodesWithRename {
    val rows = sc.makeRDD(Seq(Row("Matt", "Doran")))
    val schema = StructType(Seq(StructField("node_name", DataTypes.StringType), StructField("lastname", DataTypes.StringType)))
    val df = new SQLContext(sc).createDataFrame(rows, schema)
    val rename = Map("node_name" -> "name")
    Neo4jDataFrame.createNodes(sc, df, ("Person",Seq("node_name","lastname")), rename)

    val count = session().run("MATCH (:Person {name:'Matt', lastname: 'Doran'}) RETURN count(*) as c").single().get("c").asLong()
    assertEquals(1L, count)
  }
}
