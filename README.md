# Neo4j Connector to Apache Spark based on Neo4j 3.0's Bolt protocol

These are the beginnings of a Connector from Neo4j to Apache Spark 2.1 using the new binary protocol for Neo4j, Bolt.

Find [more information](http://neo4j.com/docs/developer-manual/current/#driver-manual-index) about the Bolt protocol, available drivers and documentation.

Please note that I still know very little about Apache Spark and might have done really dumb things.
Please let me know by [creating an issue](https://github.com/neo4j-contrib/neo4j-spark-connector/issues) or even better [submitting a pull request](https://github.com/neo4j-contrib/neo4j-spark-connector/pulls) to this repo.

## License

This neo4j-spark-connector is Apache 2 Licensed

## Building


Build `target/neo4j-spark-connector_2.11-full-2.1.0-M4.jar` for Scala 2.11

    mvn clean package

## Integration with Apache Spark Applications

**spark-shell, pyspark, or spark-submit**

`$SPARK_HOME/bin/spark-shell --jars neo4j-spark-connector_2.11-full-2.1.0-M4.jar`

`$SPARK_HOME/bin/spark-shell --packages neo4j-contrib:neo4j-spark-connector:2.1.0-M4`

**sbt**

If you use the [sbt-spark-package plugin](https://github.com/databricks/sbt-spark-package), in your sbt build file, add:

```scala spDependencies += "neo4j-contrib/neo4j-spark-connector:2.1.0-M4"```

Otherwise,

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4"
```  

**maven**  
In your pom.xml, add:   

```xml
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>neo4j-contrib</groupId>
    <artifactId>neo4j-spark-connector</artifactId>
    <version>2.1.0-M4</version>
  </dependency>
</dependencies>
<repositories>
  <!-- list of other repositories -->
  <repository>
    <id>SparkPackagesRepo</id>
    <url>http://dl.bintray.com/spark-packages/maven</url>
  </repository>
</repositories>
```

## Config

If you're running Neo4j on localhost with the default ports, you onl have to configure your password in `spark.neo4j.bolt.password=<password>`.
	
Otherwise set the `spark.neo4j.bolt.url` in your `SparkConf` pointing e.g. to `bolt://host:port`.

You can provide user and password as part of the URL `bolt://neo4j:<password>@localhost` or individually in `spark.neo4j.bolt.user` and `spark.neo4j.bolt.password`.


## Builder API

Starting with version 2.1.0-M4 you can use a fluent builder API to declare the queries or patterns you want to use, but also **partitions, total-rows and batch-sizes** and then select which Apache Spark Type to load.

This library supports:

* `RDD[Row], RDD[T]` (loadRowR)
* `DataFrame`
* GraphX `Graph`
* `GraphFrame`

The general usage is

1. create `org.neo4j.spark.Neo4j(sc)`
2. set `cypher(query,[params])`,`nodes(query,[params])`,`rels(query,[params])` as direct query, or </br>
   `pattern("Label1",Seq("REL"),"Label2")` or `pattern(("Label1","prop1",("REL","prop"),("Label2","prop2))`
3. optionally define `partitions(n)`, `batch(size)`, `rows(count)` for parallelism
4. choose which datatype to return
   * `loadRowRdd`, `loadNodeRdds`, `loadRelRdd`, `loadRdd[T]`
   * `loadDataFrame`,`loadDataFrame(schema)`
   * `loadGraph[VD,ED]`
   * `loadGraphFrame[VD,ED]`
5. execute Spark Operations
6. save graph back:
   * `saveGraph(grap, [pattern],[nodeProp],[merge=false])`
   
For Example:

```scala

org.neo4j.spark.Neo4j(sc).cypher("MATCH (n:Person) RETURN n.name").partitions(5).batch(10000).loadRowRdd

```
   

## Usage Examples


### Create Test Data in Neo4j

```cypher
UNWIND range(1,100) as id
CREATE (p:Person {id:id}) WITH collect(p) as people
UNWIND people as p1
UNWIND range(1,10) as friend
WITH p1, people[(p1.id + friend) % size(people)] as p2
CREATE (p1)-[:KNOWS {years: abs(p2.id - p2.id)}]->(p2)
```

Start the Spark-Shell with

`$SPARK_HOME/bin/spark-shell --packages neo4j-contrib:neo4j-spark-connector:2.1.0-M4`

### Loading RDDs

```scala
import org.neo4j.spark._

val neo = Neo4j(sc)

val rdd = neo.cypher("MATCH (n:Person) RETURN id(n) as id ").loadRowRdd
rdd.count

// inferred schema
rdd.first.schema.fieldNames
//   => ["id"]
rdd.first.schema("id") 
//   => StructField(id,LongType,true)

neo.cypher("MATCH (n:Person) RETURN id(n)").loadRdd[Long].mean
//   => res30: Double = 236696.5

neo.cypher("MATCH (n:Person) WHERE n.id <= {maxId} RETURN n.id").param("maxId", 10).loadRowRdd.count
//   => res34: Long = 10

// provide partitions and batch-size
neo.nodes("MATCH (n:Person) RETURN id(n) SKIP {_skip} LIMIT {_limit}").partitions(4).batch(25).loadRowRdd.count
//   => 100 == 4 * 25

// load via pattern
neo.pattern("Person",Seq("KNOWS"),"Person").rows(80).batch(21).loadNodeRdds.count
//   => 80 = b/c 80 rows given

// load relationships via pattern
neo.pattern("Person",Seq("KNOWS"),"Person").partitions(12).batch(100).loadRelRdd.count
//   => 1000
```

### Loading DataFrames

```scala
import org.neo4j.spark._

val neo = Neo4j(sc)

// load via Cypher query
neo.cypher("MATCH (n:Person) RETURN id(n) as id SKIP {_skip} LIMIT {_limit}").partitions(4).batch(25).loadDataFrame.count
//   => res36: Long = 100

val df = neo.pattern("Person",Seq("KNOWS"),"Person").partitions(12).batch(100).loadDataFrame
//   => org.apache.spark.sql.DataFrame = [id: bigint]

// TODO loadRelDataFrame
```

### Loading GraphX Graphs

```scala
import org.neo4j.spark._

val neo = Neo4j(sc)

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
    
// load graph via Cypher query
val graphQuery = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN id(n) as source, id(m) as target, type(r) as value SKIP {_skip} LIMIT {_limit}"
val graph: Graph[Long, String] = neo.rels(graphQuery).partitions(7).batch(200).loadGraph

graph.vertices.count
//    => 100
graph.edges.count
//    => 1000

// load graph via pattern
val graph = neo.pattern(("Person","id"),("KNOWS","since"),("Person","id")).partitions(7).batch(200).loadGraph[Long,Long]

val graph2 = PageRank.run(graph, 5)
//    => graph2: org.apache.spark.graphx.Graph[Double,Double] =    

graph2.vertices.sort(_._2).take(3)
//    => res46: Array[(org.apache.spark.graphx.VertexId, Long)] 
//    => Array((236746,100), (236745,99), (236744,98))

// uses pattern from above to save the data, merge parameter is false by default, only update existing nodes
neo.saveGraph(graph, "rank")
// uses pattern from parameter to save the data, merge = true also create new nodes and relationships
neo.saveGraph(graph, "rank",Pattern(("Person","id"),("FRIEND","years"),("Person","id")), merge = true)
```


### Loading GraphFrames

```scala
import org.neo4j.spark._

val neo = Neo4j(sc)

import org.graphframes._

val graphFrame = neo.pattern(("Person","id"),("KNOWS",null), ("Person","id")).partitions(3).rows(1000).loadGraphFrame

graphFrame.vertices.count
//     => 100
graphFrame.edges.count
//     => 1000

val pageRankFrame = graphFrame.pageRank.maxIter(5).run()
val ranked = pageRankFrame.vertices
ranked.printSchema()

val top3 = ranked.orderBy(ranked.col("pagerank").desc).take(3)
//     => top3: Array[org.apache.spark.sql.Row] 
//     => Array([236716,70,0.62285...], [236653,7,0.62285...], [236658,12,0.62285])


// example loading a graph frame with two dedicated Cypher statements
val nodesQuery = "match (n:Person) RETURN id(n) as id, n.name as value UNION ALL MATCH (n:Company) return id(n) as id, n.name as value"
val relsQuery = "match (p:Person)-[r]->(c:Company) return id(p) as src, id(c) as dst, type(r) as value"

val graphFrame = Neo4j(sc).nodes(nodesQuery,Map.empty).rels(relsQuery,Map.empty).loadGraphFrame
```

**NOTE: The APIs below were the previous APIs which still work, but I recommend that you use and provide feedback on the new _builder_ API above.**

## RDD's

There are a few different RDD's all named `Neo4jXxxRDD`

* `Neo4jTupleRDD` returns a Seq[(String,AnyRef)] per row
* `Neo4jRowRDD` returns a spark-sql Row per row

## DataFrames

* `Neo4jDataFrame`, a SparkSQL `DataFrame` that you construct either with explicit type information about result names and types
* or inferred from the first result-row
* Neo4jDataFrame provides:
 1.`mergeEdgeList(sc: SparkContext, dataFrame: DataFrame, source: (label,Seq[prop]), relationship: (type,Seq[prop]), target: (label,Seq[prop]))` to merge a DataFrame back into a Neo4j graph
    * both nodes are merged by first property in sequence, all the others will be set on the entity
    * relationships are merged between the two nodes and all properties included in the sequence will be set on the relationship
    * property names from the sequence are used as column names for the data-frame, currently there is no name translation.<br/>
    *For example*: with a DataFrame containing a `screen` column describing the on-screen relationship between two actors, then to get a `screen` property on the relationship we'd need to supply a *relationship* tuple of `("ACTED_WITH",Seq("screen")`. If we had a `real` column in the DataFrame representing a real-life relationship as well, then we would need to add that property into the `Seq` too, e.g. `("ACTED_WITH",Seq("screen", "real"))` and so on.
    * the result are sent in batches of 10000 to the graph
    * optional `renamedColumns` parameter - can be used to create a relationship between nodes having the same properties.<br/>
 *For example*: `(keanu:Person {name: 'Keanu'})-[:ACTED_WITH]->(Laurence:Person {name: "Laurence"})` requires a DataFrame with 2 `name` columns which is not possible.
 To overcome this, one can create a DataFrame with `src_node_name` and `dst_node_name` and provide `renamedColumns = Map("src_node_name" -> "name", "dst_node_name" -> "name")`
 2. `createNodes(sc: SparkContext, dataFrame: DataFrame, nodes: (String,Seq[String]))` to create nodes in Neo4j graph.
    * nodes are created by first property in sequence, all the others will be set on the node
    * the result are sent in batches of 10000 to the graph
    * optional `renamedColums` parameter - can be used to create a node with a label different from `DataFrame`'s column name.<br/>


## GraphX - Neo4jGraph

* `Neo4jGraph` has methods to load and save a GraphX graph
* `Neo4jGraph.execute` runs a Cypher statement and returns a `CypherResult` with the `keys` and an `rows` Iterator of `Array[Any]`

* `Neo4jGraph.loadGraph(sc, label,rel-types,label2)` loads a graph via the relationships between those labeled nodes
* `Neo4jGraph.saveGraph(sc, graph, [nodeProp], [relTypeProp (type,prop)], [mainLabelId (label,prop)],[secondLabelId (label,prop)],merge=false)` saves a graph object to Neo4j by updating the given node- and relationship-properties
* `Neo4jGraph.loadGraphFromNodePairs(sc,stmt,params)` loads a graph from pairs of node-id's
* `Neo4jGraph.loadGraphFromRels(sc,stmt,params)` loads a graph from pairs of start- and end-node-id's and and additional value per relationship
* `Neo4jGraph.loadGraph(sc, (stmt,params), (stmt,params))` loads a graph with two dedicated statements first for nodes, second for relationships

## Graph Frames

[GraphFrames](http://graphframes.github.io/) ([Spark Packages](http://spark-packages.org/package/graphframes/graphframes)) are a new Apache Spark API to process graph data.

It is similar and based on DataFrames, you can create GraphFrames from DataFrames and also from GraphX graphs.

* `Neo4jGraphFrame(sqlContext, (srcNodeLabel,nodeProp), (relType,relProp), dst:(dstNodeLabel,dstNodeProp)` loads a graph with the given source and destination nodes and the relationships in between, the relationship-property is optional and can be null
* `Neo4jGraphFrame.fromGraphX(sc,label,Seq(rel-type),label)` loads a graph with the given pattern
* `Neo4jGraphFrame.fromEdges(sqlContext, srcNodeLabel, Seq(relType), dstNodeLabel)`


## Example Usage

### Setup

Download and install Apache Spark 2.1 from http://spark.apache.org/downloads.html 

Download and install Neo4j 3.0.0 or later (e.g. from http://neo4j.com/download/)

For a simple dataset of connected people run the two following Cypher statements, that create 1M people and 1M relationships in about a minute.


    FOREACH (x in range(1,1000000) | CREATE (:Person {name:"name"+x, age: x%100}));
    
    UNWIND range(1,1000000) as x
    MATCH (n),(m) WHERE id(n) = x AND id(m)=toInt(rand()*1000000)
    CREATE (n)-[:KNOWS]->(m);

### Dependencies

You can also provide the dependencies to spark-shell or spark-submit via `--packages` and optionally `--repositories`.

    $SPARK_HOME/bin/spark-shell \
          --conf spark.neo4j.bolt.password=<neo4j-password> \
          --packages neo4j-contrib:neo4j-spark-connector:2.1.0-M4

### Neo4j(Row|Tuple)RDD

    $SPARK_HOME/bin/spark-shell --conf spark.neo4j.bolt.password=<neo4j-password> \
    --packages neo4j-contrib:neo4j-spark-connector:2.1.0-M4

```scala
<!-- tag::example_rdd[] -->

    import org.neo4j.spark._
    
    Neo4jTupleRDD(sc,"MATCH (n) return id(n)",Seq.empty).count
    // res46: Long = 1000000
    
    Neo4jRowRDD(sc,"MATCH (n) where id(n) < {maxId} return id(n)",Seq("maxId" -> 100000)).count
    // res47: Long = 100000
<!-- end::example_rdd[] -->
```

### Neo4jDataFrame

    $SPARK_HOME/bin/spark-shell --conf spark.neo4j.bolt.password=<neo4j-password> \
    --packages neo4j-contrib:neo4j-spark-connector:2.1.0-M4

```scala
    import org.neo4j.spark._
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    
    val df = Neo4jDataFrame.withDataType(sqlContext, "MATCH (n) return id(n) as id",Seq.empty, "id" -> LongType)
    // df: org.apache.spark.sql.DataFrame = [id: bigint]
    
    df.count
    // res0: Long = 1000000
    
    
    val query = "MATCH (n:Person) return n.age as age"
    val df = Neo4jDataFrame.withDataType(sqlContext,query, Seq.empty, "age" -> LongType)
    // df: org.apache.spark.sql.DataFrame = [age: bigint]
    df.agg(sum(df.col("age"))).collect()
    // res31: Array[org.apache.spark.sql.Row] = Array([49500000])
    
    query: String = MATCH (n:Person) return n.age as age
    
    // val query = "MATCH (n:Person)-[:KNOWS]->(m:Person) where n.id = {x} return m.age as age"
    val query = "MATCH (n:Person) where n.id = {x} return n.age as age"
    val rdd = sc.makeRDD(1.to(1000000))
    val ages = rdd.map( i => {
        val df = Neo4jDataFrame.withDataType(sqlContext,query, Seq("x"->i.asInstanceOf[AnyRef]), "age" -> LongType)
        df.agg(sum(df("age"))).first().getLong(0)
        })
    // TODO
    val ages.reduce( _ + _ )
    
    val df = Neo4jDataFrame(sqlContext, "MATCH (n) WHERE id(n) < {maxId} return n.name as name",Seq("maxId" -> 100000),"name" -> "string")
    df.count
    // res0: Long = 100000
```

### Neo4jGraph Operations

    $SPARK_HOME/bin/spark-shell --conf spark.neo4j.bolt.password=<neo4j-password> \
    --packages neo4j-contrib:neo4j-spark-connector:2.1.0-M4

```scala
    import org.neo4j.spark._
    
    val g = Neo4jGraph.loadGraph(sc, "Person", Seq("KNOWS"), "Person")
    // g: org.apache.spark.graphx.Graph[Any,Int] = org.apache.spark.graphx.impl.GraphImpl@574985d8
    
    g.vertices.count
    // res0: Long = 999937
    
    g.edges.count
    // res1: Long = 999906
    
    import org.apache.spark.graphx._
    import org.apache.spark.graphx.lib._
    
    val g2 = PageRank.run(g, 5)
    
    val v = g2.vertices.take(5)
    // v: Array[(org.apache.spark.graphx.VertexId, Double)] = Array((185012,0.15), (612052,1.0153273593749998), (354796,0.15), (182316,0.15), (199516,0.38587499999999997))
    
    Neo4jGraph.saveGraph(sc, g2, "rank")
    // res2: (Long, Long) = (999937,0)                        

    // full syntax example
    Neo4jGraph.saveGraph(sc, graph, "rank",("LIKES","score"),Some(("Person","name")),Some(("Movie","title")), merge=true)
```

### Neo4jGraphFrame

GraphFrames are a new Apache Spark API to process graph data.

It is similar and based on DataFrames, you can create GraphFrames from DataFrames and also from GraphX graphs.


There was a recent release (0.5.0) of GraphFrames for Spark 2.1 and Scala 2.11 which we use.
It is available on the [Maven repository for Apache Spark Packages](http://dl.bintray.com/spark-packages/maven/graphframes/graphframes).

Resources:

* [Introduction article](https://databricks.com/blog/2016/03/03/introducing-graphframes.html)
* [API Docs](http://graphframes.github.io/api/scala/index.html#org.graphframes.GraphFrame$)
// * [Flights Example](https://databricks.com/blog/2016/03/16/on-time-flight-performance-with-spark-graphframes.html)
// * [SparkSummit Video](https://spark-summit.org/east-2016/speakers/ankur-dave/)


    $SPARK_HOME/bin/spark-shell --conf spark.neo4j.bolt.password=<neo4j-password> \
    --packages neo4j-contrib:neo4j-spark-connector:2.1.0-M4

```scala  
<!-- tag::example_graphframes[] -->

    import org.neo4j.spark._
    
    val gdf = Neo4jGraphFrame(sqlContext,"Person" -> "name",("KNOWS"),"Person" -> "name")
    // gdf: org.graphframes.GraphFrame = GraphFrame(v:[id: bigint, prop: string], 
    //                                e:[src: bigint, dst: bigint, prop: string])
    
    val gdf = Neo4jGraphFrame.fromGraphX(sc,"Person",Seq("KNOWS"),"Person")
    
    gdf.vertices.count // res0: Long = 1000000
    
    gdf.edges.count    // res1: Long = 999999
    
    val results = gdf.pageRank.resetProbability(0.15).maxIter(5).run
    
    // results: org.graphframes.GraphFrame = GraphFrame(
    //                   v:[id: bigint, prop: string, pagerank: double], 
    //                   e:[src: bigint, dst: bigint, prop: string, weight: double])
    
    results.vertices.take(5)
    
    // res3: Array[org.apache.spark.sql.Row] = Array([31,name32,0.96820096875], [231,name232,0.15], 
    // [431,name432,0.15], [631,name632,1.1248028437499997], [831,name832,0.15])
    
    // pattern matching
    val results = gdf.find("(A)-[]->(B)").select("A","B").take(3)
    // results: Array[org.apache.spark.sql.Row] = Array([[159148,name159149],[31,name32]], 
    // [[461182,name461183],[631,name632]], [[296686,name296687],[1031,name1032]])
    
<!-- end::example_graphframes[] -->

    gdf.find("(A)-[]->(B);(B)-[]->(C); !(A)-[]->(C)")
    // res8: org.apache.spark.sql.DataFrame = [A: struct<id:bigint,prop:string>, B: struct<id:bigint,prop:string>, C: struct<id:bigint,prop:string>]
    
    gdf.find("(A)-[]->(B);(B)-[]->(C); !(A)-[]->(C)").take(3)
    // res9: Array[org.apache.spark.sql.Row] = Array([[904749,name904750],[702750,name702751],[122280,name122281]], [[240723,name240724],[813112,name813113],[205438,name205439]], [[589543,name589544],[600245,name600246],[659932,name659933]])
    
    // doesn't work yet ... complains about different table widths
    val results = gdf.find("(A)-[]->(B); (B)-[]->(C); !(A)-[]->(C)").filter("A.id != C.id")
    // Select recommendations for A to follow C
    val results = results.select("A", "C").take(3)
    
    gdf.labelPropagation.maxIter(3).run().take(3)
```

## Neo4j-Java-Driver

The project uses the [java driver](http://github.com/neo4j/neo4j-java-driver) for Neo4j's Bolt protocol.
We use its `org.neo4j.driver:neo4j-java-driver:1.0.4` version.

## Testing

Testing is done using `neo4j-harness`, a [test library](http://neo4j.com/docs/java-reference/current/#server-unmanaged-extensions-testing) for starting an in-process Neo4j-Server which you can use either with a JUnit `@Rule` or directly.
I only start one server and one SparkContext per test-class to avoid the lifecycle overhead. 

Please note that Neo4j running an in-process server pulls in Scala 2.11 for Cypher, so you need to run the tests with scala_2.11.
That's why I had to add two profiles for the different Scala versions.
