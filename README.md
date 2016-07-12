# Neo4j-Spark-Connector based on Neo4j 3.0's Bolt protocol

These are the beginnings of a Neo4j-Spark-Connector using the new binary protocol for Neo4j, Bolt.

Find [more information](http://neo4j.com/docs/developer-manual/current/#driver-manual-index) about the Bolt protocol, available drivers and documentation.

Please note that I still know very little about Apache Spark and might have done really dumb things.
Please let me know by [creating an issue](https://github.com/neo4j-contrib/neo4j-spark-connector/issues) or even better [submitting a pull request](https://github.com/neo4j-contrib/neo4j-spark-connector/pulls) to this repo.

## License

This neo4j-spark-connector is Apache 2 Licensed

## Building

Build `target/neo4j-spark-connector_2.10-1.0.0-RC1-full.jar` for Scala 2.10


    git clone https://github.com/neo4j-contrib/neo4j-spark-connector
    cd neo4j-spark-connector
    mvn clean compile assembly:single -DskipTests -Pscala_2.10


Build `target/neo4j-spark-connector_2.11-full-1.0.0-RC1.jar` for Scala 2.11


    mvn clean install assembly:single


## Config

If you're running Neo4j on localhost with the default ports, you onl have to configure your password in `spark.neo4j.bolt.password=<password>`.
	
Otherwise set the `spark.neo4j.bolt.url` in your `SparkConf` pointing e.g. to `bolt://host:port`.

You can provide user and password as part of the URL `bolt://neo4j:<password>@localhost` or individually in `spark.neo4j.bolt.user` and `spark.neo4j.bolt.password`.


## RDD's

There are a few different RDD's all named `Neo4jXxxRDD`

* `Neo4jTupleRDD` returns a Seq[(String,AnyRef)] per row
* `Neo4jRowRDD` returns a spark-sql Row per row

## DataFrames

* `Neo4jDataFrame`, a SparkSQL `DataFrame` that you construct either with explicit type information about result names and types
* or inferred from the first result-row

## GraphX - Neo4jGraph

* `Neo4jGraph` has methods to load and save a GraphX graph
* `Neo4jGraph.execute` runs a Cypher statement and returns a `CypherResult` with the `keys` and an `rows` Iterator of `Array[Any]`

* `Neo4jGraph.loadGraph(sc, label,rel-types,label2)` loads a graph via the relationships between those labeled nodes
* `Neo4jGraph.saveGraph(g, nodeProp, relProp)` saves a graph object to Neo4j by updating the given node- and relationship-properties
* `Neo4jGraph.loadGraphFromNodePairs(sc,stmt,params)` loads a graph from pairs of node-id's
* `Neo4jGraph.loadGraphFromRels(sc,stmt,params)` loads a graph from pairs of start- and end-node-id's and and additional value per relationship
* `Neo4jGraph.loadGraph(sc, (stmt,params), (stmt,params))` loads a graph with two dedicated statements first for nodes, second for relationships

## Graph Frames

[GraphFrames](http://graphframes.github.io/) ([Spark Packages](http://spark-packages.org/package/graphframes/graphframes)) are a new Spark API to process graph data.

It is similar and based on DataFrames, you can create GraphFrames from DataFrames and also from GraphX graphs.

NOTE: GraphFrames are still early in development and had only a first release 0.1.0, which is currently only available for Scala 2.10.

* `Neo4jGraphFrame(sqlContext, (srcNodeLabel,nodeProp), (relType,relProp), dst:(dstNodeLabel,dstNodeProp)` loads a graph with the given source and destination nodes and the relationships in between, the relationship-property is optional and can be null
* `Neo4jGraphFrame.fromGraphX(sc,label,Seq(rel-type),label)` loads a graph with the given pattern
* `Neo4jGraphFrame.fromEdges(sqlContext, srcNodeLabel, Seq(relType), dstNodeLabel)`


## Example Usage

### Setup

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
          --packages neo4j-contrib:neo4j-spark-connector:1.0.0-RC1,graphframes:graphframes:0.1.0-spark1.6

### Neo4j(Row|Tuple)RDD

    $SPARK_HOME/bin/spark-shell --conf spark.neo4j.bolt.password=<neo4j-password> \
    --packages neo4j-contrib:neo4j-spark-connector:1.0.0-RC1

```scala
    import org.neo4j.spark._
    
    Neo4jTupleRDD(sc,"MATCH (n) return id(n)",Seq.empty).count
    // res46: Long = 1000000
    
    Neo4jRowRDD(sc,"MATCH (n) where id(n) < {maxId} return id(n)",Seq(("maxId",100000))).count
    // res47: Long = 100000
```

### Neo4jDataFrame

    $SPARK_HOME/bin/spark-shell --conf spark.neo4j.bolt.password=<neo4j-password> \
    --packages neo4j-contrib:neo4j-spark-connector:1.0.0-RC1

```scala
    import org.neo4j.spark._
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    
    val df = Neo4jDataFrame.withDataType(sqlContext, "MATCH (n) return id(n) as id",Seq.empty, ("id",LongType))
    // df: org.apache.spark.sql.DataFrame = [id: bigint]
    
    df.count
    // res0: Long = 1000000
    
    
    val query = "MATCH (n:Person) return n.age as age"
    val df = Neo4jDataFrame.withDataType(sqlContext,query, Seq.empty, ("age",LongType))
    // df: org.apache.spark.sql.DataFrame = [age: bigint]
    df.agg(sum(df.col("age"))).collect()
    // res31: Array[org.apache.spark.sql.Row] = Array([49500000])
    
    // val query = "MATCH (n:Person)-[:KNOWS]->(m:Person) where n.id = {x} return m.age as age"
    val query = "MATCH (n:Person) where n.id = {x} return n.age as age"
    val rdd = sc.makeRDD(1.to(1000000))
    val ages = rdd.map( i => {
        val df = Neo4jDataFrame.withDataType(sqlContext,query, Seq("x"->i.asInstanceOf[AnyRef]), ("age",LongType))
        df.agg(sum(df("age"))).first().getLong(0)
        })
    // TODO
    val ages.reduce( _ + _ )
    
    
    val df = Neo4jDataFrame(sqlContext, "MATCH (n) WHERE id(n) < {maxId} return n.name as name",Seq(("maxId",100000)),("name","string"))
    df.count
    // res0: Long = 100000
```

### Neo4jGraph Operations

    $SPARK_HOME/bin/spark-shell --conf spark.neo4j.bolt.password=<neo4j-password> \
    --packages neo4j-contrib:neo4j-spark-connector:1.0.0-RC1

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
```

### Neo4jGraphFrame

GraphFrames are a new Spark API to process graph data.

It is similar and based on DataFrames, you can create GraphFrames from DataFrames and also from GraphX graphs.


There was a first release (0.1.0) of GraphFrames which is only available for Scala 2.10 which is available on the [Maven repository for Spark Packages](http://dl.bintray.com/spark-packages/maven/graphframes/graphframes).

Resources:

* [Introduction article](https://databricks.com/blog/2016/03/03/introducing-graphframes.html)
* [API Docs](http://graphframes.github.io/api/scala/index.html#org.graphframes.GraphFrame$)
// * [Flights Example](https://databricks.com/blog/2016/03/16/on-time-flight-performance-with-spark-graphframes.html)
// * [SparkSummit Video](https://spark-summit.org/east-2016/speakers/ankur-dave/)


    $SPARK_HOME/bin/spark-shell --conf spark.neo4j.bolt.password=<neo4j-password> \
    --packages neo4j-contrib:neo4j-spark-connector:1.0.0-RC1,graphframes:graphframes:0.1.0-spark1.6

```scala  

    import org.neo4j.spark._
    
    val gdf = Neo4jGraphFrame(sqlContext,("Person","name"),("KNOWS",null),("Person","name"))
    // gdf: org.graphframes.GraphFrame = GraphFrame(v:[id: bigint, prop: string], e:[src: bigint, dst: bigint, prop: string])
    
    val gdf = Neo4jGraphFrame.fromGraphX(sc,"Person",Seq("KNOWS"),"Person")
    
    gdf.vertices.count
    // res0: Long = 1000000
    
    gdf.edges.count
    // res3: Long = 999999
    
    val results = gdf.pageRank.resetProbability(0.15).maxIter(5).run
    // results: org.graphframes.GraphFrame = GraphFrame(v:[id: bigint, prop: string, pagerank: double], e:[src: bigint, dst: bigint, prop: string, weight: double])
    
    results.vertices.take(5)
    // res5: Array[org.apache.spark.sql.Row] = Array([31,name32,0.96820096875], [231,name232,0.15], [431,name432,0.15], [631,name632,1.1248028437499997], [831,name832,0.15])
    
    // pattern matching
    val results = gdf.find("(A)-[]->(B)").select("A","B").take(3)
    // results: Array[org.apache.spark.sql.Row] = Array([[159148,name159149],[31,name32]], [[461182,name461183],[631,name632]], [[296686,name296687],[1031,name1032]])
    
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


You can also [build it yourself](https://github.com/graphframes/graphframes) or pull the [Spark 1.6 jar from the Spark Packages page](http://spark-packages.org/package/graphframes/graphframes).

To build the `neo4j-spark-connector with GraphFrames support build and install GraphFrames locally with:


    git clone https://github.com/graphframes/graphframes
    cd graphframes
    # scala 2.10
    sbt -Dspark.version=1.6.0 assembly publishM2
    # scala 2.11 you have to patch build.sbt to scalatest 2.13 -> "org.scalatest" %% "scalatest" % "2.1.3" % "test"
    sbt -Dspark.version=1.6.0 assembly publishM2 -Dscala.version=2.11.7



## Neo4j-Java-Driver

The project uses the [java driver](http://github.com/neo4j/neo4j-java-driver) for Neo4j's Bolt protocol.
We use its `org.neo4j.driver:neo4j-java-driver:1.0.1` version.

## Testing

Testing is done using `neo4j-harness`, a [test library](http://neo4j.com/docs/java-reference/current/#server-unmanaged-extensions-testing) for starting an in-process Neo4j-Server which you can use either with a JUnit `@Rule` or directly.
I only start one server and one SparkContext per test-class to avoid the lifecycle overhead. 

Please note that Neo4j running an in-process server pulls in Scala 2.11 for Cypher, so you need to run the tests with spark_2.11.
That's why I had to add two profiles for the different Scala versions.
