# Neo4j Connector for Apache Spark

This repository contains the Neo4j Connector for Apache Spark.

## License

This neo4j-connector-apache-spark is Apache 2 Licensed

## Documentation

The documentation for Neo4j Connector for Apache Spark lives at https://github.com/neo4j/docs-spark repository.

## Spark Version Compatibility

| Spark | Scala | Java | Status |
|-------|-------|------|--------|
| 3.3.x – 3.5.x | 2.12, 2.13 | 8, 11, 17, 21 | Supported |
| 4.0.x | 2.13 | 17, 21 | Supported |
| 4.1.x | 2.13 | 17, 21 | Supported |

---

## Building for Spark 3

You can build for Spark 3.x with both Scala 2.12 and Scala 2.13:

```bash
./maven-release.sh package 2.12
./maven-release.sh package 2.13
```

These commands generate:
* `spark-3/target/neo4j-connector-apache-spark_2.12-<version>_for_spark_3.jar`
* `spark-3/target/neo4j-connector-apache-spark_2.13-<version>_for_spark_3.jar`

## Building for Spark 4

Spark 4.x uses Scala 2.13 exclusively and requires Java 17 or later.

```bash
# Spark 4.0.x (latest: 4.0.2)
mvn install -Dspark-4 -pl common,test-support,spark-4

# Spark 4.1.x (latest: 4.1.1)
mvn install -Dspark-4.1 -pl common,test-support,spark-4
```

> **Note:** `-Dspark-4` and `-Dspark-4.1` are system property flags that activate
> the corresponding Maven profile and set `scala.binary.version=2.13` and the
> appropriate `spark.version` for the full build.

---

## Using with Spark 4.1 (Classic mode)

Classic mode is where your driver application runs in the same JVM as the Spark
cluster. This is the traditional way to use Spark.

### Python (PySpark 4.1)

```bash
pip install pyspark==4.1.1
```

```python
from pyspark.sql import SparkSession

# Create a local Spark 4.1 session with the Neo4j connector on the classpath.
# Replace the version number with the connector release you are using.
spark = (
    SparkSession.builder
    .appName("Neo4j + Spark 4.1")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.neo4j:neo4j-connector-apache-spark_4_2.13:<version>_for_spark_4",
    )
    .getOrCreate()
)

NEO4J_URL      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "password"

# ── Read ─────────────────────────────────────────────────────────────────────

# Read all Person nodes into a DataFrame
people = (
    spark.read.format("neo4j")
    .option("url",                         NEO4J_URL)
    .option("authentication.basic.username", NEO4J_USER)
    .option("authentication.basic.password", NEO4J_PASSWORD)
    .option("labels", "Person")
    .load()
)
people.show()

# Read with a Cypher query
movies = (
    spark.read.format("neo4j")
    .option("url",                         NEO4J_URL)
    .option("authentication.basic.username", NEO4J_USER)
    .option("authentication.basic.password", NEO4J_PASSWORD)
    .option("query", "MATCH (m:Movie) WHERE m.released >= 2000 RETURN m.title AS title, m.released AS year")
    .load()
)
movies.show()

# Read a relationship
acted_in = (
    spark.read.format("neo4j")
    .option("url",                                NEO4J_URL)
    .option("authentication.basic.username",       NEO4J_USER)
    .option("authentication.basic.password",       NEO4J_PASSWORD)
    .option("relationship",                        "ACTED_IN")
    .option("relationship.source.labels",          "Person")
    .option("relationship.target.labels",          "Movie")
    .load()
)
acted_in.show()

# ── Write ─────────────────────────────────────────────────────────────────────

# Write a DataFrame as nodes (append)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name",  StringType(),  False),
    StructField("born",  IntegerType(), True),
])
data = [("Keanu Reeves", 1964), ("Carrie-Anne Moss", 1967)]
df = spark.createDataFrame(data, schema)

df.write.format("neo4j") \
    .option("url",                         NEO4J_URL) \
    .option("authentication.basic.username", NEO4J_USER) \
    .option("authentication.basic.password", NEO4J_PASSWORD) \
    .option("labels", ":Person") \
    .mode("append") \
    .save()

# Write with overwrite (MERGE on node.keys)
df.write.format("neo4j") \
    .option("url",                         NEO4J_URL) \
    .option("authentication.basic.username", NEO4J_USER) \
    .option("authentication.basic.password", NEO4J_PASSWORD) \
    .option("labels",     ":Person") \
    .option("node.keys",  "name") \
    .mode("overwrite") \
    .save()
```

### Scala (Spark 4.1)

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

val spark = SparkSession.builder()
  .appName("Neo4j + Spark 4.1")
  .master("local[*]")
  .config(
    "spark.jars.packages",
    "org.neo4j:neo4j-connector-apache-spark_4_2.13:<version>_for_spark_4"
  )
  .getOrCreate()

val url      = "bolt://localhost:7687"
val user     = "neo4j"
val password = "password"

// ── Read ─────────────────────────────────────────────────────────────────────

val people = spark.read.format("neo4j")
  .option("url",                          url)
  .option("authentication.basic.username", user)
  .option("authentication.basic.password", password)
  .option("labels", "Person")
  .load()

people.show()

// Read with Cypher
val movies = spark.read.format("neo4j")
  .option("url",                          url)
  .option("authentication.basic.username", user)
  .option("authentication.basic.password", password)
  .option("query",
    "MATCH (m:Movie) WHERE m.released >= 2000 RETURN m.title AS title, m.released AS year")
  .load()

movies.show()

// ── Write ─────────────────────────────────────────────────────────────────────

import spark.implicits._
val df = Seq(("Keanu Reeves", 1964), ("Carrie-Anne Moss", 1967))
  .toDF("name", "born")

df.write.format("neo4j")
  .option("url",                          url)
  .option("authentication.basic.username", user)
  .option("authentication.basic.password", password)
  .option("labels",    ":Person")
  .option("node.keys", "name")
  .mode(SaveMode.Overwrite)
  .save()
```

### PySpark 4.1 + GraphFrames 0.11.0

[GraphFrames](https://graphframes.io) lets you run distributed graph algorithms (PageRank,
BFS, Connected Components, etc.) directly on Spark DataFrames loaded from Neo4j.

```bash
pip install pyspark==4.1.1 graphframes-py==0.11.0
```

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
from graphframes import GraphFrame

spark = (
    SparkSession.builder
    .appName("Neo4j + Spark 4.1 + GraphFrames")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        # Neo4j connector + GraphFrames for Spark 4
        "org.neo4j:neo4j-connector-apache-spark_4_2.13:<version>_for_spark_4,"
        "io.graphframes:graphframes-spark4_2.13:0.11.0",
    )
    .getOrCreate()
)

NEO4J_URL      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "password"

def neo4j_read(**options):
    reader = (
        spark.read.format("neo4j")
        .option("url",                         NEO4J_URL)
        .option("authentication.basic.username", NEO4J_USER)
        .option("authentication.basic.password", NEO4J_PASSWORD)
    )
    for k, v in options.items():
        reader = reader.option(k, v)
    return reader.load()

# Vertices: Person nodes with a string "id" column required by GraphFrames
vertices = (
    neo4j_read(labels="Person")
    .select(col("`<id>`").cast("string").alias("id"), col("name"), col("born"))
)

# Edges: ACTED_IN relationships with "src" and "dst" string columns
edges_raw = neo4j_read(
    relationship="ACTED_IN",
    **{
        "relationship.source.labels": "Person",
        "relationship.target.labels": "Movie",
    }
)
edges = edges_raw.select(
    col("`<source.id>`").cast("string").alias("src"),
    col("`<target.id>`").cast("string").alias("dst"),
)

g = GraphFrame(vertices, edges)

# PageRank — find the most influential people in the graph
pr = g.pageRank(resetProbability=0.15, maxIter=10)
pr.vertices.orderBy(desc("pagerank")).show(5)

# BFS — shortest path between two people
paths = g.bfs(
    fromExpr="name = 'Kevin Bacon'",
    toExpr="name = 'Tom Hanks'",
    maxPathLength=6,
)
paths.show()

# Connected components (requires checkpoint)
spark.sparkContext.setCheckpointDir("/tmp/graphframes_checkpoint")
cc = g.connectedComponents()
cc.groupBy("component").count().orderBy(desc("count")).show(5)
```

---

## Using with Spark Connect (Spark 4.1)

[Spark Connect](https://spark.apache.org/docs/4.1.1/spark-connect-overview.html)
decouples the client from the Spark server using a gRPC/protobuf protocol. Your
client application (Python, Scala, Java) runs in its own process and sends logical
plans to a remote Spark server for execution. The Neo4j connector runs **on the
server side** — no connector changes needed compared to classic mode.

### 1. Start a Spark Connect server with the Neo4j connector

Download Spark 4.1.1 and start the Connect server with the Neo4j connector on the
classpath:

```bash
# Download
wget https://dlcdn.apache.org/spark/spark-4.1.1/spark-4.1.1-bin-hadoop3.tgz
tar xf spark-4.1.1-bin-hadoop3.tgz
cd spark-4.1.1-bin-hadoop3

# Start Spark Connect server (the connector JAR is resolved from Maven Central)
./sbin/start-connect-server.sh \
  --packages "org.neo4j:neo4j-connector-apache-spark_4_2.13:<version>_for_spark_4"
```

The server listens on `localhost:15002` by default.

To use a local JAR instead of `--packages`:

```bash
./sbin/start-connect-server.sh \
  --jars /path/to/neo4j-connector-apache-spark_4_2.13-<version>_for_spark_4.jar
```

### 2. Connect from Python (PySpark 4.1 client)

Install the PySpark Connect client — it is intentionally thin and does **not**
include Spark itself (the server handles all execution):

```bash
pip install "pyspark[connect]==4.1.1"
```

```python
from pyspark.sql import SparkSession

# Connect to the remote Spark Connect server
spark = SparkSession.builder.remote("sc://localhost").getOrCreate()

NEO4J_URL      = "bolt://neo4j-host:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "password"

# ── Read ─────────────────────────────────────────────────────────────────────

# The .format("neo4j") call is resolved server-side
people = (
    spark.read.format("neo4j")
    .option("url",                         NEO4J_URL)
    .option("authentication.basic.username", NEO4J_USER)
    .option("authentication.basic.password", NEO4J_PASSWORD)
    .option("labels", "Person")
    .load()
)
people.show()

movies_by_decade = (
    spark.read.format("neo4j")
    .option("url",                         NEO4J_URL)
    .option("authentication.basic.username", NEO4J_USER)
    .option("authentication.basic.password", NEO4J_PASSWORD)
    .option("query", """
        MATCH (m:Movie)
        RETURN m.title AS title,
               m.released AS released,
               (m.released / 10) * 10 AS decade
    """)
    .load()
)
movies_by_decade.groupBy("decade").count().orderBy("decade").show()

# ── Write ─────────────────────────────────────────────────────────────────────

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("title",    StringType(),  False),
    StructField("released", IntegerType(), True),
    StructField("tagline",  StringType(),  True),
])
new_movies = [
    ("The Matrix Resurrections", 2021, "Return to the Source"),
    ("John Wick: Chapter 4",     2023, "No Way Back. No Way Out."),
]
df = spark.createDataFrame(new_movies, schema)

df.write.format("neo4j") \
    .option("url",                         NEO4J_URL) \
    .option("authentication.basic.username", NEO4J_USER) \
    .option("authentication.basic.password", NEO4J_PASSWORD) \
    .option("labels",    ":Movie") \
    .option("node.keys", "title") \
    .mode("overwrite") \
    .save()

spark.stop()
```

### 3. Connect from Scala (Spark 4.1 client)

Add the Spark Connect client dependency (not the full `spark-sql`):

```xml
<!-- pom.xml — only the Connect client is needed, not the full Spark -->
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-connect-client-jvm_2.13</artifactId>
  <version>4.1.1</version>
</dependency>
```

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

// Connect to the remote Spark Connect server
val spark = SparkSession.builder()
  .remote("sc://localhost")
  .getOrCreate()

val url      = "bolt://neo4j-host:7687"
val user     = "neo4j"
val password = "password"

// ── Read ─────────────────────────────────────────────────────────────────────

val people = spark.read.format("neo4j")
  .option("url",                          url)
  .option("authentication.basic.username", user)
  .option("authentication.basic.password", password)
  .option("labels", "Person")
  .load()

people.printSchema()
people.show()

// Stream people into a Neo4j-powered aggregation
val topDirectors = spark.read.format("neo4j")
  .option("url",                          url)
  .option("authentication.basic.username", user)
  .option("authentication.basic.password", password)
  .option("query", """
    MATCH (p:Person)-[:DIRECTED]->(m:Movie)
    RETURN p.name AS director, count(m) AS films
    ORDER BY films DESC
  """)
  .load()

topDirectors.show()

// ── Write ─────────────────────────────────────────────────────────────────────

import spark.implicits._

val ratings = Seq(
  ("The Matrix",    9.2),
  ("John Wick",     8.5),
  ("Speed Racer",   6.9),
).toDF("title", "rating")

ratings.write.format("neo4j")
  .option("url",                          url)
  .option("authentication.basic.username", user)
  .option("authentication.basic.password", password)
  .option("labels",    ":Movie")
  .option("node.keys", "title")
  .mode(SaveMode.Overwrite)
  .save()

spark.stop()
```

### 4. Spark Connect + GraphFrames

GraphFrames 0.11.0 ships a dedicated Spark Connect server extension. This lets
remote clients (Python, Scala) run graph algorithms without the GraphX/RDD APIs
that are unavailable in Connect clients.

**Start the server with both the Neo4j connector and GraphFrames Connect extension:**

```bash
./sbin/start-connect-server.sh \
  --packages \
    "org.neo4j:neo4j-connector-apache-spark_4_2.13:<version>_for_spark_4,\
io.graphframes:graphframes-connect-spark4_2.13:0.11.0" \
  --conf "spark.connect.extensions.relation.classes=\
org.apache.spark.sql.graphframes.GraphFramesConnect"
```

**Python client:**

```bash
pip install "pyspark[connect]==4.1.1" graphframes-py==0.11.0
```

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
from graphframes import GraphFrame   # graphframes-py provides the Connect-aware client

spark = SparkSession.builder.remote("sc://localhost").getOrCreate()

NEO4J_URL      = "bolt://neo4j-host:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "password"

def neo4j_read(**options):
    reader = (
        spark.read.format("neo4j")
        .option("url",                         NEO4J_URL)
        .option("authentication.basic.username", NEO4J_USER)
        .option("authentication.basic.password", NEO4J_PASSWORD)
    )
    for k, v in options.items():
        reader = reader.option(k, v)
    return reader.load()

# Vertices: every Person as a graph node
vertices = (
    neo4j_read(labels="Person")
    .select(col("`<id>`").cast("string").alias("id"), col("name"), col("born"))
)

# Edges: every ACTED_IN relationship as a directed edge
edges_raw = neo4j_read(
    relationship="ACTED_IN",
    **{
        "relationship.source.labels": "Person",
        "relationship.target.labels": "Movie",
    }
)
edges = edges_raw.select(
    col("`<source.id>`").cast("string").alias("src"),
    col("`<target.id>`").cast("string").alias("dst"),
)

g = GraphFrame(vertices, edges)

# PageRank over Spark Connect — execution happens on the server
pagerank = g.pageRank(resetProbability=0.15, maxIter=10)
print("Top 10 most influential people:")
pagerank.vertices.orderBy(desc("pagerank")).select("name", "pagerank").show(10)

# Shortest path via BFS
paths = g.bfs(
    fromExpr="name = 'Kevin Bacon'",
    toExpr="name = 'Meryl Streep'",
    maxPathLength=6,
)
print(f"Paths from Kevin Bacon to Meryl Streep: {paths.count()}")

spark.stop()
```

### Key differences between Classic and Connect mode

| | Classic (local/cluster) | Spark Connect |
|---|---|---|
| Where connector runs | Same JVM as Spark driver | **Spark server** (remote) |
| Client dependency | `pyspark==4.1.1` | `pyspark[connect]==4.1.1` |
| GraphFrames | `graphframes-py` + `graphframes-spark4_2.13` JAR | `graphframes-py` + `graphframes-connect-spark4_2.13` on **server** |
| `SparkSession.builder` | `.master("local[*]")` + `.config("spark.jars.packages", ...)` | `.remote("sc://host")` |
| RDD access | Yes | No |
| `SparkContext` access | Yes | No |
| Connector JAR needed on client | Yes (via `--jars` or `--packages`) | **No** — only on server |

---

## Integration with Apache Spark 3 Applications

**spark-shell, pyspark, or spark-submit**

```bash
$SPARK_HOME/bin/spark-shell --jars neo4j-connector-apache-spark_2.12-<version>_for_spark_3.jar
$SPARK_HOME/bin/spark-shell --packages org.neo4j:neo4j-connector-apache-spark_2.12:<version>_for_spark_3
```

**Maven**

```xml
<dependencies>
  <dependency>
    <groupId>org.neo4j</groupId>
    <artifactId>neo4j-connector-apache-spark_2.12</artifactId>
    <version>[version]_for_spark_3</version>
  </dependency>
</dependencies>
```

**sbt**

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "org.neo4j" % "neo4j-connector-apache-spark_2.12" % "<version>_for_spark_3"
```

For more info about available versions visit https://neo4j.com/developer/spark/overview/#_compatibility
