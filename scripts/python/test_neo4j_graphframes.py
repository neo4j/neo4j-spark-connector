#!/usr/bin/env python3
"""
Integration test: Neo4j Community + PySpark 4.x + GraphFrames 0.11.0

Verifies that the Neo4j Spark connector works with PySpark 4.x and that
GraphFrames 0.11.0 can run graph algorithms (PageRank) on data loaded from Neo4j.

Usage:
    python test_neo4j_graphframes.py <connector_jar> <neo4j_image>

Example:
    python test_neo4j_graphframes.py neo4j-connector-spark4-4.1.1.jar neo4j:5
"""
import sys
import unittest
from tzlocal import get_localzone
from testcontainers.neo4j import Neo4jContainer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
from graphframes import GraphFrame


NEO4J_CONNECTOR_JAR = None
NEO4J_IMAGE = None
GRAPHFRAMES_JAR = "io.graphframes:graphframes-spark4_2.13:0.11.0"


class Neo4jGraphFramesTest(unittest.TestCase):
    neo4j_container = None
    spark: SparkSession = None
    bolt_url: str = None
    neo4j_password: str = "password"

    def neo4j_read(self, **options):
        reader = (
            self.spark.read.format("neo4j")
            .option("url", self.bolt_url)
            .option("authentication.type", "basic")
            .option("authentication.basic.username", "neo4j")
            .option("authentication.basic.password", self.neo4j_password)
        )
        for k, v in options.items():
            reader = reader.option(k, v)
        return reader.load()

    def tearDown(self):
        """Clear all data between tests using Community Edition compatible approach."""
        from neo4j import GraphDatabase
        from neo4j import Auth
        with GraphDatabase.driver(
            self.bolt_url, auth=("neo4j", self.neo4j_password)
        ) as driver:
            with driver.session() as session:
                session.run("MATCH (n) DETACH DELETE n").consume()
                for row in session.run("SHOW CONSTRAINTS YIELD name RETURN name").data():
                    session.run(f"DROP CONSTRAINT `{row['name']}` IF EXISTS").consume()
                for row in session.run(
                    "SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name"
                ).data():
                    session.run(f"DROP INDEX `{row['name']}` IF EXISTS").consume()

    def _create_person_graph(self):
        """Create a small social graph: 6 people and KNOWS relationships."""
        from neo4j import GraphDatabase
        with GraphDatabase.driver(
            self.bolt_url, auth=("neo4j", self.neo4j_password)
        ) as driver:
            with driver.session() as session:
                session.run("""
                    CREATE (alice:Person {id: 1, name: 'Alice', age: 34})
                    CREATE (bob:Person   {id: 2, name: 'Bob',   age: 27})
                    CREATE (carol:Person {id: 3, name: 'Carol', age: 45})
                    CREATE (dave:Person  {id: 4, name: 'Dave',  age: 31})
                    CREATE (eve:Person   {id: 5, name: 'Eve',   age: 22})
                    CREATE (frank:Person {id: 6, name: 'Frank', age: 55})
                    CREATE (alice)-[:KNOWS {since: 2010}]->(bob)
                    CREATE (alice)-[:KNOWS {since: 2012}]->(carol)
                    CREATE (bob)-[:KNOWS   {since: 2015}]->(dave)
                    CREATE (carol)-[:KNOWS {since: 2018}]->(dave)
                    CREATE (dave)-[:KNOWS  {since: 2019}]->(eve)
                    CREATE (eve)-[:KNOWS   {since: 2020}]->(frank)
                    CREATE (frank)-[:KNOWS {since: 2021}]->(alice)
                """).consume()

    # ------------------------------------------------------------------
    # Basic connector tests
    # ------------------------------------------------------------------

    def test_read_nodes_from_neo4j(self):
        """Read Person nodes from Neo4j into a Spark DataFrame."""
        self._create_person_graph()
        df = self.neo4j_read(labels="Person")
        assert df.count() == 6, f"Expected 6 nodes, got {df.count()}"
        names = {row["name"] for row in df.select("name").collect()}
        assert "Alice" in names
        assert "Frank" in names

    def test_write_nodes_to_neo4j(self):
        """Write Spark DataFrame rows as nodes into Neo4j."""
        data = [(10, "Greta"), (11, "Hans"), (12, "Ingrid")]
        df = self.spark.createDataFrame(data, ["id", "name"])
        df.write.format("neo4j") \
            .option("url", self.bolt_url) \
            .option("authentication.type", "basic") \
            .option("authentication.basic.username", "neo4j") \
            .option("authentication.basic.password", self.neo4j_password) \
            .option("labels", ":NewPerson") \
            .mode("append") \
            .save()
        result = self.neo4j_read(labels="NewPerson")
        assert result.count() == 3

    def test_read_relationships_from_neo4j(self):
        """Read KNOWS relationships from Neo4j."""
        self._create_person_graph()
        df = self.neo4j_read(
            relationship="KNOWS",
            **{"relationship.source.labels": "Person",
               "relationship.target.labels": "Person"}
        )
        assert df.count() == 7, f"Expected 7 relationships, got {df.count()}"

    def test_cypher_query(self):
        """Execute a Cypher query via the connector."""
        self._create_person_graph()
        df = self.neo4j_read(
            query="MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name, p.age AS age"
        )
        assert df.count() == 4, f"Expected 4 persons over 30, got {df.count()}"

    # ------------------------------------------------------------------
    # GraphFrames tests
    # ------------------------------------------------------------------

    def test_graphframes_pagerank(self):
        """Load graph from Neo4j, build a GraphFrame, and run PageRank."""
        self._create_person_graph()

        # Vertices: Person nodes
        vertices = self.neo4j_read(labels="Person") \
            .select(col("`<id>`").cast("string").alias("id"), col("name"), col("age"))

        # Edges: KNOWS relationships — need src/dst as string vertex IDs
        edges_raw = self.neo4j_read(
            relationship="KNOWS",
            **{"relationship.source.labels": "Person",
               "relationship.target.labels": "Person"}
        )
        edges = edges_raw.select(
            col("`<source.id>`").cast("string").alias("src"),
            col("`<target.id>`").cast("string").alias("dst"),
            col("`rel.since`").alias("since")
        )

        g = GraphFrame(vertices, edges)
        assert g.vertices.count() == 6
        assert g.edges.count() == 7

        # Run PageRank
        pr = g.pageRank(resetProbability=0.15, maxIter=5)
        top = pr.vertices.orderBy(desc("pagerank")).first()

        # Verify we get a reasonable result — PageRank runs without error
        assert top["pagerank"] > 0.0, "PageRank should produce positive scores"
        print(f"  Top PageRank: {top['name']} = {top['pagerank']:.4f}")

    def test_graphframes_bfs(self):
        """Breadth-first search from Alice to Frank."""
        self._create_person_graph()

        vertices = self.neo4j_read(labels="Person") \
            .select(col("`<id>`").cast("string").alias("id"), col("name"))
        edges_raw = self.neo4j_read(
            relationship="KNOWS",
            **{"relationship.source.labels": "Person",
               "relationship.target.labels": "Person"}
        )
        edges = edges_raw.select(
            col("`<source.id>`").cast("string").alias("src"),
            col("`<target.id>`").cast("string").alias("dst")
        )

        g = GraphFrame(vertices, edges)
        paths = g.bfs(
            fromExpr="name = 'Alice'",
            toExpr="name = 'Frank'",
            maxPathLength=6
        )
        assert paths.count() > 0, "BFS should find at least one path from Alice to Frank"
        print(f"  BFS paths from Alice to Frank: {paths.count()}")

    def test_graphframes_connected_components(self):
        """Connected components: all nodes should be in one component."""
        self._create_person_graph()

        # GraphFrames connected components needs a checkpoint dir
        self.spark.sparkContext.setCheckpointDir("/tmp/graphframes_checkpoint")

        vertices = self.neo4j_read(labels="Person") \
            .select(col("`<id>`").cast("string").alias("id"), col("name"))
        edges_raw = self.neo4j_read(
            relationship="KNOWS",
            **{"relationship.source.labels": "Person",
               "relationship.target.labels": "Person"}
        )
        # Make edges undirected by unioning with reversed
        edges = edges_raw.select(
            col("`<source.id>`").cast("string").alias("src"),
            col("`<target.id>`").cast("string").alias("dst")
        )
        edges_rev = edges.select(col("dst").alias("src"), col("src").alias("dst"))
        all_edges = edges.union(edges_rev)

        g = GraphFrame(vertices, all_edges)
        cc = g.connectedComponents()
        num_components = cc.select("component").distinct().count()
        assert num_components == 1, \
            f"Expected 1 connected component in our test graph, got {num_components}"
        print(f"  Connected components: {num_components}")

    def test_graphframes_triangle_count(self):
        """Triangle count — verify count runs without error."""
        self._create_person_graph()

        vertices = self.neo4j_read(labels="Person") \
            .select(col("`<id>`").cast("string").alias("id"), col("name"))
        edges_raw = self.neo4j_read(
            relationship="KNOWS",
            **{"relationship.source.labels": "Person",
               "relationship.target.labels": "Person"}
        )
        edges = edges_raw.select(
            col("`<source.id>`").cast("string").alias("src"),
            col("`<target.id>`").cast("string").alias("dst")
        )
        g = GraphFrame(vertices, edges)
        from pyspark import StorageLevel
        tc = g.triangleCount(storage_level=StorageLevel.MEMORY_ONLY)
        total_triangles = tc.agg({"count": "sum"}).collect()[0][0]
        assert total_triangles is not None
        print(f"  Total triangle count (sum): {total_triangles}")


def main():
    global NEO4J_CONNECTOR_JAR, NEO4J_IMAGE
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <connector_jar> <neo4j_image>")
        sys.exit(1)

    NEO4J_CONNECTOR_JAR = sys.argv[1]
    NEO4J_IMAGE = sys.argv[2]

    current_tz = get_localzone().zone

    container = (
        Neo4jContainer(NEO4J_IMAGE)
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        .with_env("NEO4J_db_temporal_timezone", current_tz)
        # Use default neo4j/password auth (testcontainers sets this by default)
    )
    with container:
        Neo4jGraphFramesTest.neo4j_container = container
        bolt_base = container.get_connection_url()
        Neo4jGraphFramesTest.bolt_url = bolt_base
        # Authentication credentials for the driver calls
        Neo4jGraphFramesTest.neo4j_password = "password"

        Neo4jGraphFramesTest.spark = (
            SparkSession.builder
            .appName("Neo4jGraphFramesIntegrationTest")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.jars.packages", GRAPHFRAMES_JAR)
            .config("spark.jars", NEO4J_CONNECTOR_JAR)
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )

        # Suppress noisy Spark logging during test
        Neo4jGraphFramesTest.spark.sparkContext.setLogLevel("WARN")

        loader = unittest.TestLoader()
        suite = loader.loadTestsFromTestCase(Neo4jGraphFramesTest)
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)

        Neo4jGraphFramesTest.spark.stop()
        sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
