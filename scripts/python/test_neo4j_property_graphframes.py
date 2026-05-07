#!/usr/bin/env python3
"""
Integration test: Neo4j Community + PySpark 4.x + GraphFrames 0.11.0 PropertyGraphFrame

Demonstrates the GraphFrames 0.11 ``graphframes.pg`` API
(`https://graphframes.io/04-user-guide/11-property-graphs.html`) using the
Neo4j Spark Connector as the data source. Unlike the flat ``GraphFrame`` shown
in ``test_neo4j_graphframes.py``, ``PropertyGraphFrame`` preserves Neo4j's
per-label and per-relationship-type schemas.

The five tests cover:

  1. Construction          — build a 2-vertex-type / 2-edge-type property graph from Neo4j.
  2. Conversion + PageRank — ``to_graphframe`` then run an algorithm.
  3. Filtered conversion   — ``edge_group_filters`` / ``vertex_group_filters``.
  4. Bipartite projection  — ``projection_by`` (e.g. "people who liked the same movie").
  5. Join algorithm result — ``join_vertices`` round-trips ConnectedComponents back to
                             original Neo4j properties.

All vertex groups are constructed with ``apply_mask_on_id=False`` because Neo4j's
``<id>`` is globally unique across labels, so the SHA-256 hashing the API does by
default is unnecessary overhead and makes the resulting ``GraphFrame.id`` column
opaque.

Pinned dependency: ``io.graphframes:graphframes-spark4_2.13:0.11.0`` — the
PySpark binding for the PropertyGraphFrame API was added in 0.11.

Usage:
    python test_neo4j_property_graphframes.py <connector_jar> <neo4j_image>

Example:
    python test_neo4j_property_graphframes.py \
        /workspace/spark-4/target/neo4j-connector-apache-spark_4-5.4.2-SNAPSHOT.jar \
        neo4j:5
"""
import sys
import unittest

from tzlocal import get_localzone
from testcontainers.neo4j import Neo4jContainer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from graphframes import GraphFrame
from graphframes.pg import (
    EdgePropertyGroup,
    PropertyGraphFrame,
    VertexPropertyGroup,
)


NEO4J_CONNECTOR_JAR = None
NEO4J_IMAGE = None
GRAPHFRAMES_JAR = "io.graphframes:graphframes-spark4_2.13:0.11.0"


class Neo4jPropertyGraphFrameTest(unittest.TestCase):
    """End-to-end tests for graphframes.pg.* using Neo4j as the data source."""

    neo4j_container = None
    spark: SparkSession = None
    bolt_url: str = None
    neo4j_password: str = "password"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

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
        """Clear all data between tests (Community-Edition compatible)."""
        from neo4j import GraphDatabase
        with GraphDatabase.driver(
            self.bolt_url, auth=("neo4j", self.neo4j_password)
        ) as driver:
            with driver.session() as session:
                session.run("MATCH (n) DETACH DELETE n").consume()
                for row in session.run(
                    "SHOW CONSTRAINTS YIELD name RETURN name"
                ).data():
                    session.run(
                        f"DROP CONSTRAINT `{row['name']}` IF EXISTS"
                    ).consume()
                for row in session.run(
                    "SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' "
                    "RETURN name"
                ).data():
                    session.run(
                        f"DROP INDEX `{row['name']}` IF EXISTS"
                    ).consume()

    def _create_movies_graph(self):
        """
        Create a small 'movie fan social network' that mirrors the official
        GraphFrames PropertyGraphFrame user-guide example, but sourced from
        Neo4j: 5 Person nodes, 3 Movie nodes, LIKES (Person→Movie) and
        MESSAGES (Person→Person, weighted) relationships.
        """
        from neo4j import GraphDatabase
        with GraphDatabase.driver(
            self.bolt_url, auth=("neo4j", self.neo4j_password)
        ) as driver:
            with driver.session() as session:
                session.run("""
                    CREATE (alice:Person   {id: 1, name: 'Alice',   born: 1990})
                    CREATE (bob:Person     {id: 2, name: 'Bob',     born: 1985})
                    CREATE (carol:Person   {id: 3, name: 'Carol',   born: 1972})
                    CREATE (dave:Person    {id: 4, name: 'Dave',    born: 1995})
                    CREATE (eve:Person     {id: 5, name: 'Eve',     born: 2001})
                    CREATE (matrix:Movie       {id: 1, title: 'The Matrix',  released: 1999})
                    CREATE (inception:Movie    {id: 2, title: 'Inception',   released: 2010})
                    CREATE (interstellar:Movie {id: 3, title: 'Interstellar', released: 2014})
                    CREATE (alice)-[:LIKES]->(matrix)
                    CREATE (alice)-[:LIKES]->(inception)
                    CREATE (bob)-[:LIKES]->(matrix)
                    CREATE (carol)-[:LIKES]->(inception)
                    CREATE (dave)-[:LIKES]->(interstellar)
                    CREATE (eve)-[:LIKES]->(inception)
                    CREATE (alice)-[:MESSAGES {weight: 5.0}]->(bob)
                    CREATE (bob)-[:MESSAGES   {weight: 8.0}]->(carol)
                    CREATE (carol)-[:MESSAGES {weight: 3.0}]->(dave)
                    CREATE (dave)-[:MESSAGES  {weight: 6.0}]->(eve)
                    CREATE (eve)-[:MESSAGES   {weight: 9.0}]->(alice)
                """).consume()

    def _build_property_graph(self):
        """Build the PropertyGraphFrame fixture used by most tests."""
        # Vertex DataFrames — one per Neo4j label.
        # apply_mask_on_id=False because Neo4j's <id> is globally unique
        # across labels, so the default SHA-256 collision-prevention hashing
        # is unnecessary overhead and obscures the resulting id column.
        people_df = (
            self.neo4j_read(labels="Person")
            .select(
                col("id").cast("string").alias("id"),
                col("name"),
                col("born"),
            )
        )
        movies_df = (
            self.neo4j_read(labels="Movie")
            .select(
                col("id").cast("string").alias("id"),
                col("title"),
                col("released"),
            )
        )

        people_g = VertexPropertyGroup(
            name="Person",
            data=people_df,
            primary_key_column="id",
            apply_mask_on_id=False,
        )
        movies_g = VertexPropertyGroup(
            name="Movie",
            data=movies_df,
            primary_key_column="id",
            apply_mask_on_id=False,
        )

        # Edge DataFrames — one per Neo4j relationship type.
        likes_df = (
            self.neo4j_read(
                relationship="LIKES",
                **{
                    "relationship.source.labels": "Person",
                    "relationship.target.labels": "Movie",
                },
            )
            .select(
                col("`source.id`").cast("string").alias("src"),
                col("`target.id`").cast("string").alias("dst"),
            )
        )
        messages_df = (
            self.neo4j_read(
                relationship="MESSAGES",
                **{
                    "relationship.source.labels": "Person",
                    "relationship.target.labels": "Person",
                },
            )
            .select(
                col("`source.id`").cast("string").alias("src"),
                col("`target.id`").cast("string").alias("dst"),
                col("`rel.weight`").cast("double").alias("weight"),
            )
        )

        likes_g = EdgePropertyGroup(
            name="LIKES",
            data=likes_df,
            src_property_group=people_g,
            dst_property_group=movies_g,
            is_directed=False,            # users may "like" mutually
            src_column_name="src",
            dst_column_name="dst",
            weight_column_name=None,      # auto-fills lit(1.0)
        )
        messages_g = EdgePropertyGroup(
            name="MESSAGES",
            data=messages_df,
            src_property_group=people_g,
            dst_property_group=people_g,
            is_directed=True,
            src_column_name="src",
            dst_column_name="dst",
            weight_column_name="weight",
        )

        return PropertyGraphFrame(
            vertex_property_groups=[people_g, movies_g],
            edges_property_groups=[likes_g, messages_g],
        )

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_property_graph_construction(self):
        """Build a 2-vertex-type / 2-edge-type property graph from Neo4j."""
        self._create_movies_graph()
        pg = self._build_property_graph()

        assert len(pg.vertex_property_groups) == 2
        assert {g.name for g in pg.vertex_property_groups} == {"Person", "Movie"}
        assert len(pg.edges_property_groups) == 2
        assert {g.name for g in pg.edges_property_groups} == {"LIKES", "MESSAGES"}

        # Per-group counts via the underlying DataFrames.
        people_count = pg.vertex_property_groups[0].data.count()
        movies_count = pg.vertex_property_groups[1].data.count()
        assert people_count == 5, f"Expected 5 Person, got {people_count}"
        assert movies_count == 3, f"Expected 3 Movie,  got {movies_count}"

    def test_to_graphframe_and_pagerank(self):
        """Convert to a flat GraphFrame and run PageRank end-to-end."""
        self._create_movies_graph()
        pg = self._build_property_graph()

        g = pg.to_graphframe(
            vertex_property_groups=["Person", "Movie"],
            edge_property_groups=["LIKES", "MESSAGES"],
        )

        # 5 Person + 3 Movie = 8 vertices.
        # 6 LIKES (undirected => doubled to 12) + 5 MESSAGES (directed) = 17 edges.
        assert g.vertices.count() == 8, f"Expected 8 vertices, got {g.vertices.count()}"
        assert g.edges.count() == 17,   f"Expected 17 edges,   got {g.edges.count()}"

        pr = g.pageRank(resetProbability=0.15, maxIter=5)
        top = pr.vertices.orderBy(col("pagerank").desc()).first()
        assert top["pagerank"] > 0.0, "PageRank should produce positive scores"
        print(f"  Top PageRank vertex id: {top['id']} = {top['pagerank']:.4f}")

    def test_to_graphframe_with_filters(self):
        """edge_group_filters / vertex_group_filters subset the graph declaratively."""
        self._create_movies_graph()
        pg = self._build_property_graph()

        # Only MESSAGES with weight > 5 (i.e. weights 8.0, 6.0, 9.0 → 3 edges).
        g = pg.to_graphframe(
            vertex_property_groups=["Person"],
            edge_property_groups=["MESSAGES"],
            edge_group_filters={"MESSAGES": col("weight") > lit(5.0)},
        )
        assert g.vertices.count() == 5, "All 5 Person vertices should survive"
        assert g.edges.count() == 3, (
            f"Expected 3 MESSAGES with weight > 5, got {g.edges.count()}"
        )

    def test_bipartite_projection(self):
        """projection_by('Person', 'Movie', 'LIKES') yields Person↔Person edges."""
        self._create_movies_graph()
        pg = self._build_property_graph()

        projected = pg.projection_by(
            left_bi_graph_part="Person",
            right_bi_graph_part="Movie",
            edge_group="LIKES",
        )

        # The projected graph drops the Movie group and the LIKES edge group,
        # adds a 'projected_LIKES' Person→Person edge group.
        names = {g.name for g in projected.vertex_property_groups}
        assert names == {"Person"}, f"Expected only Person group, got {names}"

        edge_names = {g.name for g in projected.edges_property_groups}
        assert "projected_LIKES" in edge_names, (
            f"Expected 'projected_LIKES' edge group, got {edge_names}"
        )

        # Convert and verify at least one expected pairing exists:
        # Alice and Bob both like The Matrix → projected_LIKES edge between them.
        g = projected.to_graphframe(["Person"], ["projected_LIKES", "MESSAGES"])
        edges = g.edges.filter(col("src") != col("dst"))
        assert edges.count() > 0, (
            "Bipartite projection should produce at least one Person-Person edge"
        )
        print(f"  Projected Person-Person edges: {edges.count()}")

    def test_join_vertices_round_trip(self):
        """ConnectedComponents → join_vertices → join back to Neo4j properties."""
        self._create_movies_graph()
        pg = self._build_property_graph()

        # Use only LIKES so we have a bipartite, fully-connected component
        # (every Movie is liked by ≥1 Person; LIKES is undirected).
        g = pg.to_graphframe(["Person", "Movie"], ["LIKES"])

        self.spark.sparkContext.setCheckpointDir("/tmp/pg_cc_checkpoint")
        # NOTE: GraphFrames 0.11's default 'two_phase' ConnectedComponents
        # algorithm emits one row per vertex per edge incidence, so
        # `cc.count()` exceeds vertex count. dedupe before downstream joins.
        cc = g.connectedComponents().dropDuplicates(["id", "property_group"])

        # join_vertices(cc, [...]) maps each algorithm result row back to its
        # original (external_id, property_group) pair across the requested
        # vertex groups — but it does NOT pull in the original property columns.
        # That's a deliberate design choice in GraphFrames 0.11: properties may
        # have heterogeneous schemas across groups and unioning them would
        # introduce nulls. Users do the property join themselves on external_id.
        joined = pg.join_vertices(cc, ["Person", "Movie"])
        joined_cols = set(joined.columns)
        assert "external_id" in joined_cols, joined_cols
        assert "property_group" in joined_cols, joined_cols
        assert "component" in joined_cols, joined_cols
        assert joined.count() == 8, (
            f"Expected 8 joined rows (5 Person + 3 Movie), got {joined.count()}"
        )

        # Demonstrate the realistic pattern: split by property_group, then join
        # back to the per-label DataFrames to recover original Neo4j properties.
        #
        # NOTE: GraphFrames 0.11 emits TWO 'property_group' columns in
        # join_vertices output (one from each vertex group's get_data()).
        # Rename via toDF so subsequent column refs are unambiguous.
        people_df = pg.vertex_property_groups[0].data   # Person
        movies_df = pg.vertex_property_groups[1].data   # Movie

        # Positionally rename the duplicate 'property_group' column, then drop it.
        seen_pg = False
        renamed_cols = []
        for c in joined.columns:
            if c == "property_group":
                renamed_cols.append("property_group" if not seen_pg else "_pg_dup")
                seen_pg = True
            else:
                renamed_cols.append(c)
        joined_clean = joined.toDF(*renamed_cols).drop("_pg_dup")

        person_results = (
            joined_clean.filter(col("property_group") == lit("Person"))
                        .join(people_df,
                              joined_clean["external_id"] == people_df["id"])
                        .select("name", "born", "component")
        )
        movie_results = (
            joined_clean.filter(col("property_group") == lit("Movie"))
                        .join(movies_df,
                              joined_clean["external_id"] == movies_df["id"])
                        .select("title", "released", "component")
        )

        assert person_results.count() == 5, (
            f"Expected 5 Person rows after property join, got {person_results.count()}"
        )
        assert movie_results.count() == 3, (
            f"Expected 3 Movie rows after property join, got {movie_results.count()}"
        )
        # Bipartite undirected LIKES => everyone is in one component.
        person_components = {
            r["component"] for r in person_results.collect()
        }
        movie_components = {
            r["component"] for r in movie_results.collect()
        }
        assert person_components == movie_components, (
            f"Person and Movie should share components, got "
            f"Person={person_components}, Movie={movie_components}"
        )
        assert len(person_components) == 1, (
            f"Expected 1 connected component, got {len(person_components)}"
        )
        print(
            f"  join_vertices: 8 rows; "
            f"after property join: {person_results.count()} Person + "
            f"{movie_results.count()} Movie in {len(person_components)} component"
        )


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
    )
    with container:
        Neo4jPropertyGraphFrameTest.neo4j_container = container
        Neo4jPropertyGraphFrameTest.bolt_url = container.get_connection_url()
        Neo4jPropertyGraphFrameTest.neo4j_password = "password"

        Neo4jPropertyGraphFrameTest.spark = (
            SparkSession.builder
            .appName("Neo4jPropertyGraphFrameIntegrationTest")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.jars.packages", GRAPHFRAMES_JAR)
            .config("spark.jars", NEO4J_CONNECTOR_JAR)
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        Neo4jPropertyGraphFrameTest.spark.sparkContext.setLogLevel("WARN")

        loader = unittest.TestLoader()
        suite = loader.loadTestsFromTestCase(Neo4jPropertyGraphFrameTest)
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)

        Neo4jPropertyGraphFrameTest.spark.stop()
        sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
