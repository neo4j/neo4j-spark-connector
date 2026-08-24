import os

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()

NEO4J_URL = os.environ.get("NEO4J_URL")
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "password"


@dp.materialized_view(name="knows_relationships", format="parquet")
def knows_relationships() -> DataFrame:
    return (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("url", NEO4J_URL)
        .option("authentication.type", "basic")
        .option("authentication.basic.username", NEO4J_USERNAME)
        .option("authentication.basic.password", NEO4J_PASSWORD)
        .option("relationship", "KNOWS")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Person")
        .option("relationship.target.labels", ":Person")
        .load()
        .select(
            F.col("`source.person_id`").cast("long").alias("source_id"),
            F.col("`source.name`").cast("string").alias("source_name"),
            F.col("`target.person_id`").cast("long").alias("target_id"),
            F.col("`target.name`").cast("string").alias("target_name"),
            F.col("`rel.since`").cast("long").alias("since"),
        )
    )


@dp.materialized_view(name="recent_knows_relationships", format="parquet")
def recent_knows_relationships() -> DataFrame:
    return (
        spark.table("knows_relationships")
        .where(F.col("since") >= F.lit(2020))
        .select(
            "source_id",
            "source_name",
            "target_id",
            "target_name",
            "since",
            F.concat_ws(
                " -> ",
                F.col("source_name"),
                F.col("target_name"),
            ).alias("connection"),
        )
    )


@dp.materialized_view(name="pipeline_verification", format="parquet")
def pipeline_verification() -> DataFrame:
    actual = spark.table("recent_knows_relationships").select(
        "source_id",
        "source_name",
        "target_id",
        "target_name",
        "since",
        "connection",
    )
    expected = spark.sql(
        """
        SELECT
            CAST(1 AS BIGINT) AS source_id,
            'Alice' AS source_name,
            CAST(3 AS BIGINT) AS target_id,
            'Carol' AS target_name,
            CAST(2022 AS BIGINT) AS since,
            'Alice -> Carol' AS connection
        UNION ALL
        SELECT
            CAST(2 AS BIGINT) AS source_id,
            'Bob' AS source_name,
            CAST(3 AS BIGINT) AS target_id,
            'Carol' AS target_name,
            CAST(2023 AS BIGINT) AS since,
            'Bob -> Carol' AS connection
        """
    )

    differences = actual.exceptAll(expected).unionByName(expected.exceptAll(actual))
    check = differences.agg(F.count(F.lit(1)).alias("difference_count"))

    return check.select(
        F.col("difference_count"),
        F.assert_true(
            F.col("difference_count") == F.lit(0),
            "Expected exactly Alice->Carol/2022 and Bob->Carol/2023",
        )
        .cast("boolean")
        .alias("assertion"),
    )
