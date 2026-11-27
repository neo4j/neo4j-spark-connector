from __future__ import annotations

from importlib.resources import files
from typing import TYPE_CHECKING, Any

from neo4j_spark_ingest._sources import SparkSource

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class Neo4jIngestionException(Exception):
    pass


class Neo4jIngestionPlan:
    def __init__(self, java_plan: Any) -> None:
        self._java_plan = java_plan

    def ingest(self, disjoint: bool = False) -> None:
        self._java_plan.ingest(disjoint)

    @staticmethod
    def plan(
        spark: SparkSession,
        sources: set[SparkSource],
        import_spec: str,
        config: dict[str, str | int | bool | None] | None = None,
        **options: str | int | bool,
    ) -> Neo4jIngestionPlan:
        """Create an ingestion plan for a specific SparkSession. Provided set of sources
        can be referenced in your provided import_spec.

        Keyword options override config entries. Underscores in keyword names become dots,
        so batch_size=20000 is passed to the connector as batch.size=20000.
        """
        jvm = spark._jvm
        if jvm is None:
            raise Neo4jIngestionException("jvm unreachable from your spark session")

        java_sources = jvm.java.util.HashSet()
        if java_sources is None:
            raise Neo4jIngestionException("unable to access java.util.*")

        for s in sources:
            if not isinstance(s, SparkSource):
                raise TypeError(
                    "provided sources must contain only SparkSource instances"
                )

            java_source = jvm.org.neo4j.spark.ingest.SparkSource(s.name, s.df._jdf)
            if java_source is None:
                raise Neo4jIngestionException("unable to cast SparkSource to jvm")

            java_sources.add(java_source)  # pyright: ignore reportAttributeAccessIssue

        java_config = jvm.java.util.HashMap()
        if java_config is None:
            raise Neo4jIngestionException("unable to access java.util.*")

        merged_options = dict(config or {})
        merged_options.update(
            {key.replace("_", "."): value for key, value in options.items()}
        )

        for key, value in merged_options.items():
            java_config.put(key, _java_string(value))  # pyright: ignore reportAttributeAccessIssue

        return Neo4jIngestionPlan(
            jvm.org.neo4j.spark.ingest.Neo4jIngestionAPI.plan(
                spark._jsparkSession, java_sources, import_spec, java_config
            )
        )


def _java_string(value: str | int | bool | None) -> str:
    if value is None:
        raise ValueError("configuration values cannot be None")
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def neo4j_ingest_jar_path() -> str:
    return str(
        files("neo4j_spark_ingest").joinpath("lib", "neo4j-spark-ingest-portable.jar")
    )
