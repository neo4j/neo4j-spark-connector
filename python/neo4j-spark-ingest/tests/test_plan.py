import unittest
from pathlib import Path
from unittest.mock import MagicMock

from neo4j_spark_ingest import Neo4jIngestionPlan, SparkSource, neo4j_ingest_jar_path

"""
This is just a smoke test for the build tool. White box testing via mocks asserting
that expected call paths are traversed. We do more testing for Neo4jIngestionPlan in
the JVM module.
"""


class PlanTest(unittest.TestCase):
    def test_plan_converts_python_objects_and_options(self):
        spark = MagicMock()
        dataframe = MagicMock()
        java_sources = spark._jvm.java.util.HashSet.return_value
        java_config = spark._jvm.java.util.HashMap.return_value
        java_plan = (
            spark._jvm.org.neo4j.spark.ingest.Neo4jIngestionAPI.plan.return_value
        )

        result = Neo4jIngestionPlan.plan(
            spark,
            [SparkSource("customer", dataframe)],
            "version: '1'",
            {"url": "neo4j://localhost:7687", "workers": "2"},
            workers=4,
            batch_size=20_000,
            some_flag=True,
        )

        java_source = spark._jvm.org.neo4j.spark.ingest.SparkSource.return_value
        spark._jvm.org.neo4j.spark.ingest.SparkSource.assert_called_once_with(
            "customer", dataframe._jdf
        )
        java_sources.add.assert_called_once_with(java_source)
        java_config.put.assert_any_call("url", "neo4j://localhost:7687")
        java_config.put.assert_any_call("workers", "4")
        java_config.put.assert_any_call("batch.size", "20000")
        java_config.put.assert_any_call("some.flag", "true")
        spark._jvm.org.neo4j.spark.ingest.Neo4jIngestionAPI.plan.assert_called_once_with(
            spark._jsparkSession, java_sources, "version: '1'", java_config
        )

        result.ingest(disjoint=True)
        java_plan.ingest.assert_called_once_with(True)

    def test_jar_path_has_a_stable_name(self):
        self.assertEqual(
            Path(neo4j_ingest_jar_path()).name, "neo4j-spark-ingest-portable.jar"
        )


if __name__ == "__main__":
    unittest.main()
