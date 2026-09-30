import sys
import unittest

from pyspark.errors.exceptions.captured import IllegalArgumentException
from pyspark.sql import SparkSession


class PySparkInvalidSaveModesTest(unittest.TestCase):
    spark: SparkSession = None

    def test_recjects_when_node_save_modes_disallowed(self):
        df = self.spark.createDataFrame([("sourceNode", "targetNode")], ["from", "to"])

        cases = [
            ("source", "ErrorIfExists", "Match"),
            ("target", "Overwrite", "ErrorIfExists"),
            ("both", "ErrorIfExists", "ErrorIfExists"),
        ]

        for name, source_save_mode, target_save_mode in cases:
            with (
                self.subTest(name=name),
                self.assertRaisesRegex(
                    IllegalArgumentException,
                    "Save mode 'ErrorIfExists' is not a supported",
                ),
            ):
                (
                    df.write.mode("Overwrite")
                    .format("org.neo4j.spark.DataSource")
                    .option("relationship", "TEST")
                    .option("relationship.source.labels", ":DESTINATION")
                    .option("relationship.source.node.keys", "from")
                    .option("relationship.source.save.mode", source_save_mode)
                    .option("relationship.target.labels", ":DESTINATION")
                    .option("relationship.target.node.keys", "to")
                    .option("relationship.target.save.mode", target_save_mode)
                    .save()
                )

    def test_rejects_when_streaming_save_mode_disallowed(self):
        with self.assertRaisesRegex(
            IllegalArgumentException, "Save mode 'ErrorIfExists' is not a supported"
        ):
            (
                (
                    self.spark.readStream.format("rate")
                    .option("rowsPerSecond", 1)
                    .load()
                    .selectExpr("CAST(value AS STRING) AS id")
                )
                .writeStream.format("org.neo4j.spark.DataSource")
                .option("save.mode", "ErrorIfExists")
                .option("checkpointLocation", "/tmp/checkpoint/myCheckPoint")
                .option("labels", "Node")
                .option("node.keys", "id")
                .start()
            )

    def test_rejects_when_streaming_when_node_save_mode_disallowed(self):
        cases = [
            ("source", "ErrorIfExists", "Match"),
            ("target", "Overwrite", "ErrorIfExists"),
            ("both", "ErrorIfExists", "ErrorIfExists"),
        ]

        for name, source_save_mode, target_save_mode in cases:
            with (
                self.subTest(name=name),
                self.assertRaisesRegex(
                    IllegalArgumentException,
                    "Save mode 'ErrorIfExists' is not a supported",
                ),
            ):
                (
                    (
                        self.spark.readStream.format("rate")
                        .option("rowsPerSecond", 1)
                        .load()
                        .selectExpr("CAST(value AS STRING) AS id")
                    )
                    .writeStream.format("org.neo4j.spark.DataSource")
                    .option("save.mode", "Overwrite")
                    .option("relationship", "TEST")
                    .option("relationship.source.labels", ":DESTINATION")
                    .option("relationship.source.node.keys", "from")
                    .option("relationship.source.save.mode", source_save_mode)
                    .option("relationship.target.labels", ":DESTINATION")
                    .option("relationship.target.node.keys", "to")
                    .option("relationship.target.save.mode", target_save_mode)
                    .start()
                )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Wrong arguments count")
        print(sys.argv)
        sys.exit(1)

    connector_jar = str(sys.argv.pop())

    PySparkInvalidSaveModesTest.spark = (
        SparkSession.builder.appName("Neo4jConnectorPySparkTests")
        .master("local[*]")
        .config("spark.jars", connector_jar)
        .config("spark.driver.host", "127.0.0.1")
        .config("neo4j.url", "neo4j://localhost:7687")
        .config("neo4j.database", "neo4j")
        .config("neo4j.authentication.basic.username", "neo4j")
        .config("neo4j.authentication.basic.password", "password")
        .getOrCreate()
    )

    unittest.main()
