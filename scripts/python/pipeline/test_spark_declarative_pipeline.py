from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path
from shutil import copy

import pyspark
from neo4j import Driver
from testcontainers.community.neo4j import Neo4jContainer

TEST_PROJECT = "test-pipeline"


class IntegrationTestError(RuntimeError):
    """Test failure due to an error"""


def log(stage: str, message: str) -> None:
    print(f"[pipeline-integration-test # {stage}] {message}", flush=True)


def require_connector_jar(raw_path: str) -> Path:
    jar_path = Path(raw_path).expanduser().resolve()
    if not jar_path.is_file():
        raise IntegrationTestError(f"connector JAR not found here: {jar_path}")

    return jar_path


def validate_spark_version(expected_major: int, expected_minor: int) -> None:
    version = str(pyspark.__version__)
    version_parts = version.split(".")

    try:
        major_version = int(version_parts[0])
        minor_version = int(version_parts[1])
    except (IndexError, ValueError) as error:
        raise IntegrationTestError(f"PySpark version parse error: {version}") from error

    if not major_version >= expected_major or not minor_version >= expected_minor:
        raise IntegrationTestError(f"Require at lest version 4.1.x, found: {version}")


def seed_test_graph(neo4j_driver: Driver) -> None:
    with neo4j_driver.session() as session:
        session.run("MATCH (node) DETACH DELETE node").consume()
        session.run(
            """
            CREATE
                (alice:Person {person_id: 1, name: 'Alice'}),
                (bob:Person {person_id: 2, name: 'Bob'}),
                (carol:Person {person_id: 3, name: 'Carol'}),
                (alice)-[:KNOWS {since: 2018}]->(bob),
                (alice)-[:KNOWS {since: 2022}]->(carol),
                (bob)-[:KNOWS {since: 2023}]->(carol)
            """
        ).consume()

        record = session.run(
            """
            MATCH (:Person)-[relationship:KNOWS]->(:Person)
            RETURN count(relationship) AS relationship_count
            """
        ).single()

    if record is None:
        raise IntegrationTestError("failed to write test data to neo4j: No data")

    count = record["relationship_count"]

    if count != 3:
        raise IntegrationTestError(
            f"failed to write test data to neo4j, wrong count: {count}, expected 3"
        )


def run_pipeline_command(
    connector_jar: Path,
    pipeline_project_root: Path,
    neo4j_url: str,
    subcommand: str,
) -> None:
    command = [
        "spark-pipelines",
        "--jars",
        str(connector_jar),
        "--driver-class-path",
        str(connector_jar),
        "--conf",
        "spark.driver.host=127.0.0.1",
        "--conf",
        "spark.driver.bindAddress=127.0.0.1",
        "--conf",
        "spark.ui.enabled=false",
        subcommand,
        "--spec",
        str(pipeline_project_root / "spark-pipeline.yml"),
    ]

    spark_local = (pipeline_project_root / "spark-local").resolve()
    spark_local.mkdir(parents=True, exist_ok=True)

    environment = os.environ.copy()
    environment["PYSPARK_PYTHON"] = sys.executable
    environment["SPARK_LOCAL_IP"] = "127.0.0.1"
    environment["SPARK_LOCAL_DIRS"] = str(spark_local)
    environment["NEO4J_URL"] = neo4j_url

    log(subcommand, "Executing: " + " ".join(command))
    subprocess.run(
        command,
        cwd=pipeline_project_root,
        env=environment,
        check=True,
    )


def run_integration_test(connector_jar: Path, temp_dir: Path, neo4j_url: str) -> None:
    for command in ("dry-run", "run"):
        run_pipeline_command(
            connector_jar,
            temp_dir / TEST_PROJECT,
            neo4j_url,
            command,
        )

    log("success", "Pipeline executed and asserted with no errors!")


def main(raw_connector_jar_path: str, neo4j_image: str) -> int:
    try:
        validate_spark_version(4, 1)
        log("setup", f"Using PySpark {pyspark.__version__}")

        connector_jar = require_connector_jar(raw_connector_jar_path)
        log("setup", f"Using connector JAR {connector_jar}")
        log("setup", f"Starting Neo4j container {neo4j_image}...")

        container = (
            Neo4jContainer(neo4j_image)
            .with_exposed_ports(7474, 7473)
            .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        )

        with (
            container as neo4j_container,
            neo4j_container.get_driver() as neo4j_driver,
            tempfile.TemporaryDirectory(prefix="neo4j-sdp-test-") as temp_dir_name,
        ):
            neo4j_url = neo4j_container.get_connection_url()
            log("setup", f"Loading test graph into {neo4j_url}")
            seed_test_graph(neo4j_driver)

            temp_dir_path = Path(temp_dir_name).resolve()
            log("setup", f"Creating pipeline project: {temp_dir_path}/{TEST_PROJECT}")

            subprocess.run(
                ["spark-pipelines", "init", "--name", TEST_PROJECT],
                cwd=temp_dir_path,
                check=True,
            )

            transformations = temp_dir_path / TEST_PROJECT / "transformations"

            # when we do `spark-pipelines init --name xyz` we get example files
            for example_file in transformations.iterdir():
                log("setup", f"Deleting excessive file {example_file}")
                example_file.unlink()

            copy(
                Path(__file__).resolve().parent / "neo4j_pipeline.py",
                transformations / "neo4j_pipeline.py",
            )

            log("test", "Starting test execution...")
            run_integration_test(connector_jar, temp_dir_path, neo4j_url)

        return 0
    except KeyboardInterrupt:
        print("[failed] Interrupted", file=sys.stderr)
        return 2
    except subprocess.CalledProcessError as error:
        print(
            f"[failed] spark-pipelines exited with status {error.returncode}",
            file=sys.stderr,
        )
        return 1
    except IntegrationTestError as error:
        print(f"[failed] Failure to set up the test: {error}", file=sys.stderr)
        return 2
    except Exception as error:
        print(f"[failed] Unexpected error: {error}", file=sys.stderr)
        traceback.print_exc()
        return 2


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <connector-jar> <neo4j-image>")
        sys.exit(2)

    sys.exit(main(sys.argv[1], sys.argv[2]))
