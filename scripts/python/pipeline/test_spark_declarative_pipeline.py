from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

import pyspark
from neo4j import Driver
from testcontainers.community.neo4j import Neo4jContainer

TEMPLATE_STORAGE_TOKEN = "%%%__PIPELINE_STORAGE__%%%"
TEMPLATE_URL_TOKEN = "%%%__NEO4J_URL__%%%"


class IntegrationTestError(RuntimeError):
    """Test failure due to an error"""


def log(stage: str, message: str) -> None:
    print(f"[{stage}] {message}", flush=True)


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


def find_spark_pipelines_executable() -> Path:
    candidates: list[Path] = []

    configured = os.environ.get("SPARK_PIPELINES")
    if configured:
        candidates.append(Path(configured).expanduser())

    on_path = shutil.which("spark-pipelines")
    if on_path:
        candidates.append(Path(on_path))

    spark_home = os.environ.get("SPARK_HOME")
    if spark_home:
        candidates.append(Path(spark_home) / "bin" / "spark-pipelines")

    pyspark_file = getattr(pyspark, "__file__", None)
    if pyspark_file:
        candidates.append(
            Path(pyspark_file).resolve().parent / "bin" / "spark-pipelines"
        )

    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if resolved.is_file() and os.access(resolved, os.X_OK):
            return resolved

    rendered = "\n  - ".join(str(path) for path in candidates) or "<none>"
    raise IntegrationTestError(
        "Could not find the spark-pipelines executable. Checked:\n"
        f"  - {rendered}\n"
        "Install PySpark with the 'pipelines' extra or set SPARK_PIPELINES."
    )


def template_generate(path: Path, token: str, value: str, count: int = 1) -> str:
    try:
        content = path.read_text(encoding="utf-8")
    except OSError as error:
        raise IntegrationTestError(f"failed to read template: {path}") from error

    return content.replace(token, value, count)


def create_pipeline_project(
    project_root: Path,
    template_root: Path,
    neo4j_url: str,
) -> tuple[Path, Path]:
    transformations_dir = project_root / "transformations"
    pipeline_storage_dir = project_root / "pipeline-storage"
    spark_local_dir = project_root / "spark-local"

    for directory in (
        transformations_dir,
        pipeline_storage_dir,
        spark_local_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    specification_path = project_root / "spark-pipeline.yml"
    specification_path.write_text(
        template_generate(
            template_root / "spark-pipeline-template.yml",
            TEMPLATE_STORAGE_TOKEN,
            pipeline_storage_dir.resolve().as_uri(),
        ),
        encoding="utf-8",
    )

    definition_path = transformations_dir / "neo4j_pipeline.py"
    definition_path.write_text(
        template_generate(
            template_root / "neo4j_pipeline_template.py",
            TEMPLATE_URL_TOKEN,
            neo4j_url,
        ),
        encoding="utf-8",
    )

    return specification_path, spark_local_dir


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
        raise IntegrationTestError("failed to write test data to neo4j: NO data")

    count = record["relationship_count"]

    if count != 3:
        raise IntegrationTestError(
            f"failed to write test data to neo4j, wrong count: {count}, expected 3"
        )


def run_pipeline_command(
    spark_pipelines: Path,
    connector_jar: Path,
    spec_path: Path,
    spark_local: Path,
    subcommand: str,
) -> None:
    command = [
        str(spark_pipelines),
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
        str(spec_path),
    ]

    environment = os.environ.copy()
    environment["PYSPARK_PYTHON"] = sys.executable
    environment["SPARK_LOCAL_IP"] = "127.0.0.1"
    environment["SPARK_LOCAL_DIRS"] = str(spark_local.resolve())

    log(subcommand, "Executing: " + " ".join(command))
    subprocess.run(
        command,
        cwd=spec_path.parent,
        env=environment,
        check=True,
    )


def run_integration_test(
    connector_jar: Path,
    spark_pipelines_executable: Path,
    spark_pipelines_spec_path: Path,
    spark_local_path: Path,
) -> None:
    for command in ("dry-run", "run"):
        run_pipeline_command(
            spark_pipelines_executable,
            connector_jar,
            spark_pipelines_spec_path,
            spark_local_path,
            command,
        )

    log("success", "Pipeline executed and asserted with no errors!")


def main(raw_connector_jar_path: str, neo4j_image: str) -> int:
    try:
        validate_spark_version(4, 1)
        log("setup", f"Using PySpark {pyspark.__version__}")

        spark_pipelines_executable = find_spark_pipelines_executable()
        log("setup", f"Using spark-pipelines at {spark_pipelines_executable}")

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
            tempfile.TemporaryDirectory(prefix="neo4j-sdp-test-") as temp_dir,
        ):
            templates_path = Path(__file__).resolve().parent
            neo4j_url = neo4j_container.get_connection_url()
            log("setup", f"Loading test graph into {neo4j_url}")
            seed_test_graph(neo4j_driver)

            temp_project_root = Path(temp_dir).resolve()
            log(
                "setup", f"Generating temporary pipeline project at {temp_project_root}"
            )
            spec_path, spark_local_path = create_pipeline_project(
                temp_project_root, templates_path, neo4j_url
            )

            log("test", "Starting test execution...")
            run_integration_test(
                connector_jar,
                spark_pipelines_executable,
                spec_path,
                spark_local_path,
            )

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
