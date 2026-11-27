# neo4j-spark-ingest

Thin PySpark bindings for the Neo4j Spark ingestion API.
The package exposes Python DataFrames and configuration objects while hiding Py4J conversions.

The wheel contains a portable `spark-ingest` JAR with `import-spec` and its dependencies.
Supply a compatible connector through Spark's normal `--packages`, `--jars`, or cluster configuration.

This version supports the familiar pattern of JVM-backed PySpark.
The portable JAR must be on Spark's classpath before the Spark session starts.

## Build

From the repository root:

```bash
python/neo4j-spark-ingest/build.sh
```

This builds the Maven artifacts, copies the portable JAR and license files into the Python project, and writes a wheel to `python/neo4j-spark-ingest/dist/`.

## Install locally

```bash
python3 -m pip install python/neo4j-spark-ingest/dist/neo4j_spark_ingest-6.*.whl
```

For development, after running `build.sh`, an editable install also works:

```bash
python3 -m pip install -e python/neo4j-spark-ingest
```

## Configure Spark

Locate the bundled JAR's path by calling the `neo4j_ingest_jar_path` function:

```bash
INGEST_JAR="$(python3 -c 'from neo4j_spark_ingest import neo4j_ingest_jar_path; print(neo4j_ingest_jar_path())')"
```

Then supply it together with the connector:

```bash
spark-submit \
  --packages org.neo4j.connectors:spark:6.2.1 \
  --jars "${INGEST_JAR}" \
  your_job.py
```

Use a connector version appropriate for the Spark installation.
Use the Neo4j Spark Ingest python lib appropriate to the selected connector version.
Ingest python library should match the connector version (e.g. connector version v6.3.0 goes with ingest v6.3.0)

> [!NOTE]
> Ingestion via this API does so with parallelization and for high worker counts, the memory consumption in Spark might increase.
> You might see OOM errors, in that case, pass `--driver-memory Xg` to spark-submit to adjust how many gigabytes of RAM you provision.

## Neo4j Spark Ingestion API

With this set up you can now orchestrate Spark ingestions via PySpark using the API.
Below is an example on how to interact with the API.
Once your job hits `spark-submit`, you can enjoy a deadlock-free ingestion.

```python
from neo4j_spark_ingest import Neo4jIngestionPlan, SparkSource

# define you sources, it should be a set of `SparkSource` objects, where you freely pick any spark format
sources = {
    SparkSource("customer", spark.read().parquet("customer.parquet")),
    SparkSource("invoice", spark.read().option("header", "true").csv("invoice.csv")),
}

# provide your `import-spec` which will declare the import job, yaml or json accepted
# see here: https://github.com/neo4j/import-spec/
with open("retail-ingestion-import-spec.yaml", encoding="utf-8") as spec_file:
    spec = spec_file.read()

# now you have all the pieces!
ingestion = Neo4jIngestionPlan.plan(
    spark,
    sources,
    spec,
    workers=4,
    url="neo4j://localhost:7687",
    authentication_type="basic",
    authentication_basic_username="neo4j",
    authentication_basic_password="secret",
    batch_size=20_000,
)

ingestion.ingest()
```

Note that the SparkSession definition is omited but of course you need it from your environment.
Note that `spec` is the raw string represented YAML or JSON document.
Keyword arguments create a configuration which will be bypassed directly to the Neo4j Spark Connector.
You can also pass a dict from str to str.
Numeric and Boolean option values are converted to the strings expected by the Java API.
