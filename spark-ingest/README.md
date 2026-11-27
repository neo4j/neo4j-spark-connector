# spark-ingest

Ingest API for the Neo4j Connector for Apache Spark: building blocks for defining Neo4j ingestion jobs.

Spark and the connector are `provided` dependencies. The module drives the connector through `format("neo4j")`, so the
connector must be on the Spark classpath at runtime, installed separately by the user.

## Portable JAR

The `package` phase attaches a second artifact with the `portable` classifier:

```text
spark-ingest-${project.version}-portable.jar
```

It contains `spark-ingest`, `import-spec`, and their non-Spark dependencies. Spark and the Neo4j Spark Connector are
excluded, so applications must supply them separately. The normal thin JAR remains the primary Maven artifact.

The Python package under `python/neo4j-spark-ingest` bundles this portable JAR. Run its `build.sh` script to build both
the Java artifacts and the Python wheel.

## Planning an ingestion

`Neo4jIngestionAPI` is the single public entry point. It parses and validates the raw YAML or JSON import specification,
binds its source names to the supplied `SparkSource` objects, and returns an executable plan. Creating a plan does not
start any Spark jobs.

```java
Set<SparkSource> sources = Set.of(
        new SparkSource("customer", spark.read().option("header", "true").csv("customer.csv")),
        new SparkSource("invoice", spark.read().option("header", "true").csv("invoice.csv"))
        // Declare the remaining sources referenced by the specification.
);

Map<String, String> options = Map.of(
        "url", "neo4j://localhost:7687",
        "username", "neo4j",
        "password", "password",
        "workers", "4",
        "batch.size", "5000");

String spec = Files.readString(Path.of("fruteria.yaml"), StandardCharsets.UTF_8);
Neo4jParallelIngestion plan = Neo4jIngestPlanAPI.plan(spark, sources, spec, options);
plan.ingest(true); // true when relationship start/end keys are disjoint per partition
```

`plan` throws `SpecificationException` when parsing or validation fails. The options map supplies the Neo4j connection
and execution settings required by the plan; keeping it explicit avoids storing credentials in Spark configuration.

### Python through Py4J

PySpark already exposes the JVM through `spark._jvm`. Pass the underlying JVM objects (`DataFrame._jdf` and
`SparkSession._jsparkSession`) and use Java collections rather than Python `set` and `dict` objects:

```python
from pathlib import Path

jvm = spark._jvm

def spark_source(name, dataframe):
    return jvm.org.neo4j.spark.ingest.SparkSource(name, dataframe._jdf)

declared_sources = [
    spark_source("customer", spark.read.option("header", "true").csv("customer.csv")),
    spark_source("invoice", spark.read.option("header", "true").csv("invoice.csv")),
    # Declare the remaining sources referenced by the specification.
]

sources = jvm.java.util.HashSet()
for source in declared_sources:
    sources.add(source)

options = jvm.java.util.HashMap()
for key, value in {
    "url": "neo4j://localhost:7687",
    "username": "neo4j",
    "password": "password",
    "workers": "4",
    "batch.size": "5000",
}.items():
    options.put(key, value)

plan = jvm.org.neo4j.spark.ingest.Neo4jIngestionAPI.plan(
    spark._jsparkSession,
    sources,
    Path("fruteria.yaml").read_text(encoding="utf-8"),
    options,
)
plan.ingest(False)
```

The returned plan is a JVM object; Py4J forwards `ingest` calls and converts the Python boolean to a Java `boolean`.
Both the `spark-ingest` and Neo4j Spark connector jars must be on the Spark driver and executor classpaths.

## Not published yet

`spark-ingest` is built and tested with the rest of the reactor, but it is **not deployed** by releases.
It inherits `maven.deploy.skip=true` from `spark-parent`, so a release keeps shipping only `org.neo4j.connectors:spark`.

Before publishing:

- set `maven.deploy.skip` to `false` in this module, and deploy `spark-parent` too, since this module inherits from it
- add the Maven Central metadata (`url`, `licenses`, `scm`, `developers`), and sources and javadoc jars
- add third-party licensing checks (`NOTICE.txt`, `LICENSES.txt`) if the module gains runtime dependencies
- include the jar in the jreleaser and TeamCity artifact paths
