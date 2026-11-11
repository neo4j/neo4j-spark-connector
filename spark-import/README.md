# spark-import

Import API for the Neo4j Connector for Apache Spark: building blocks for defining Neo4j ingestion jobs.

The module only depends on Spark (`provided`). It drives the connector through `format("neo4j")`, so the connector
must be on the Spark classpath at runtime, installed separately by the user.

## Not published yet

`spark-import` is built and tested with the rest of the reactor, but it is **not deployed** by releases.
It inherits `maven.deploy.skip=true` from `spark-parent`, so a release keeps shipping only `org.neo4j.connectors:spark`.

Before publishing it:

- set `maven.deploy.skip` to `false` in this module, and deploy `spark-parent` too, since this module inherits from it
- add the Maven Central metadata (`url`, `licenses`, `scm`, `developers`), and sources and javadoc jars
- add third-party licensing checks (`NOTICE.txt`, `LICENSES.txt`) if the module gains runtime dependencies
- include the jar in the jreleaser and TeamCity artifact paths
