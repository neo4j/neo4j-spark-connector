# Neo4j Connector for Apache Spark

This repository contains the Neo4j Connector for Apache Spark.

## License

The Neo4j Connector for Apache Spark is Apache 2 licensed.

## Documentation

The documentation for Neo4j Connector for Apache Spark lives at https://github.com/neo4j/docs-spark repository.

## Building for Spark 4

You can build for Spark 4.x with Scala 2.13

```
./maven-release.sh package 2.13
```

This generates two artifacts:
* `spark/target/neo4j-spark-connector-<version>.jar` — the shaded ("fat") jar with all dependencies bundled
* `spark-slim/target/neo4j-spark-connector-<version>-slim.jar` — the non-shaded ("slim") jar; use this when you want to manage the connector's transitive dependencies yourself


## Integration with Apache Spark Applications

**spark-shell, pyspark, or spark-submit**

`$SPARK_HOME/bin/spark-shell --jars neo4j-spark-connector-<version>.jar`

`$SPARK_HOME/bin/spark-shell --packages org.neo4j.connectors:spark:<version>`

**maven**

The connector is published in two flavors: use the shaded `spark` artifact for a self-contained jar, or `spark-slim` if you want Maven to resolve the connector's transitive dependencies.

In your pom.xml, add:

```xml
<dependencies>
  <!-- shaded: all dependencies bundled -->
  <dependency>
    <groupId>org.neo4j.connectors</groupId>
    <artifactId>spark</artifactId>
    <version>[version]</version>
  </dependency>

  <!-- non-shaded: manage transitive dependencies yourself -->
  <dependency>
    <groupId>org.neo4j.connectors</groupId>
    <artifactId>spark-slim</artifactId>
    <version>[version]</version>
  </dependency>
</dependencies>
```

For more info about the available version visit https://neo4j.com/developer/spark/overview/#_compatibility
