# Neo4j Connector for Apache Spark

This repository contains the Neo4j Connector for Apache Spark.

## License

This neo4j-connector-apache-spark is Apache 2 Licensed

## Documentation

The documentation for Neo4j Connector for Apache Spark lives at https://github.com/neo4j/docs-spark repository.

## Spark Version Compatibility

| Spark | Scala | Java | Status |
|-------|-------|------|--------|
| 3.3.x – 3.5.x | 2.12, 2.13 | 8, 11, 17, 21 | Supported |
| 4.0.x | 2.13 | 17, 21 | Supported |
| 4.1.x | 2.13 | 17, 21 | Supported |

## Building for Spark 3

You can build for Spark 3.x with both Scala 2.12 and Scala 2.13

```
./maven-release.sh package 2.12
./maven-release.sh package 2.13
```

These commands will generate the corresponding targets
* `spark-3/target/neo4j-connector-apache-spark_2.12-<version>_for_spark_3.jar`
* `spark-3/target/neo4j-connector-apache-spark_2.13-<version>_for_spark_3.jar`

## Building for Spark 4

Spark 4.x uses Scala 2.13 exclusively and requires Java 17 or later.

```bash
# Build for Spark 4.0.x (latest: 4.0.2)
mvn install -Dspark-4 -pl common,test-support,spark-4

# Build for Spark 4.1.x (latest: 4.1.1)
mvn install -Dspark-4.1 -pl common,test-support,spark-4
```

These commands generate:
* `spark-4/target/neo4j-connector-apache-spark_4-<version>.jar`

> **Note:** The `-Dspark-4` and `-Dspark-4.1` system properties activate the corresponding
> Maven profile. This sets Scala 2.13 and the appropriate `spark.version` for the entire build.

## Integration with Apache Spark 3 Applications

**spark-shell, pyspark, or spark-submit**

```bash
$SPARK_HOME/bin/spark-shell --jars neo4j-connector-apache-spark_2.12-<version>_for_spark_3.jar
$SPARK_HOME/bin/spark-shell --packages org.neo4j:neo4j-connector-apache-spark_2.12:<version>_for_spark_3
```

**Maven**

```xml
<dependencies>
  <dependency>
    <groupId>org.neo4j</groupId>
    <artifactId>neo4j-connector-apache-spark_2.12</artifactId>
    <version>[version]_for_spark_3</version>
  </dependency>
</dependencies>
```

**sbt**

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "org.neo4j" % "neo4j-connector-apache-spark_2.12" % "<version>_for_spark_3"
```

## Integration with Apache Spark 4 Applications

**spark-shell, pyspark, or spark-submit**

```bash
$SPARK_HOME/bin/spark-shell --jars neo4j-connector-apache-spark_4_2.13-<version>_for_spark_4.jar
$SPARK_HOME/bin/spark-shell --packages org.neo4j:neo4j-connector-apache-spark_4_2.13:<version>_for_spark_4
```

**PySpark 4.x + GraphFrames**

```bash
pip install graphframes-py==0.11.0
$SPARK_HOME/bin/pyspark \
  --jars neo4j-connector-apache-spark_4_2.13-<version>_for_spark_4.jar \
  --packages io.graphframes:graphframes-spark4_2.13:0.11.0
```

**Maven**

```xml
<dependencies>
  <dependency>
    <groupId>org.neo4j</groupId>
    <artifactId>neo4j-connector-apache-spark_4_2.13</artifactId>
    <version>[version]_for_spark_4</version>
  </dependency>
</dependencies>
```

## Spark Connect

The connector runs server-side and works transparently with Spark Connect.
Deploy the connector JAR on the Spark Connect server's classpath:

```bash
./sbin/start-connect-server.sh \
  --jars neo4j-connector-apache-spark_4_2.13-<version>_for_spark_4.jar
```

Clients then use the standard DataFrame API:

```python
spark = SparkSession.builder.remote("sc://localhost").getOrCreate()
df = spark.read.format("neo4j") \
    .option("url", "bolt://neo4j-host:7687") \
    .option("labels", "Person") \
    .load()
```

For more info about the available versions visit https://neo4j.com/developer/spark/overview/#_compatibility
