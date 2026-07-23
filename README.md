# Neo4j Connector for Apache Spark

This repository contains the Neo4j Connector for Apache Spark.

## License

The Neo4j Connector for Apache Spark is licensed under the Apache License 2.0.

## Documentation

The documentation for Neo4j Connector for Apache Spark lives in this repository, under the `docs` directory.

## Building for Spark 4

You can build for Spark 4.x with Scala 2.13. Starting with version 6.0.0, only Scala 2.13 is supported.

```
./maven-release.sh package 2.13
```

This command generates the following artifact:
* `target/neo4j-spark-connector-<version>-s_2.13.jar`


## Integration with Apache Spark Applications

**spark-shell, pyspark, or spark-submit**

`$SPARK_HOME/bin/spark-shell --jars neo4j-spark-connector-<version>-s_2.13.jar`

`$SPARK_HOME/bin/spark-shell --packages org.neo4j.connectors:spark:<version>-s_2.13`

**sbt**

In your `build.sbt` file, add:

```scala
libraryDependencies += "org.neo4j.connectors" % "spark" % "<version>-s_2.13"
```

**maven**

In your pom.xml, add:

```xml
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>org.neo4j.connectors</groupId>
    <artifactId>spark</artifactId>
    <version>[version]-s_2.13</version>
  </dependency>
</dependencies>
```

For more info about the available version visit https://neo4j.com/developer/spark/overview/#_compatibility
