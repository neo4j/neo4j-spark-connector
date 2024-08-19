# Neo4j Connector for Apache Spark

This repository contains the Neo4j Connector for Apache Spark.

## License

This neo4j-connector-apache-spark is Apache 2 Licensed

## Documentation

The documentation for Neo4j Connector for Apache Spark lives at https://github.com/neo4j/docs-spark repository.

## Building for Spark 4

You can build for Spark 4.x with Scala 2.13

```
./maven-release.sh package 2.13
```

These commands will generate the corresponding targets
* `spark-4/target/neo4j-connector-apache-spark_2.13-<version>_for_spark_4.jar`


## Integration with Apache Spark Applications

**spark-shell, pyspark, or spark-submit**

`$SPARK_HOME/bin/spark-shell --jars neo4j-connector-apache-spark_2.13-<version>_for_spark_4.jar`

`$SPARK_HOME/bin/spark-shell --packages org.neo4j:neo4j-connector-apache-spark_2.13:<version>_for_spark_4`

**sbt**

If you use the [sbt-spark-package plugin](https://github.com/databricks/sbt-spark-package), in your sbt build file, add:

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "org.neo4j" % "neo4j-connector-apache-spark_2.13" % "<version>_for_spark_4"
```  

**maven**  

In your pom.xml, add:   

```xml
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>org.neo4j</groupId>
    <artifactId>neo4j-connector-apache-spark_2.13</artifactId>
    <version>[version]_for_spark_4</version>
  </dependency>
</dependencies>
```

For more info about the available version visit https://neo4j.com/developer/spark/overview/#_compatibility
