# Neo4j Connector for Apache Spark

This repository contains the Neo4j Connector for Apache Spark.

## License

This neo4j-connector-apache-spark is Apache 2 Licensed

## Generating Documentation from Source

```
cd doc
# Install NodeJS dependencies
npm install
# Generate HTML/CSS from asciidoc
./node_modules/.bin/antora docs.yml
# Start local server to browse docs
npm run start
```

This will open http://localhost:8000/ which will serve development docs.

## Building

### Building for Spark 2.4

You can build for Spark 2.4 with both Scala 2.11 and Scala 2.12

```
./maven-release.sh package 2.11 2.4
./maven-release.sh package 2.12 2.4
```

These commands will generate the corresponding targets
* `spark-2.4/target/neo4j-connector-apache-spark_2.11-<version>_for_spark_2.4.jar`
* `spark-2.4/target/neo4j-connector-apache-spark_2.12-<version>_for_spark_2.4.jar`


### Building for Spark 3

You can build for Spark 2.4 with both Scala 2.12 and Scala 2.13

```
./maven-release.sh package 2.12 3
./maven-release.sh package 2.13 3
```

This will generate:
These commands will generate the corresponding targets
* `spark-3/target/neo4j-connector-apache-spark_2.12-<version>_for_spark_3.jar`
* `spark-3/target/neo4j-connector-apache-spark_2.13-<version>_for_spark_3.jar`


## Integration with Apache Spark Applications

**spark-shell, pyspark, or spark-submit**

`$SPARK_HOME/bin/spark-shell --jars neo4j-connector-apache-spark_2.12-<version>_for_spark_3.jar`

`$SPARK_HOME/bin/spark-shell --packages org.neo4j:neo4j-connector-apache-spark_2.12:<version>_for_spark_3`

**sbt**

If you use the [sbt-spark-package plugin](https://github.com/databricks/sbt-spark-package), in your sbt build file, add:

```scala spDependencies += "org.neo4j/neo4j-connector-apache-spark_2.11:<version>_for_spark_2.4"```

Otherwise,

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "org.neo4j" % "neo4j-connector-apache-spark_2.12" % "<version>_for_spark_2.4"
```

Or, for Spark 3

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "org.neo4j" % "neo4j-connector-apache-spark_2.12" % "<version>_for_spark_3"
```  

**maven**  

In your pom.xml, add:   

```xml
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>org.neo4j</groupId>
    <artifactId>neo4j-connector-apache-spark_2.11</artifactId>
    <version>[version]_for_spark_2.4</version>
  </dependency>
</dependencies>
```

In case of Spark 3

```xml
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>org.neo4j</groupId>
    <artifactId>neo4j-connector-apache-spark_2.12</artifactId>
    <version>[version]_for_spark_3</version>
  </dependency>
</dependencies>
```

For more info about the available version visit https://neo4j.com/developer/spark/overview/#_compatibility
