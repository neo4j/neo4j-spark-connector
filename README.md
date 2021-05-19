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
./mvnw clean package -P spark-2.4 -P scala-2.11
./mvnw clean package -P spark-2.4 -P scala-2.12
```

These commands will generate the corresponding targets
* `spark-2.4/target/neo4j-connector-apache-spark_2.11-4.0.1_for_spark_2.4.jar`
* `spark-2.4/target/neo4j-connector-apache-spark_2.12-4.0.1_for_spark_2.4.jar`


### Building for Spark 3

You can build for Spark 3 by running

```
./mvnw clean package -P spark-3 -P scala-2.12
```

This will generate `spark-3/target/neo4j-connector-apache-spark_2.12-4.0.1_for_spark_3.jar`


## Integration with Apache Spark Applications

**spark-shell, pyspark, or spark-submit**

`$SPARK_HOME/bin/spark-shell --jars neo4j-connector-apache-spark_2.12-4.0.1_for_spark_3.jar`

`$SPARK_HOME/bin/spark-shell --packages neo4j-contrib:neo4j-connector-apache-spark_2.12:4.0.1_for_spark_3`

**sbt**

If you use the [sbt-spark-package plugin](https://github.com/databricks/sbt-spark-package), in your sbt build file, add:

```scala spDependencies += "neo4j-contrib/neo4j-connector-apache-spark_2.11:4.0.1_for_spark_2.4"```

Otherwise,

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "neo4j-contrib" % "neo4j-connector-apache-spark_2.12" % "4.0.1_for_spark_2.4"
```

Or, for Spark 3

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "neo4j-contrib" % "neo4j-connector-apache-spark_2.12" % "4.0.1_for_spark_3"
```  

**maven**  
In your pom.xml, add:   

```xml
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>org.neo4j</groupId>
    <artifactId>neo4j-connector-apache-spark_2.11</artifactId>
    <version>4.0.1_for_spark_2.4</version>
  </dependency>
</dependencies>
<repositories>
  <!-- list of other repositories -->
  <repository>
    <id>SparkPackagesRepo</id>
    <url>http://dl.bintray.com/spark-packages/maven</url>
  </repository>
</repositories>
```

In case of Spark 3

```xml
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>org.neo4j</groupId>
    <artifactId>neo4j-connector-apache-spark_2.12</artifactId>
    <version>4.0.1_for_spark_3</version>
  </dependency>
</dependencies>
<repositories>
  <!-- list of other repositories -->
  <repository>
    <id>SparkPackagesRepo</id>
    <url>http://dl.bintray.com/spark-packages/maven</url>
  </repository>
</repositories>
```

For more info about the available version visit https://neo4j.com/developer/spark/overview/#_compatibility
